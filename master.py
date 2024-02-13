import os, glob, subprocess, logging, multiprocessing, datetime, traceback

import xarray as xr
import s3fs
import psutil
import time

from generate_namelist import rapid_namelist_from_directories
from inflow import create_inflow_file
from check_and_download_era import download_era5
from append import append_week
from cloud_logger import CloudLog

logging.basicConfig(level=logging.INFO,
                    filename='log.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')
S5CMD = 's5cmd'

def inflow_and_namelist(
        working_dir: str,
        namelist_dir: str,
        nc: str,
        vpu_dirs: list[str]) -> None:
    for vpu_dir in vpu_dirs:
        if not os.path.isdir(vpu_dir):
            continue
        vpu = os.path.basename(vpu_dir)
        
        inflow_dir = os.path.join(working_dir, 'data','inflows', vpu)
        output_dir = os.path.join(working_dir, 'data','outputs', vpu)
        init = glob.glob(os.path.join(output_dir,'Qfinal*.nc'))[0]


        create_inflow_file(nc,
                        vpu_dir,
                        inflow_dir,
                        vpu,
                        )
        
        rapid_namelist_from_directories(vpu_dir,
                                        inflow_dir,
                                        namelist_dir,
                                        output_dir,
                                        qinit_file=init
                                        )
        namelist = glob.glob(os.path.join(namelist_dir, f'namelist_{vpu}*'))[0]

        # Correct the paths in the namelist file
        with open(namelist, 'r') as f:
            text = f.read().replace('/home/ubuntu', '/mnt')
        with open(namelist, 'w') as f:
            f.write(text)

def get_initial_qinits(s3: s3fs.S3FileSystem, 
                       vpu_dirs: list[str],
                       qfinal_dir: str,
                       working_dir: str) -> None:
    """
    Get q initialization files from S3
    """
    if len(glob.glob(os.path.join(working_dir, 'data','outputs','*','*Qfinal*.nc'))) != 125: # If we don't have enough qfinal files, reget from S3
        {os.remove(f) for f in glob.glob(os.path.join(working_dir, 'data','outputs','*','*Qfinal*.nc'))}
        logging.info('Pulling qfinal files from s3')
        qfinal_files = [sorted([qfinal for qfinal in s3.ls(f'{qfinal_dir}/{os.path.basename(f)}') if 'Qfinal' in qfinal], \
                               key= lambda x: datetime.datetime.strptime(os.path.basename(x).split('_')[2].split('.')[0], '%Y%m%d'))[-1] for f in vpu_dirs if os.path.isdir(f)]
        for s3_file in qfinal_files:
            vpu = os.path.basename(s3_file).split('_')[1]
            os.makedirs(os.path.join(working_dir, 'data','outputs', vpu), exist_ok=True)
            with s3.open(s3_file, 'rb') as s3_f:
                with open(os.path.join(working_dir, 'data','outputs', vpu, os.path.basename(s3_file)), 'wb') as local_f:
                    local_f.write(s3_f.read())

def cache_to_s3(s3: s3fs.S3FileSystem,
                working_dir: str,
                s3_path: str,
                delete_all: bool= False) -> None:
    vpu_dirs = glob.glob(os.path.join(working_dir,'data', 'outputs','*'))
    for vpu_dir in vpu_dirs:
        # Delete the earliest qfinal, upload the latest qfinal
        qfinals = sorted([f for f in glob.glob(os.path.join(vpu_dir, 'Qfinal*.nc'))], 
                         key= lambda x: datetime.datetime.strptime(os.path.basename(x).split('_')[-1].split('.')[0], '%Y%m%d'))
        if delete_all:
            {os.remove(f) for f in qfinals}
        elif len(qfinals) == 2:
            os.remove(qfinals[0])
            upload_to_s3(s3, qfinals[1], f'{s3_path}/{os.path.basename(vpu_dir)}/{os.path.basename(qfinals[1])}')

        qouts = glob.glob(os.path.join(vpu_dir, 'Qout*.nc'))
        if qouts:
            qout = qouts[0]
            upload_to_s3(s3, qout,f'{s3_path}/{os.path.basename(vpu_dir)}/{os.path.basename(qout)}')
            os.remove(qout)

def drop_coords(ds: xr.Dataset, qout: str ='Qout'):
    """
    Helps load faster, gets rid of variables/dimensions we do not need (lat, lon, etc.)
    """
    return ds[[qout]].reset_coords(drop=True)

def upload_to_s3(s3: s3fs.S3FileSystem,
                 file_path: str,
                 s3_path: str) -> None:
    pass
    # with open(file_path, 'rb') as f:
    #     with s3.open(s3_path, 'wb') as sf:
    #         sf.write(f.read())

def cleanup(working_dir: str,
            qfinal_dir: str,
            delete_all: bool = False) -> None:
    # Delete namelists
    for file in glob.glob(os.path.join(working_dir, 'data','namelists', '*')):
        os.remove(file)

    # Delete era5 data
    for f in glob.glob(os.path.join(working_dir, 'data', 'era5_data', '*.nc')):
        os.remove(f)
        
    # Delete inflow files
    for f in glob.glob(os.path.join(working_dir, 'data','inflows','*','*.nc')):
        os.remove(f)

    # Cache qfinals and qouts, remove
    cache_to_s3(s3fs.S3FileSystem(), working_dir, qfinal_dir, delete_all)

def sync_local_to_s3(local_zarr: str,
                     s3_zarr: str) -> None:
    """
    Embarrassingly fast sync zarr to S3 (~3 minutes for 150k files). Note we only sink neccesarry files: all Qout/1.*, and all . files in the zarr
    """
    global S5CMD
    # all . files in the top folder (.zgroup, .zmetadata), and all . files in the Qout var (.zarray)
    files_to_upload = glob.glob(os.path.join(local_zarr, '.*')) 
    command = ""
    for f in files_to_upload:
        if 'Qout' in f:
            # If file is in Qout var, upload to Qout folder
            destination = os.path.join(s3_zarr, 'Qout')
        else:
            # Otherwise, upload to the top-level folder
            destination = s3_zarr
        command += f"{S5CMD} cp {f} {destination}\n"
    command += f"{S5CMD} sync --size-only --include=\"*1.*\" {local_zarr}/Qout {s3_zarr}/Qout/"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    # Check if the command was successful
    if result.returncode == 0:
        logging.info("Sync completed successfully.")
    else:
        logging.error(f"Sync failed. Error: {result.stderr}")

def setup_configs(working_directory: str, 
               configs_dir: str) -> None:
    """
    Setup all the directories we need, populate files
    """
    c_dir = os.path.join(working_directory, 'data','configs')
    os.makedirs(c_dir, exist_ok=True)
    if len(glob.glob(os.path.join(c_dir, '*','*.csv'))) == 0:
        result = subprocess.run(f"aws s3 sync {configs_dir} {c_dir}", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logging.info("Obtained config files")
        else:
            logging.error(f"Config file sync error:{result.stderr}")
 
def check_installations() -> None:
    """
    Check that the following are installed:
    aws cli
    s5cmd
    docker
    """
    global S5CMD
    class NotInstalled(Exception):
        pass
    try:
        subprocess.run(['aws', '--version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        raise NotInstalled('Please install the AWS cli: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html')
    
    try:
        subprocess.run([S5CMD, 'version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as e:
        S5CMD = os.environ['CONDA_PREFIX']
        if 'envs' not in S5CMD:
            S5CMD = os.environ['CONDA_PREFIX_2']
            if 'envs' not in S5CMD:
                raise e
        S5CMD = os.path.join(S5CMD, 'bin','s5cmd')
        subprocess.run([S5CMD, 'version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        raise NotInstalled('Please install s5cmd: `conda install s5cmd`')
    
    try:
        subprocess.run(['docker', '--version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        raise NotInstalled('Please install docker, and run "docker pull chdavid/rapid"')


def main(working_dir: str,
         retro_zarr: str,
         qfinal_dir: str,
         s3_zarr: str) -> None:
    """
    Assumes docker is installed and this command was run: docker pull chdavid/rapid.
    Assumes AWS CLI and s5cmd are likewise installed. 
    """
    nc, run_again = download_era5(working_dir, retro_zarr, cl)
    if not nc:
        return
    
    config_vpu_dirs = glob.glob(os.path.join(working_dir, 'data', 'configs', '*'))
    get_initial_qinits(s3fs.S3FileSystem(), config_vpu_dirs, qfinal_dir, working_dir)

    a_qfinal = glob.glob(os.path.join(working_dir, 'data','outputs','*','*Qfinal*.nc'))[0]
    cl.add_qinit(datetime.datetime.strptime(os.path.basename(a_qfinal).split('_')[2].split('.')[0], '%Y%m%d'))

    # For inflows files and multiprocess, for each 1GB of runoff data, we need ~ 6GB for peak memory consumption. Otherwise, some m3 files will never be written and no error is raised
    processes = min(multiprocessing.cpu_count(), round(psutil.virtual_memory().total * 0.8 /  (os.path.getsize(nc) * 6)))
    logging.info(f"Using {processes} processes for inflows")
    worker_lists = [config_vpu_dirs[i:i+processes] for i in range(0, len(config_vpu_dirs), processes)]
    with multiprocessing.Pool(processes) as pool:
        pool.starmap(inflow_and_namelist, [(working_dir, os.path.join(working_dir, 'data','namelists'), nc, w) for w in worker_lists])
    
    if len(glob.glob(os.path.join(working_dir, 'data','inflows','*','m3*.nc'))) != 125:
        logging.error('Not all of the m3 files were generated!!!')
        return

    # Run rapid
    logging.info('Running rapid')
    rapid_result = subprocess.run([f'sudo docker run --rm --name rapid --mount type=bind,source={os.path.join(working_dir, 'data')},target=/mnt/data \
                        chdavid/rapid python3 /mnt/data/runrapid.py --rapidexec /home/rapid/src/rapid --namelistsdir /mnt/data/namelists'],
                        shell=True,
                        capture_output=True,
                        text=True)
    
    if rapid_result.returncode != 0:
        class RapidError(Exception):
            pass
        raise RapidError(rapid_result.stderr)
    logging.info(rapid_result.stdout)
        
    # Build the week dataset
    qouts = glob.glob(os.path.join(working_dir, 'data','outputs','*','Qout*.nc'))
    
    with xr.open_mfdataset(qouts, 
                        combine='nested', 
                        concat_dim='rivid',
                        preprocess=drop_coords, 
                        parallel=True).reindex(rivid=xr.open_zarr(retro_zarr).rivid) as ds:

        append_week(ds, retro_zarr)

    return

    if run_again:
        cl.add_message("Succeeded, running again")
        cl.log_message('Pass')
        cl.clear()
        cleanup(working_dir, qfinal_dir)
        main(working_dir, retro_zarr, qfinal_dir, s3_zarr)
    
    # At last, sync to S3
    #sync_local_to_s3(retro_zarr, s3_zarr)
    S5CMD
    result = subprocess.run(f"{S5CMD} cp {retro_zarr} s3://geoglows-scratch", shell=True, capture_output=True, text=True)

    # Check if the command was successful
    if result.returncode == 0:
        logging.info("Sync completed successfully.")
    else:
        logging.error(f"Sync failed. Error: {result.stderr}")

if __name__ == '__main__':
    start = time.time()
    working_directory = '/home/ubuntu/'
    # The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab
    #retro_zarr = '/home/ubuntu/retro_backup/geoglows_v2_retrospective.zarr'
    retro_zarr = '/home/ubuntu/2022_test.zarr' # Local zarr to append to
    s3_zarr = 's3://geoglows-scratch/retrospective.zarr' # Zarr located on S3
    qfinal_dir = 's3://geoglows-scratch/retro' # Directory containing subdirectories, containing Qfinal files
    configs_dir = 's3://geoglows-v2/configs' # Directory containing subdirectories, containing the files to run RAPID. Only needed if running this for the first time

    cl = CloudLog()
    status = 'Pass'
    error = None

    xr.set_options(engine='h5netcdf')

    try:
        if not os.path.exists(retro_zarr):
            logging.error(f"{retro_zarr} does not exist!")
            cl.log_message('Fail', f"{retro_zarr} does not exist!")
            exit()
        if not os.path.exists(os.path.join(working_directory, 'data', 'runrapid.py')):
            msg = f"Please put 'runrapid.py' in {working_directory}/data so that RAPID may use it"
            logging.error(msg)
            cl.log_message('Fail', msg)
            exit()

        check_installations()
        setup_configs(working_directory, configs_dir)
        cleanup(working_directory, qfinal_dir)
        main(working_directory, retro_zarr, qfinal_dir, s3_zarr)
        cleanup(working_directory, qfinal_dir)
    except Exception as e:
        error = traceback.format_exc()
        logging.error(error)
        status = 'Fail'
        cleanup(working_directory, qfinal_dir)
    finally:
        logging.info(f'FINISHED: {round((time.time() - start) / 60, 1)} minutes')
        cl.log_message(status, error)
        #os.system("sudo shutdown -h now")
