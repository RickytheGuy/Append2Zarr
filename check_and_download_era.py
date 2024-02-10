import cdsapi
import dask
import xarray as xr
import numpy as np
import pandas as pd

import multiprocessing
import logging
import glob
import os
from datetime import datetime
from typing import Tuple

from cloud_logger import CloudLog

def download_era5(working_dir: str,
                  retro_zarr: str,
                  cl: CloudLog) -> Tuple[str, bool]:
    """
    Downloads era5 runoff data to {working_dir}/data/era5_data. Uses the retrospective zarr to query 7 days of data. Logs to the CloudLog class. 
    Converts hourly runoff to daily. Saves as {working_dir}/data/era5_data/era5_hourly_combined.nc
    """
    os.makedirs(os.path.join(working_dir, 'data','era5_data'), exist_ok=True)
    try:
        c = cdsapi.Client()
    except:
        cdsapirc_file = os.path.join(working_dir, '.cdsapirc')
        if not os.path.exists(cdsapirc_file):
            raise FileNotFoundError(f"Cannot find a .cdsapirc in {working_dir}")
        os.environ['CDSAPI_RC'] = os.path.join(working_dir, '.cdsapirc')
        c = cdsapi.Client()

    try:
        last_date = xr.open_zarr(retro_zarr)['time'][-1].values
    except IndexError:
        last_date = xr.open_zarr(retro_zarr)['time'].values
    cl.add_last_date(last_date)

    run_again = False
    if pd.to_datetime(last_date + np.timedelta64(14,'D')) > datetime.now():
        # If the last date in the zarr file is within 2 weeks of today, then prohibit the script from continuing
        # since the ECMWF has a lag time of 6 days ish (a week for rounding)
        logging.warning('Script executed too soon! Exiting script...')
        return ('', False)
    if pd.to_datetime(last_date + np.timedelta64(21,'D')) < datetime.now():
        # If there are more than two weeks of data, we'll need to run again
        run_again = True

    number_of_days = 7

    times_to_download = pd.to_datetime([last_date + np.timedelta64(i,'D') for i in range(1,number_of_days + 1)])
    cl.add_time_period(times_to_download)
    years = {d.year for d in times_to_download}
    months = {d.month for d in times_to_download}

    # Create a request for each month for each year, using only the days in that month. This will support any timespan
    requests = []
    num_requests = 0
    for year in years:
        for month in months:
            if month in {t.month for t in times_to_download if t.year == year}:
                requests.append((working_dir, c, year, month, sorted({t.day for t in times_to_download if t.year == year and t.month == month}), num_requests))
                num_requests += 1

    # Use multiprocessing so that we can make two requests at once if need be
    with multiprocessing.Pool(num_requests) as pool:
        pool.starmap(retrieve_data, requests)

    # Check that the number of files downloaded match the number of requests
    ncs = glob.glob(os.path.join(working_dir, 'data', 'era5_data', '*.nc'))
    if len(ncs) != num_requests:
        raise ValueError(f"Found {len(ncs)} in {os.path.join(working_dir, 'data', 'era5_data')}, but expected {num_requests}")
 
    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        ds = (xr.open_mfdataset(ncs, 
                            concat_dim='time', 
                            combine='nested', 
                            parallel=True, 
                            chunks = {'time':'auto', 'lat':'auto','lon':'auto'}, # Chunk to prevent weird Slicing behavior and missing data
                            preprocess=process_expver_variable)
                            .sortby('time')
                            .groupby('time.date')
                            .sum(dim='time') # Convert to daily
                            .rename({'date':'time'})
        )
        ds['time'] = ds['time'].values.astype('datetime64[ns]')
        
    # Make sure all days were downloaded
    if ds['time'].shape[0] != number_of_days: 
        raise ValueError('The entire time series was not downloaded correctly... likely ECMWF has not updated their datasets')
    
    ds.to_netcdf(os.path.join(working_dir, 'data', 'era5_data', 'era5_hourly_combined.nc'))

    # Remove uncombined netcdfs. 
    for nc in ncs:
        os.remove(nc)
    ncs = glob.glob(os.path.join(working_dir, 'data', 'era5_data', '*.nc'))

    return (ncs[0], run_again)
    
def retrieve_data(working_dir: str,
                  client: cdsapi.Client, 
                  year: int, 
                  month: int, 
                  days: list[int], 
                  index: int = 0,) -> None:
    """
    Retrieves era5 data. Note that if future dates are requested, 
    cdsapi returns all data for dates that ARE available, ignoring future/not-done dates
    """        
    client.retrieve(
                f'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'format': 'netcdf',
                    'variable': 'runoff',
                    'year': year,
                    'month': str(month).zfill(2),
                    'day': [str(day).zfill(2) for day in days],
                    'time': [f'{x:02d}:00' for x in range(0, 24)],
                }, 
                target=os.path.join(working_dir, 'data', 'era5_data', f'era5_hourly_{index}.nc')
            )


def process_expver_variable(ds: xr.Dataset,
                            runoff: str = 'ro') -> xr.DataArray:
    """
    Function used in opening the downloaded files. If 'expver' is found, raise an error, since we should not use these files.
    """
    if 'expver' in ds.dims:
        raise ValueError('"expver" found in downloaded ERA files')
    return ds[runoff]
    #     ds = ds[runoff].isel(expver=0).fillna(ds[runoff].isel(expver=1))
    # return ds[runoff]