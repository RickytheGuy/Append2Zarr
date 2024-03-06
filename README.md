# retrospective-update
Using two EC2 instances, download ERA5 data on one, and use that data on the other to run RAPID and append the output data to a zarr file on an Amazon S3 Bucket.

## Setup
### Create your IAM role
1. Go to the IAM service page, and click on "Roles" in the navigation pane.
2. Click the "Create role" button.
3. Under "Trusted entity type", select "AWS Service". Under "Use case", select "EC2". CLick "Next"
4. Search and select "AmazonS3FullAccess" and "AmazonEC2FullAccess". CLick "Next"
5. Give your role a name. Click "Create role"

### Launch the computation instance
1. On the "Launch an instance" page, select Ubuntu as the OS in "Application and OS Images (Amazon Machine Image)". 
2. Under "Instance Type", select your desired instance type. A possible choice is m5.2xlarge, which has eight vCPUs and 32 GiB of total memory. More CPUs generally equates to quicker execution, due to the scripts' highly parallelized nature. Additionally, this instance type has a better network performance.
3. Under "Key pair (login)", choose or create a key pair.
4. Under "Network settings", choose or create a security group. Enable "Auto-assign public IP".
5. Under "Configure Storage", choose an appropriate amount of storage to hold the OS and a few GBs of data.
6. Under "Advanced details", selct the IAM instance profile created earlier. 
7. Finally, click the "Launch instance" button

### Setup the computation instance
1. Use SSH to connect to your instance (it should automatically start after launching). The command should look something like "ssh -i PATH/TO/KEY-PAIR.PEM ubuntu@IP-ADDRESS". The IP address can be viewed in the Instances page of the EC2 service.
2. Run the following code in the EC2 isntance's terminal:
``` 
cd $HOME
sudo apt-get install git
git clone https://github.com/RickytheGuy/retrospective-update/archive/refs/heads/main.zip
mv retrospective-update-main retrospective-update
sudo chmod +x retrospective-update/computation_scripts/install.sh
retrospective-update/computation_scripts/install.sh
```
3. Create a 1000GB volume (read how to do that [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-volume.html)). Setup the instance to automatically attatch that volume on each startup (instructions under "Automatically mount an attached volume after reboot" [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html)). Remember the mount location you choose (we recommend simply using "/mnt"). Attach the volume.
4. Add a copy of the retrospective zarr to the volume. 
5. Create a file in the ".aws" folder called "credentials". Place into this file your aws access key id and secret access key: (maybe?)
```
aws_access_key_id=YOUR_KEY_ID
aws_secret_access_key=YOUR_KEY
region=YOUR_REGION
```
6. Fill out the .profile file found in retrospective-update/computation_scripts/
7. Stop the instance.

### Launch the downloader instance
1. Go to the EC2 service page, and click on the "Launch instance" button.
2. On the "Launch an instance" page, select Ubuntu as the OS in "Application and OS Images (Amazon Machine Image)". 
3. Under "Instance Type", select your desired instance type. A possible choice is m7a.medium, which has one vCPU and 4 GiB of memory. This is sufficient for the downloader. This instance was selected amnong others because of its high network performance (up to 12.5 Gibps).
4. Under "Key pair (login)", choose or create a key pair.
5. Under "Network settings", choose or create a security group (make sure you either have or make your key-pair .pem file from this step!!!). Enable "Auto-assign public IP".
6. Under "Configure Storage", choose an appropriate amount of storage to hold the OS.
7. Under "Advanced details", selct the IAM instance profile created earlier. 
8. Finally, click the "Launch instance" button

### Setup the downloader instance
1. Use SSH to connect to your instance (it should automatically start after launching). The command should look something like "ssh -i PATH/TO/KEY-PAIR.PEM ubuntu@IP-ADDRESS". The IP address can be viewed in the Instances page of the EC2 service.
2. Run the following code in the EC2 isntance's terminal:
``` 
cd $HOME
sudo apt-get install git
git clone https://github.com/RickytheGuy/retrospective-update/archive/refs/heads/main.zip
mv retrospective-update-main retrospective-update
sudo chmod +x retrospective-update/downloader_scripts/install.sh
retrospective-update/downloader_scripts/install.sh
```
3. Create a 100GB volume (read how to do that [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-volume.html)). Setup the instance to automatically attatch that volume on each startup (instructions under "Automatically mount an attached volume after reboot" [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html)). Remember the mount location you choose (we recommend simply using "/mnt")
4. Create a file in the ".aws" folder called "credentials". Place into this file your aws access key id and secret access key:
```
aws_access_key_id=YOUR_KEY_ID
aws_secret_access_key=YOUR_KEY
region=YOUR_REGION
```
5. Fill out the .profile file found in retrospective-update/downloader_scripts/
6. Stop the instance.

