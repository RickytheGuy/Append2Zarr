import datetime
import json
import os
import time
import logging

import boto3
import numpy as np

# LOG_GROUP_NAME = os.getenv('AWS_LOG_GROUP_NAME')
# LOG_STREAM_NAME = os.getenv('AWS_LOG_STREAM_NAME')
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID') 
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') 
REGION = os.getenv('AWS_REGION')

class CloudLog():
    """
    Class made to log messages to AWS Cloudwatch
    """
    def __init__(self, 
                 
                 ACCESS_KEY_ID = ACCESS_KEY_ID,
                 SECRET_ACCESS_KEY= SECRET_ACCESS_KEY,
                 REGION= REGION,
                 save_path: str = 'log.json',
                 ):
        self.save_path = save_path
        self.start = datetime.datetime.now().ctime()
        self.last_date = None
        self.qinit = None
        self.time_period = None
        self.message = ''
        # Create a CloudWatch Logs client
        if not REGION:
            REGION = 'us-west-2'

        self.client = boto3.client(
            'logs',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
            region_name=REGION
        )

    def add_last_date(self, date) -> None:
        if isinstance(date, np.datetime64):
            self.last_date = np.datetime_as_string(date,unit='h' )
        else:
            self.last_date = str(date)

    def add_qinit(self, qinit: datetime.datetime) -> None:
        self.qinit = qinit.strftime('%m/%d/%Y')

    def add_time_period(self, time_range: list[datetime.datetime]) -> None:
        self.time_period = f"{time_range[0].strftime('%m/%d/%Y')} to {time_range[-1].strftime('%m/%d/%Y')}"

    def add_message(self, msg) -> None:
        self.message = str(msg)

    def clear(self):
        self.start = datetime.datetime.now().ctime()
        self.last_date = None
        self.qinit = None
        self.time_period = None
        self.message = ''


    def log_message(self, status: str, error: Exception = None) -> None:
        log_message = {'Start time': self.start,
                  'End time':datetime.datetime.now().ctime(),
                  'Status': status,
                  'Message': self.message,
                  'Error': str(error),
                  'Most recent date in Zarr':self.last_date,
                  'Qinit used': self.qinit,
                  'Time period': self.time_period
                  }

        # Send the log message to CloudWatch
        try:
            response = self.client.put_log_events(
                logGroupName='AppendWeekLog',
                logStreamName='EC2',
                logEvents=[
                    {
                        'timestamp': int(round(time.time() * 1000)),
                        'message': json.dumps(log_message)
                    }
                ]
            )
        except Exception as e:
            logging.error(e)

        return response
