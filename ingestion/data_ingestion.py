import sgs
import datetime
from google.cloud import storage
import pandas as pd
from io import StringIO
import os

class ingestion:
    def __init__(self, config):
        """
        Description
            Constructor Method

        Args
            config (dict): Dict with data ingestion configuration 
                serie (Integer): BCB Serie number to be collected
                date (String): Date to collect data
                bucket (String): Bucket name to store data

        Returns
            Instance of the class
        """
        
        self.gcs_client = storage.Client()
        self.data = pd.DataFrame()

        self.serie_name = config['serie']
        self.start_date = config['start_date']
        self.end_date = config['end_date']
        self.path = config['raw_path']
        self.bucket = config['bucket']

    def _get_data(self):
        """
        Descrition
            Uses sgs library to get data from BCB based on config setted
        """
        
        print("Getting Data...")
        self.data = sgs.dataframe(self.serie_name, 
                                   start = self.start_date, 
                                   end = self.end_date)

        print(f"Done! {self.data.shape[0]} rows were collected")
        
        self.data.reset_index(inplace=True)
        self.data.columns = ['date', 'cdi']

        return self.data    

    def _upload_to_raw(self):
        """
        Description 
            Uploads the data to specified raw path in csv format
        """

        csv_buf = StringIO()
        self.data.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)

        bucket = self.gcs_client.bucket(self.bucket)

        file_name = f'serie-{self.serie_name}_{self.start_date.replace("/","-")}_{self.end_date.replace("/","-")}.csv'
        full_path = os.path.join(self.path, file_name)

        blob = bucket.blob(full_path)
        
        try:
            blob.upload_from_string(csv_buf.getvalue())
            
            return f'Success! File was uploaded to {full_path}'

        except Exception as e:
            return f'Failure! Details: {e}'

    def pipeline(self):
        """
        Description
            Executes the data ingestion pipeline
        """

        self._get_data()
        self._upload_to_raw()

def get_data():

    JOB_CONFIG = {'serie':12 ,
                  'start_date': '02/01/2018',
                  'end_date': '31/12/2018',
                  'bucket': 'bcb-series',
                  'raw_path': 'raw-data/cdi'
                  }

    inj = ingestion(config=JOB_CONFIG)

    try:
        inj.pipeline()
        return 'Pipeline was successfully executed'

    except Exception as e:
        return f'Error during pipeline execution! Details:{e}'

get_data()