from google.cloud import storage
from google.cloud import bigquery
import json
import gcsfs
import datetime
import pandas as pd
import io
import os

class ExtractTransformLoad:
    def __init__(self, bucket, output_prefix):
        """
        Description
            Constructor method

        Args
            bucket (String):
            output_prefix (String):
        """
        self.bq_client = bigquery.Client()
        self.gcs_client = storage.Client()
        self.bucket = bucket
        self.output_prefix = output_prefix
        self.data = self.__read_data()

    def __read_data(self):
        """
        Description
            Reads csv file and converts to a Panda DataFrame 

        Returns
            df (Pandas DataFrame): Returns a pandas DataFrame with all  data
        """
        global file_name
        file_name= "serie-12_02-01-2018_31-12-2018"
        full_path = f"gs://bcb-series/cdi/{file_name}.csv"

        return pd.read_csv(full_path)

    def __clean_data(self):
        pass

    def __convert_to_parquet(self):
        """
        Description
            Converts a Pandas DataFrame to parquet file and stores it in Bytes IO

        Returns
            parquet_file (Bytes IO): Parquet file in Bytes IO
        """

        parquet_file = io.BytesIO()
        self.data.to_parquet(parquet_file)
        parquet_file.seek(0)
        
        return parquet_file

    def __upload_file(self):
        """
        Description
            Uploads the parquet file to Google Cloud Storage

        Returns
            True if file has been uploaded. Otherwise fails     
        """

        bucket = self.gcs_client.bucket(self.bucket)
        blob = bucket.blob(os.path.join(self.output_prefix, f'{file_name}.parquet'))
        
        try:
            blob.upload_from_file(self.__convert_to_parquet())
            print(f"Success! File was been uploaded to {self.output_prefix}")
            return True

        except Exception as e:
            print(f'Failure! Details: {e}')
            return False

    def __insert_rows(self):
        """
        Description
            Inserts all rows from a DataFrame into a BigQuery table

        Returns:
            True if the rows was successfuly inserted. Otherwise, False
        """

        # your-project.your_dataset.your_table_name
        #table_id = 'hopeful-adapter-285018.ecommerce.order_address'
        table_id = "trainning-cesar.time_series.cdi"

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        try:
            # Load data to BQ
            job = self.bq_client.load_table_from_dataframe(self.data, table_id, job_config=job_config)
            job.result()

            print(f"Success! {self.data.shape[0]} rows inserted.")
            return True
            
        except Exception as e:
            print(f"Erro! Details: {e}.")
            return False

    def pipeline(self):
        self.__clean_data()
        self.__upload_file()
        self.__insert_rows()

def transform_data():

    BUCKET = "bcb-series"
    OUTPUT_PREFIX = "normalized-data/cdi"

    etl = ExtractTransformLoad(bucket=BUCKET, output_prefix=OUTPUT_PREFIX)

    try:
        etl.pipeline()
        return 'Pipeline was successfully executed'

    except Exception as e:
        return f'Error during pipeline execution! Details:{e}'

if __name__ == '__main__':
    transform_data()