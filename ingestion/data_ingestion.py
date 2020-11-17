import sgs
import datetime
from google.cloud import storage
import pandas as pd
from io import StringIO


class ingestion:
    def __init__(self, config):
        """
        """
        self.gcs_client = storage.Client()
        self.data = pd.DataFrame()

        self.serie_name = config['serie']
        self.date = config['date']
        self.path = config['raw_path']
        self.bucket = config['bucket']

    def _get_data(self):
        """
        """
        print("Getting Data...")
        self.data = sgs.time_serie(self.serie_name, 
                                           start = self.date, 
                                           end = self.date)

        print(f"Done! {self.data.shape[0]} rows were collected")

        return self.data    

    def _upload_to_raw(self):
        """
        """

        csv_buf = StringIO()
        self.data.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)

        bucket = self.gcs_client.bucket(self.bucket)

        full_path = f"{self.path}/from-{self.start_date}_to_{self.end_date}.csv"

        blob = bucket.blob(full_path)
        
        try:
            blob.upload_from_string(csv_buf.getvalue())
            print("Deu bom")
            return f'Success! File was uploaded to {full_path}'

        except Exception as e:
            print(e)
            return f'Failure! Details: {e}'

    def pipeline(self):
        self._get_data()
        self._upload_to_raw()

def main():

    JOB_CONFIG = {'serie': 12,
                  'date': (datetime.datetime.now() - datetime.timedelta(days=1)).date(),
                  'bucket': 'bcb-series',
                  'raw_path': 'bovespa_indice'
                  }

    inj = ingestion(config=JOB_CONFIG)

    inj.pipeline()

if __name__ == '__main__':
    main()