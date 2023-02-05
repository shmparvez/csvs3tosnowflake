#!/usr/bin/env python3
import glob
import pandas as pd
import os 
import csvprocessor
import config
import boto3
from zipfile import ZipFile 
import io


class CsvReader:

    def __init__(self, config):
        self.config = config
        self.csvproc = csvprocessor.CsvProcessor(self.config)
        self.client = boto3.client('s3', aws_access_key_id=self.config.ACCESS_KEY,
                            aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
                            region_name=self.config.REGION)
        self.session = boto3.session.Session(
                            aws_access_key_id=self.config.ACCESS_KEY,
                            aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
                            region_name=self.config.REGION)
       
    def process_s3files(self, bucket_name, prefix, files_to_include, files_to_exclude):
        response = self.client.list_objects(Bucket=bucket_name, Prefix=prefix)
        for content in response.get('Contents', []):
            s3filepath = content.get('Key')
            name = s3filepath.rsplit('/', 1)[-1][:-4]
            if name in files_to_exclude or (len(files_to_include) > 0 and name not in files_to_include):
                print(f'Skipping {name}')
                continue
            filepath = f'./{name}.csv'
            self.client.download_file(bucket_name, s3filepath, filepath)
            self.process_csvfile(filepath, name)

    def get_s3files(self, bucket_name, prefix):
        response = self.client.list_objects(Bucket=bucket_name, Prefix=prefix)
        for content in response.get('Contents', []):
            s3filepath = content.get('Key')
            name = s3filepath.rsplit('/', 1)[-1]
            self.client.download_file(bucket_name, s3filepath, name)

    def process_allfiles(self, csvfilepath, files_to_include, files_to_exclude):
        files = glob.glob(csvfilepath)
        for file in files:
            name = file.rsplit('/', 1)[-1][:-4]
            if name in files_to_exclude or (len(files_to_include) > 0 and name not in files_to_include):
                print(f'Skipping {name}')
                continue
            self.process_csvfile(file, name)
            #os.remove(file)

    def process_csvfile(self, file, name):
        print(f'Processing {name}')
        try:
            df = pd.read_csv(file, on_bad_lines='warn', nrows=3)
            #print(len(df.index))
            cols = ",".join([f'{c} date default 1/1/1950 '
                                if c.lower().endswith("_dt") 
                                else f'{c} varchar' for c in df.columns.values])
            self.csvproc.process_csvfile(file, name, cols)
            print(f'Processed {name} with cols: {cols}')
        except Exception as err:
            print(f"Failed to process {name} {err=}, {type(err)=}")

    def process_zipfile(self, zipfilepath, files_to_include, files_to_exclude):
        with ZipFile(zipfilepath, mode='r') as zipf:
            for subfile in zipf.namelist():
                name = subfile[:-4]
                if name in files_to_exclude or (len(files_to_include) > 0 and name not in files_to_include):
                    print(f'Skipping {name}')
                    continue
                filepath = zipf.extract(subfile)
                print(f"processing {subfile} at {filepath}")
                self.process_csvfile(filepath, name)
                 
    def process_s3zipfile(self, bucket_name, prefix, files_to_include, files_to_exclude):
        s3 = self.session.resource("s3")
        bucket = s3.Bucket(bucket_name)
        obj = bucket.Object(prefix)

        with io.BytesIO(obj.get()["Body"].read()) as tf:
            # rewind the file
            tf.seek(0)
            # Read the file as a zipfile and process the members
            with ZipFile(tf, mode='r') as zipf:
                for subfile in zipf.namelist():
                    #print(f'Checking if {subfile} is in {len(processed_files)}')
                    name = subfile[:-4]
                    if name in files_to_exclude or (len(files_to_include) > 0 and name not in files_to_include):
                        print(f'Skipping {name}')
                        continue
                    filepath = zipf.extract(subfile)
                    print(f"processing {subfile} at {filepath}")
                    self.process_csvfile(filepath, name)

    def get_rowsin_table(self, tablenamefile):
        names = [line.strip() for line in open(tablenamefile, 'r')]
        self.csvproc.test_table(names)


