#!/usr/bin/env python3
import glob
import os 
import csvreader
import pandas as pd
import boto3
import io
import sys
from optparse import OptionParser
import config


def process_files(options, yamlfile):
    local_config = config.Config(yamlfile)
    if not local_config.validate():
        print(f'{yamlfile} is not valid')
        return

    csvread = csvreader.CsvReader(local_config)
    files_to_include = []
    files_to_exclude = []

    if isinstance(options.getzip,str):
        print(f'Downloading zip')
        values = options.getzip.split(":")
        csvread.get_s3files(values[0], values[1])
        return

    if isinstance(options.stage, str):
        print(f"creating stage {options.stage}")
        csvproc.create_stage()

    if options.includefilepath and os.path.isfile(options.includefilepath):
        print(f'processing file to include {options.includefilepath}')
        files_to_include = [line.strip() for line in open(options.includefilepath, 'r')]

    if options.excludefilepath and os.path.isfile(options.excludefilepath):
        print(f'processing file to exclude {options.excludefilepath}')
        files_to_exclude = [line.strip() for line in open(options.excludefilepath, 'r')]

    if options.s3zip:
        print(f'Will process s3 zip {options.s3zip}')
        values = options.s3zip.split(":")
        csvread.process_s3zipfile(values[0], values[1], files_to_include, files_to_exclude)
    elif options.s3files:
        print(f'Will process s3 files {options.s3files}')
        values = options.s3files.split(":")
        csvread.process_s3files(values[0], values[1], files_to_include, files_to_exclude)
    elif options.files:
        print(f'Will process files {options.files}')
        csvread.process_allfiles(options.files, files_to_include, files_to_exclude)
    elif options.zipfile and os.path.isfile(options.zipfile):
        print(f'Will process zip {options.zipfile}')
        csvread.process_zipfile(options.zipfile,  files_to_include, files_to_exclude)
    elif options.table_list and os.path.isfile(options.table_list):
        print("Getting table row couts")
        csvread.get_rowsin_table(options.table_list)



# =================================================================
#                   Main
# =================================================================
def main():
    parser = OptionParser('''Script that inserts CSV files ito snowflake creating a table for them in the exact same format''')

    parser.add_option(
        "-s",
        "--s3zip",
        dest="s3zip",
        help="archivename:path/to/the/zip")

    parser.add_option(
        "-b",
        "--s3files",
        dest="s3files",
        help="bucketname:path/for/the/prefix/to/filter")

    parser.add_option(
        "-f",
        "--files",
        dest="files",
        help="path that returns csv files")

    parser.add_option(
        "-z",
        "--zipfile",
        dest="zipfile",
        help="path to local zip file to process")

    parser.add_option(
        "-t",
        "--stage",
        dest="stage",
        help="Pass a stage name if it needs to be created")

    parser.add_option(
        "-i",
        "--include",
        dest="includefilepath",
        help="Path to a file that lists files to be included")

    parser.add_option(
        "-e",
        "--exclude",
        dest="excludefilepath",
        help="Path to a file that lists files to be excluded")

    parser.add_option(
        "-c",
        "--config",
        dest="configpath",
        default="./config.yaml",
        help="Path to a yaml that gives settings")
    
    parser.add_option(
        "-v",
        "--test",
        dest="table_list",
        help="list of tables to get record count")

    parser.add_option(
        "-g",
        "--getzip",
        dest="getzip",
        help="ucketname:Path to s3 zip")

    (options, args) = parser.parse_args()

    if options.configpath and os.path.isfile(options.configpath):
        process_files(options, options.configpath)
    else:
        print("Need path to config yaml file")

if __name__ == '__main__':
    main()


