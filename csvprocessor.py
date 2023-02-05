#!/usr/bin/env python3
import snowflake.connector
import config
import json
import glob
import os 


class CsvProcessor:
    def __init__(self, config):
        self.config = config
        self.conn = snowflake.connector.connect(
            user=self.config.User,
            password=self.config.Password,
            account=self.config.Account
            )

    def execute_query(self, queries):
        cursor = self.conn.cursor()
        for query in queries:
            print(f'executing {query}')
            cursor.execute(query)
            for c in cursor:
                print(c)
        cursor.close()

    def execute_one_query(self, query, table_name):
        cursor_list = self.conn.execute_string(query, return_cursors=True)
        for row in cursor_list[-1]:
            print(f'{table_name}:{row}')

    def create_stage(self):
        sql_queries = []
        sql_queries.append(f'use role SYSADMIN')
        sql_queries.append(f'use database {self.config.Database}')
        sql_queries.append(f'use warehouse {self.config.Warehouse}')
        sql_queries.append(f'use schema {self.config.Schema}')
        sql_queries.append(f'create stage CCBHISTORICAL.CCB.DATA_STAGE file_format = (type = ""csv"" field_delimiter = "","" skip_header = 1)')
        # try:
        self.execute_query(sql_queries)
        print('Create stage successfully')
        # except:
        #     print('Stage not created since it already exists')
        #     pass

    def process_csvfile(self, csvfile, name, cols):
        sql_queries = []
        
        sql_queries.append(f'use role SYSADMIN')
        sql_queries.append(f'use database {self.config.Database}')
        sql_queries.append(f'use warehouse {self.config.Warehouse}')
        sql_queries.append(f'ALTER WAREHOUSE {self.config.Warehouse} RESUME IF SUSPENDED')
        sql_queries.append(f'use schema {self.config.Schema}')
        sql_queries.append(f'drop table if exists {name}')
        sql_queries.append(f'create table {name}({cols})')
        sql_queries.append("PUT file://" + csvfile + " @CCBHISTORICAL.CCB.DATA_STAGE auto_compress=true OVERWRITE = TRUE")
        sql_queries.append(f'copy into {name} from @CCBHISTORICAL.CCB.DATA_STAGE/{csvfile}.gz file_format = (type = "csv" field_delimiter = "," skip_header = 1 field_optionally_enclosed_by = \'"\' trim_space = false  NULL_IF = (\'\\\\N\', \'NULL\', \'NUL\', \'\') ) ')
        self.execute_query(sql_queries)


    def test_table(self, files_to_include):
        sql_queries = []
        for name in files_to_include:
            sql_queries.append(f'use role SYSADMIN')
            sql_queries.append(f'use database {self.config.Database}')
            sql_queries.append(f'use warehouse {self.config.Warehouse}')
            sql_queries.append(f'ALTER WAREHOUSE {self.config.Warehouse} RESUME IF SUSPENDED')
            sql_queries.append(f'use schema {self.config.Schema}')
            sql_queries.append(f'select count(*) from {self.config.Database}.{self.config.Schema}.{name}')
            self.execute_one_query(";".join(sql_queries), name)
            sql_queries = []

