import csv
import datetime
import json
import happybase
import logging
import os
import re
from hdfs import InsecureClient


def extract_timestamp_from_filename(file_name):
    date_str = re.search(r'\d{4}_\d{2}_\d{2}', file_name).group(0)
    # Convert date string to timestamp using datetime module
    ts = datetime.datetime.strptime(date_str, '%Y_%m_%d').timestamp()
    return str(int(ts))


class PersistenceLoader:
    def __init__(self, host, hbase_port, hdfs_port, hdfs_user, temporal_landing_dir, temporal_landing_csv,
                 temporal_landing_json, logger):
        self.connection = None
        self.host = host
        self.hbase_port = int(hbase_port)
        self.hdfs_port = hdfs_port
        self.hdfs_user = hdfs_user
        self.logger = logger or logging.getLogger(__name__)
        self.temporal_landing_dir = temporal_landing_dir.replace('\\', '/')
        self.temporal_landing_csv = temporal_landing_csv.replace('\\', '/')
        self.temporal_landing_json = temporal_landing_json.replace('\\', '/')
        self.hdfs_client = InsecureClient(f'http://{self.host}:{self.hdfs_port}', user=self.hdfs_user)
        self.logger.info(f"Connection to HDFS has been established successfully.")
        try:
            self.connect()
        except Exception as e:
            self.logger.exception(e)
            self.close()

    def connect(self):
        self.logger.info(f"Connecting to HBase at {self.host}:{self.hbase_port}")
        con = happybase.Connection(self.host, self.hbase_port)
        con.open()
        self.connection = con
        self.logger.info("Successfully connected to HBase")

    def close(self):
        if self.connection is not None:
            self.logger.info("Closing HBase connection")
            self.connection.close()
            self.connection = None
            self.logger.info("HBase connection closed")

    def create_table(self, table_name, flag=False):
        try:
            # Check if the table exists
            if table_name.encode() in self.connection.tables():
                self.logger.info(f"Table '{table_name}' already exists.")
            elif not flag:
                # Create the table with 'data' and 'metadata' column families
                self.connection.create_table(table_name, {'data': {}, 'metadata': {}})
                self.logger.info(f"Table '{table_name}' created.")
            else:
                # Create the table with 'idealista_data', 'income_data', 'metadata' column families
                self.connection.create_table(table_name, {'idealista_data': {}, 'income_data': {}, 'metadata': {}})
                self.logger.info(f"Table '{table_name}' created.")
        except Exception as e:
            self.close()
            self.logger.exception(e)


    def load_opendatabcn_income(self):

        try:
            # Create a table for the data source
            table_name = 'opendatabcn-income'
            self.create_table(table_name)

            # Get a handle to the table
            table = self.connection.table(table_name)

            # Iterate over the CSV files in the temporal landing zone
            csv_dir = os.path.join(self.temporal_landing_dir, self.temporal_landing_csv, table_name).replace('\\', '/')
            filenames = self.hdfs_client.list(csv_dir)
            for filename in filenames:
                if filename.endswith('.csv'):
                    with self.hdfs_client.read(os.path.join(csv_dir, filename).replace('\\', '/'), encoding='utf-8') as f:
                        # Iterate over the rows in the CSV file
                        reader = csv.reader(f, delimiter=',', quotechar='"')
                        num_rows = 0
                        column_names = []
                        for row in reader:

                            if num_rows == 0:
                                column_names = row
                                num_rows += 1
                                continue

                            # Parse the row and construct the row key
                            valid_year = row[0]
                            district_code = row[1]
                            neighborhood_code = row[3]
                            row_key = f"{table_name}_{district_code}_{neighborhood_code}_{valid_year}"

                            # Construct a dictionary of column name to value
                            data_dict = {
                                'data:District_Name': row[2].encode('utf-8'),
                                'data:Neighborhood_Name': row[4].encode('utf-8'),
                                'data:Population': row[5].encode('utf-8'),
                                'data:Index RFD Barcelona = 100': row[6].encode('utf-8'),
                            }

                            # Insert the data into HBase
                            table.put(row_key.encode('utf-8'), data_dict)

                            # Update metadata
                            num_rows += 1

                        # Insert metadata into HBase
                        metadata_dict = {
                            'metadata:num_rows': str(num_rows - 1).encode('utf-8'),
                            'metadata:num_cols': str(len(column_names)).encode('utf-8'),
                            'metadata:column_names': (','.join(column_names)).encode('utf-8'),
                            'metadata:ingestion_date': (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                        ).encode('utf-8')
                        }
                        table.put(f"{table_name}_{valid_year}_metadata".encode('utf-8'), metadata_dict)
                        self.logger.info(f"Imported {num_rows} rows into table '{table_name}' from file: {filename}.")
        except Exception as e:
            self.close()
            self.logger.exception(e)

    def load_veh_index_motoritzacio(self):
        try:
            # Create a table for the data source
            table_name = 'veh_index_motoritzacio'
            self.create_table(table_name)

            # Get a handle to the table
            table = self.connection.table(table_name)

            # Iterate over the CSV files in the temporal landing zone
            csv_dir = os.path.join(self.temporal_landing_dir, self.temporal_landing_csv, table_name).replace('\\', '/')
            filenames = self.hdfs_client.list(csv_dir)
            for filename in filenames:
                if filename.endswith('.csv'):
                    with self.hdfs_client.read(os.path.join(csv_dir, filename).replace('\\', '/'), encoding='utf-8') as f:
                        # Iterate over the rows in the CSV file
                        reader = csv.reader(f, delimiter=',', quotechar='"')
                        num_rows = 0
                        column_names = []
                        for row in reader:

                            if num_rows == 0:
                                column_names = row
                                num_rows += 1
                                continue

                            # Parse the row and construct the row key
                            valid_year = row[0]
                            district_code = row[1]
                            neighborhood_code = row[3]
                            row_key = f"{table_name}_{district_code}_{neighborhood_code}_{valid_year}"

                            # Construct a dictionary of column name to value
                            data_dict = {
                                'data:District_Name': row[2].encode('utf-8'),
                                'data:Neighborhood_Name': row[4].encode('utf-8'),
                                'data:Seccio_Censal': row[5].encode('utf-8'),
                                'data:Tipus_Vehicle': row[6].encode('utf-8'),
                                'data:Index_Motoritzacio': row[7].encode('utf-8'),
                            }

                            # Insert the data into HBase
                            table.put(row_key.encode('utf-8'), data_dict)

                            # Update metadata
                            num_rows += 1

                        # Insert metadata into HBase
                        metadata_dict = {
                            'metadata:num_rows': str(num_rows - 1).encode('utf-8'),
                            'metadata:num_cols': str(len(column_names)).encode('utf-8'),
                            'metadata:column_names': (','.join(column_names)).encode('utf-8'),
                            'metadata:ingestion_date': (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                        ).encode('utf-8')
                        }
                        table.put(f"{table_name}_{valid_year}_metadata".encode('utf-8'), metadata_dict)
                        self.logger.info(f"Imported {num_rows} rows into table '{table_name}' from file: {filename}.")
        except Exception as e:
            self.close()
            self.logger.exception(e)

    def load_lookup_tables(self):
        try:
            # Create a table for the data source
            table_name = 'lookup_tables'
            self.create_table(table_name, True)

            # Get a handle to the table
            table = self.connection.table(table_name)

            # Iterate over the CSV files in the temporal landing zone
            csv_dir = os.path.join(self.temporal_landing_dir, self.temporal_landing_csv, table_name).replace('\\', '/')
            filenames = self.hdfs_client.list(csv_dir)
            for filename in filenames:
                if filename.endswith('.csv'):
                    source_name = filename.split('_')[0]
                    with self.hdfs_client.read(os.path.join(csv_dir, filename).replace('\\', '/'), encoding='utf-8') as f:
                        # Iterate over the rows in the CSV file
                        reader = csv.reader(f, delimiter=',', quotechar='"')
                        num_rows = 0
                        column_names = []
                        for row in reader:

                            if num_rows == 0:
                                column_names = row
                                num_rows += 1
                                continue

                            # Parse the row and construct the row key
                            district_id = row[4]
                            neighborhood_id = row[7]
                            row_key = f"{table_name}_{district_id}_{neighborhood_id}"

                            # Construct a dictionary of column name to value
                            data_dict = {
                                f'{source_name}_data:district': row[0].encode('utf-8'),
                                f'{source_name}_data:neighborhood': row[1].encode('utf-8'),
                                f'{source_name}_data:district_n_reconciled': row[2].encode('utf-8'),
                                f'{source_name}_data:district_n': row[3].encode('utf-8'),
                                f'{source_name}_data:neighborhood_n_reconciled': row[5].encode('utf-8'),
                                f'{source_name}_data:neighborhood_n': row[6].encode('utf-8'),
                            }

                            # Insert the data into HBase
                            table.put(row_key.encode('utf-8').strip(), data_dict)

                            # Update metadata
                            num_rows += 1

                        # Insert metadata into HBase
                        metadata_dict = {
                            'metadata:num_rows': str(num_rows - 1).encode('utf-8'),
                            'metadata:num_cols': str(len(column_names)).encode('utf-8'),
                            'metadata:column_names': (','.join(column_names)).encode('utf-8'),
                            'metadata:ingestion_date': (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                        ).encode('utf-8')
                        }
                        table.put(f"{table_name}_{source_name}_metadata".encode('utf-8'), metadata_dict)
                        self.logger.info(f"Imported {num_rows} rows into table '{table_name}' from file: {filename}.")
        except Exception as e:
            self.close()
            self.logger.exception(e)

    def load_idealista(self):
        try:
            # Create a table for the data source
            table_name = 'idealista'
            self.create_table(table_name, False)

            # Get a handle to the table
            table = self.connection.table(table_name)

            # Iterate over the JSON files in the temporal landing zone
            json_dir = os.path.join(self.temporal_landing_dir, self.temporal_landing_json, table_name).replace('\\', '/')
            filenames = self.hdfs_client.list(json_dir)
            for filename in filenames:
                if filename.endswith('.json'):
                    with self.hdfs_client.read(os.path.join(json_dir, filename).replace('\\', '/'), encoding='utf-8') as f:
                        data = json.load(f)
                        for property_dict in data:

                            # Extract necessary data
                            property_code = property_dict['propertyCode']
                            neighborhood = property_dict.get('neighborhood', None)
                            district = property_dict.get('district', None)
                            valid_time = extract_timestamp_from_filename(filename)
                            # Generate key and value
                            key = f"{property_code}_{district}_{neighborhood}_{valid_time}"
                            value = {k: v.encode('utf-8') if isinstance(v, str) else v for k, v in property_dict.items()
                                     if k not in ['propertyCode', 'neighborhood', 'district']}
                            # Add data to HBase table
                            table.put(key.encode(), {'data:property_info'.encode(): str(value).encode()})

                        num_rows = len(data)
                        # Insert metadata into HBase
                        metadata_dict = {
                            'metadata:num_rows': str(num_rows).encode('utf-8'),
                            'metadata:num_cols': str(len(data[0])).encode('utf-8'),
                            'metadata:column_names': (','.join(data[0].keys())).encode('utf-8'),
                            'metadata:ingestion_date': (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                        ).encode('utf-8')
                        }
                        table.put(f"{table_name}_{valid_time}_metadata".encode('utf-8'), metadata_dict)
                        self.logger.info(f"Imported {num_rows} rows into table '{table_name}' from file: {filename}.")
        except Exception as e:
            self.close()
            self.logger.exception(e)