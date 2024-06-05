from mescobrad_edge.plugins.edge_plugin_upload.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
from io import BytesIO

class GenericPlugin(EmptyPlugin):

    def execute_sql_on_trino(self, sql, conn):
        """Generic function to execute a SQL statement"""

        # Get a cursor from the connection object
        cur = conn.cursor()

        # Execute sql statement
        cur.execute(sql)

        # Get the results from the cluster
        rows = cur.fetchall()

        # Return the results
        return rows

    def transform_input_data(self, data, source_name, workspace_id, MRN,
                             metadata_file_name):
        """Transform input data into table suitable for creating query"""

        data = data.reset_index(drop=True)

        if MRN is not None:
            data["MRN"] = MRN

        if metadata_file_name is not None:
            data["metadata_file_name"] = metadata_file_name

        # Add rowid column representing id of the row in the file
        data["rowid"] = data.index + 1

        # Insert source column representing name of the source file
        data.insert(0, "source", source_name)

        # Transform table into table with 5 columns: source,
        # rowid, variable_name, variable_value, workspace_id
        data = data.melt(id_vars=["source","rowid"])
        data = data.sort_values(by=['rowid'])

        # As a variable values type string is expected
        data = data.astype({"value":"str"})

        # Add workspace id into workspace column of the table
        data.insert(4, "workspace_id", workspace_id)

        return data

    def upload_data_on_trino(self, schema_name, table_name, data, conn):
        """Create sql statement for inserting data and update
        the table with data"""

        print(data.shape[0], "rows to insert ...")

        batch_size = 5000

        # Iterate through pandas dataframe to extract each row values
        for start in range(0, data.shape[0], batch_size):
            end = start + batch_size
            batch = data.iloc[start:end]

            # Create a batch insert statement
            data_list = []
            for row in batch.itertuples(index=False):
                data_list.append(str(tuple(row)))

            data_to_insert = ", ".join(data_list)

            # Insert data into the table
            sql_statement = "INSERT INTO iceberg.{schema_name}.{table_name} VALUES {data}"\
                .format(schema_name=schema_name, table_name=table_name, data=data_to_insert)
            self.execute_sql_on_trino(sql=sql_statement, conn=conn)

            percent_start = int((start / data.shape[0]) *100)
            percent_complete = int((end / data.shape[0]) * 100) if end < data.shape[0] else 100
            if percent_complete != percent_start:
                self.print_progress_bar(percent_complete)

    def print_progress_bar(self, percent):
        import sys
        from time import sleep
        bar_length = 50

        sys.stdout.write('\r')
        sys.stdout.write("Completed: [{:{}}] {:>3}%"
                         .format('='*int(percent/(100.0/bar_length)),
                                 bar_length, int(percent)))
        sys.stdout.flush()
        sleep(0.002)

    def update_filename_pid_mapping(self, obj_name, list_ids, s3_local):
        import csv
        import io

        folder = "file_pid/"
        filename = "filename_pid.csv"
        file_path = f"{folder}{filename}"

        # Column names od the csv file, where the mapping between file and patient
        # personal ID is saved
        key_values = ['filename', 'personal_id']

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)
        obj_files = bucket_local.objects.filter(Prefix=folder, Delimiter="/")

        # Prepare data which needs to be saved (pairs of: (obj_name, personal_id))
        data_id = list_ids.to_frame()
        data_id.insert(0, key_values[0], obj_name)
        data_to_append = data_id.values.tolist()

        # If file where mapping is saved already exist, append data to file, and replace
        # object in MinIO, otherise create new object and insert data
        if (len(list(obj_files))) > 0:
            existing_object = s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__, file_path)
            existing_data = existing_object.get()["Body"].read().decode('utf-8')
            existing_rows = list(csv.reader(io.StringIO(existing_data)))
            existing_rows.extend(data_to_append)

            updated_data = io.StringIO()
            csv.writer(updated_data).writerows(existing_rows)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                io.BytesIO(updated_data.getvalue().encode('utf-8')), file_path)
        else: # file doesn't exist, create file and insert data
            file_data = [key_values, data_to_append]
            updated_data = io.StringIO()
            csv.writer(updated_data).writerows(file_data)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                io.BytesIO(updated_data.getvalue().encode('utf-8')), file_path)

    def upload_metadata_file(self, metadata_file_name, metadata_content, s3_data_lake):
        "Upload metadata files to support FAIR process and make data interoperable"

        obj_name = f"metadata_files/{metadata_file_name}"
        s3_data_lake.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_fileobj(
            BytesIO(metadata_content), obj_name, ExtraArgs={'ContentType': "text/json"})

        print(f"\nMetadata file {metadata_file_name} is attached to the data.")


    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        import boto3
        from botocore.client import Config
        import os
        import pandas as pd
        from trino.dbapi import connect
        from trino.auth import BasicAuthentication

        # Initialize the connection with Trino
        conn = connect(
            host=self.__TRINO_HOST__,
            port=self.__TRINO_PORT__,
            http_scheme="https",
            auth=BasicAuthentication(self.__TRINO_USER__, self.__TRINO_PASSWORD__),
            max_attempts=1,
            request_timeout=600
        )

        # Initialize local MinIO client
        s3_local = boto3.resource('s3',
                    endpoint_url= self.__OBJ_STORAGE_URL_LOCAL__,
                    aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                    aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                    config=Config(signature_version='s3v4'),
                    region_name=self.__OBJ_STORAGE_REGION__)

        # Init MinIO cloud client
        s3_data_lake = boto3.resource('s3',
                        endpoint_url= self.__OBJ_STORAGE_URL__,
                        aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID__,
                        aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET__,
                        config=Config(signature_version='s3v4'),
                        region_name=self.__OBJ_STORAGE_REGION__)

        # Get the schema name, schema in Trino is an equivalent to a bucket in MinIO
        # Trino doesn't allow to have "-" in schema name so it needs to be replaced
        # with "_"
        schema_name = self.__OBJ_STORAGE_BUCKET__.replace("-", "_")

        # Get the table name
        table_name = self.__OBJ_STORAGE_TABLE__.replace("-", "_")

        # Path to the anonymized file
        obj_name_template = "anonymous_data/{name}"

        # Source name with the timestamp to add in table within column source
        source_name_template = "{name}_{timestamp}.csv"

        # Metadata file template
        metadata_file_template = "{name}_{timestamp}.json"

        # Path to file to remove
        remove_file = "./{filename}"

        # Transform anonymized data into the appropriate form to upload with Trino
        # and perform the uploading of the anonymized data
        for file, ts in zip(input_meta.file_name, input_meta.created_on):
            obj_name = obj_name_template.format(name=file)

            # Read data from anonymized parquet file
            data = pd.read_parquet(obj_name)

            # Extract list of personal IDs
            list_ids = data["PID"]

            # Transform data using pandas dataframe to format used in final table within MinIO
            source_name = source_name_template.format(name=os.path.splitext(file)[0],
                                                      timestamp=ts)

            if input_meta.data_info["metadata_json_file"] is not None:
                metadata_file_name = metadata_file_template.format(
                    name=os.path.splitext(file)[0], timestamp=ts)
            else:
                metadata_file_name = None

            data = self.transform_input_data(data, source_name,
                                             input_meta.data_info['workspace_id'],
                                             input_meta.data_info['MRN'],
                                             metadata_file_name)

            self.upload_data_on_trino(schema_name, table_name, data, conn)

            # Upload metadata file
            if input_meta.data_info["metadata_json_file"] is not None:
                self.upload_metadata_file(metadata_file_name,
                                          input_meta.data_info["metadata_json_file"],
                                          s3_data_lake)

            # Delete file after upload
            os.remove(remove_file.format(filename=obj_name))
            print("Anonymized data are uploaded into MinIO storage.")

        # Upload updated files to local MinIO storage
        for file, ts in zip(input_meta.file_name, input_meta.created_on):
            data = self.__load__(file)

            obj_name = self.__OBJ_STORAGE_ARTEFACT_TEMPLATE_LOCAL__\
                .replace('{name}', os.path.splitext(file)[0])\
                .replace('{timestamp}', ts)

            # Upload updated data
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                BytesIO(data), obj_name,
                ExtraArgs={'ContentType': input_meta.file_content_type})

            # Update key value file for mapping files with the generated personal IDs, in
            # local instance of MinIO
            self.update_filename_pid_mapping(obj_name, list_ids, s3_local)

            # Delete file after upload
            os.remove(remove_file.format(filename=file))

            print("Original csv is updated")

        return PluginActionResponse()
