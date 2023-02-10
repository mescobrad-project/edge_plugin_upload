from mescobrad_edge.plugins.edge_plugin_upload.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
from io import BytesIO

class GenericPlugin(EmptyPlugin):

    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        import datetime
        import boto3
        from botocore.client import Config

        # Init client
        s3 = boto3.resource('s3',
                            endpoint_url= self.__OBJ_STORAGE_URL__,
                            aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID__,
                            aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET__,
                            config=Config(signature_version='s3v4'),
                            region_name=self.__OBJ_STORAGE_REGION__)

        s3_local = boto3.resource('s3',
                    endpoint_url= self.__OBJ_STORAGE_URL_LOCAL__,
                    aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                    aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                    config=Config(signature_version='s3v4'),
                    region_name=self.__OBJ_STORAGE_REGION__)


        # Read incoming data and upload to cloud MinIO storage
        for file in input_meta.file_name:
            obj_name = self.__OBJ_STORAGE_ARTEFACT_TEMPLATE__.replace('{name}', file)
            data = self.__load__(obj_name)
            # Upload data
            s3.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_fileobj(BytesIO(data), obj_name, ExtraArgs={'ContentType': input_meta.file_content_type})


        # Upload updated files to local MinIO storage
        for file in input_meta.file_name:
            data = self.__load__(file)
            obj_name = self.__OBJ_STORAGE_ARTEFACT_TEMPLATE_LOCAL__.replace('{name}', file)
            # Upload updated data
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(BytesIO(data), obj_name, ExtraArgs={'ContentType': input_meta.file_content_type})

        return PluginActionResponse()
