from models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
from io import BytesIO

class UploadPlugin(EmptyPlugin):

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

        # Read incoming data
        data = self.__load__(input_meta)

        # Upload data
        obj_name = self.__OBJ_STORAGE_ARTEFACT_TEMPLATE__\
            .replace('{ts}', str(datetime.datetime.now()))\
            .replace('{fn}', input_meta.file_name)\
            .replace('{fct}', input_meta.file_content_type)\
            .replace('{co}', input_meta.created_on) # created_on
        s3.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_fileobj(BytesIO(data), obj_name, ExtraArgs={'ContentType': input_meta.file_content_type})

        return PluginActionResponse()
