from io import BytesIO

import boto3 as boto3

BODY = 'Body'
CONTENTS = 'Contents'
DOT = '.'
KEY = 'Key'
LINE_BREAK = '\n'
SUCCESS = '_SUCCESS'


class S3ObjectsMerger:

    def merge(self, bucket_name: str, new_object_key: str, objects_to_merge_initial_name: str,
              objects_to_merge_prefix=''):

        client = boto3.client('s3')
        objects = client.list_objects_v2(Bucket=bucket_name, Prefix=objects_to_merge_prefix)

        if CONTENTS in objects:
            new_object = ''
            new_object_extension = new_object_key.split(DOT)[-1]

            for object_content in objects[CONTENTS]:
                object_name = object_content[KEY].split('/')[-1]
                object_to_merge_key = f'{objects_to_merge_prefix}{object_name}'

                if object_name.startswith(objects_to_merge_initial_name):
                    if object_name.endswith(new_object_extension):
                        object_to_merge = client.get_object(Bucket=bucket_name, Key=object_to_merge_key)
                        new_object = self.__merge_objects_line_by_line(object_to_merge, new_object)
                    client.delete_object(Bucket=bucket_name, Key=object_to_merge_key)

            with BytesIO(new_object[:-1].encode()) as file_obj:
                client.upload_fileobj(Bucket=bucket_name, Key=f'{new_object_key}', Fileobj=file_obj)

            self.__delete_success_objects(client, bucket_name, objects_to_merge_prefix, new_object_extension)
            client.close()

    @staticmethod
    def __merge_objects_line_by_line(object_to_merge, new_object):
        for object_line_to_merge in object_to_merge[BODY].iter_lines():
            line_to_merge = object_line_to_merge.decode()
            new_object += f'{line_to_merge}{LINE_BREAK}'

        return new_object

    @staticmethod
    def __delete_success_objects(client, bucket_name, objects_to_merge_prefix, new_object_extension):
        objects_to_delete = []
        for extension in [new_object_extension, 'crc']:
            objects_to_delete.append({KEY: f'{objects_to_merge_prefix}{SUCCESS}{DOT}{extension}'})
        client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
