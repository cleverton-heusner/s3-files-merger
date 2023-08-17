import boto3

from botocore.exceptions import ClientError
from io import BytesIO

BODY = 'Body'
BUCKET_ROOT = ''
CONTENTS = 'Contents'
DOT = '.'
KEY = 'Key'
LINE_BREAK = '\n'
PATH_SEPARATOR = '/'

SUCCESS_NO_EXTENSION = '_SUCCESS'
SUCCESS_WITH_CRC_EXTENSION = f'.{SUCCESS_NO_EXTENSION}.crc'


class S3FilesMerger:

    def __init__(self):
        self.__client = None
        self.__bucket_name = None
        self.__new_file_key = None
        self.__files_to_merge_initial_name = None
        self.__files_to_merge_path = None
        self.__is_success_files_deletion_enabled = None
        self.__is_files_to_merge_deletion_enabled = None

    def merge(self,
              bucket_name: str,
              file_name_with_extension: str,
              files_to_merge_initial_name: str,
              files_to_merge_path=BUCKET_ROOT,
              is_success_files_deletion_enabled=True,
              is_files_to_merge_deletion_enabled=True):

        self.__client = boto3.client('s3')
        self.__files_to_merge_initial_name = files_to_merge_initial_name
        self.__bucket_name = self.__validate_bucket(bucket_name)
        self.__new_file_key = self.__validate_file_key(file_name_with_extension)
        self.__is_success_files_deletion_enabled = is_success_files_deletion_enabled
        self.__is_files_to_merge_deletion_enabled = is_files_to_merge_deletion_enabled

        self.__add_separator_to_path_if_absent(files_to_merge_path)
        files_to_merge = self.__validate_files_to_merge_path()

        new_file_extension = self.__extract_file_extension_from_key()
        new_file = self.__merge_files(files_to_merge, new_file_extension)

        new_file_without_last_blank_line = new_file[:-1]
        self.__upload_file_to_bucket(new_file_without_last_blank_line)

        if self.__is_success_files_deletion_enabled:
            self.__delete_success_files()

        self.__client.close()

    def __validate_bucket(self, bucket_name: str):
        self.__check_if_bucket_name_is_informed(bucket_name)
        self.__check_if_bucket_exists(bucket_name)

        return bucket_name

    @staticmethod
    def __check_if_bucket_name_is_informed(bucket_name):
        if not bucket_name:
            raise ValueError('Bucket not informed!')

    @staticmethod
    def __check_if_bucket_exists(bucket_name: str):
        try:
            boto3.resource('s3').meta.client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise BucketNotFoundException()

    @staticmethod
    def __validate_file_key(new_file_key: str):
        if not new_file_key:
            raise ValueError('File key not informed!')

        return new_file_key

    def __add_separator_to_path_if_absent(self, files_to_merge_path: str):
        self.__files_to_merge_path = f'{files_to_merge_path}{PATH_SEPARATOR}' \
            if files_to_merge_path and not files_to_merge_path.endswith(PATH_SEPARATOR) \
            else files_to_merge_path

    def __validate_files_to_merge_path(self):
        paths = self.__client.list_objects_v2(Bucket=self.__bucket_name, Prefix=self.__files_to_merge_path)
        if CONTENTS not in paths:
            raise FileNotFoundException('Files to merge path not found!')
        return paths

    def __extract_file_extension_from_key(self) -> str:
        return self.__new_file_key.split(DOT)[-1]

    def __merge_files(self, files_to_merge, new_file_extension):
        new_file = ''
        total_files_to_merge_with_initial_name = 0

        for file_content in files_to_merge[CONTENTS]:
            file_name = self.__extract_file_name_from_key(file_content[KEY])
            file_to_merge_key = f'{self.__files_to_merge_path}{file_name}'

            if file_name.startswith(self.__files_to_merge_initial_name):
                total_files_to_merge_with_initial_name += 1

                if file_name.endswith(new_file_extension):
                    file_to_merge = self.__client.get_object(Bucket=self.__bucket_name, Key=file_to_merge_key)
                    new_file = self.__merge_files_line_by_line(file_to_merge, new_file)

                if self.__is_files_to_merge_deletion_enabled:
                    self.__client.delete_object(Bucket=self.__bucket_name, Key=file_to_merge_key)

        if not total_files_to_merge_with_initial_name:
            raise FileNotFoundException(f'Files to merge not found with the initial name '
                                        f'"{self.__files_to_merge_initial_name}"!')

        return new_file

    @staticmethod
    def __extract_file_name_from_key(file_key: str) -> str:
        return file_key.split(PATH_SEPARATOR)[-1]

    @staticmethod
    def __merge_files_line_by_line(file_to_merge: dict, new_file: str) -> str:
        for file_line_to_merge in file_to_merge[BODY].iter_lines():
            line_to_merge = file_line_to_merge.decode()
            new_file += f'{line_to_merge}{LINE_BREAK}'

        return new_file

    def __upload_file_to_bucket(self, new_file: str):
        with BytesIO(new_file.encode()) as f:
            self.__client.upload_fileobj(Bucket=self.__bucket_name, Key=f'{self.__new_file_key}', Fileobj=f)

    def __delete_success_files(self):
        success_files = [SUCCESS_NO_EXTENSION, SUCCESS_WITH_CRC_EXTENSION]
        for f in success_files:
            self.__client.delete_object(Bucket=self.__bucket_name, Key=f'{self.__files_to_merge_path}{f}')


class BucketNotFoundException(Exception):
    def __init__(self):
        super().__init__('Bucket not found!')


class FileNotFoundException(Exception):
    pass
