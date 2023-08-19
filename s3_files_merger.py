import boto3
from boto3.s3.transfer import TransferConfig

from botocore.exceptions import ClientError
from io import BytesIO

BODY = 'Body'
BUCKET_ROOT = ''
CONTENTS = 'Contents'
DOT = '.'
KEY = 'Key'
LINE_BREAK = '\n'
PATH_SEPARATOR = '/'

MERGED_FILE_CHUNK_DEFAULT_SIZE_IN_MB = 10
IS_SUCCESS_FILES_DELETION_ENABLED = True
IS_FILES_TO_MERGE_DELETION_ENABLED = True

SUCCESS_NO_EXTENSION = '_SUCCESS'
SUCCESS_WITH_CRC_EXTENSION = f'.{SUCCESS_NO_EXTENSION}.crc'


class S3FilesMergerBuilder:

    def __init__(self):
        self.__bucket_name = None
        self.__merged_file_full_filename = None
        self.__files_to_merge_initial_name = None
        self.__files_to_merge_full_path = BUCKET_ROOT
        self.__is_success_files_deletion_enabled = IS_SUCCESS_FILES_DELETION_ENABLED
        self.__is_files_to_merge_deletion_enabled = IS_FILES_TO_MERGE_DELETION_ENABLED
        self.__merged_file_chunk_size_in_mb = MERGED_FILE_CHUNK_DEFAULT_SIZE_IN_MB

    def bucket_name(self, bucket_name):
        self.__bucket_name = bucket_name
        return self

    def merged_file_full_filename(self, merged_file_full_filename):
        self.__merged_file_full_filename = merged_file_full_filename
        return self

    def files_to_merge_initial_name(self, files_to_merge_initial_name):
        self.__files_to_merge_initial_name = files_to_merge_initial_name
        return self

    def files_to_merge_full_path(self, files_to_merge_full_path):
        self.__files_to_merge_full_path = files_to_merge_full_path
        return self

    def is_success_files_deletion_enabled(self, is_success_files_deletion_enabled):
        self.__is_success_files_deletion_enabled = is_success_files_deletion_enabled
        return self

    def is_files_to_merge_deletion_enabled(self, is_files_to_merge_deletion_enabled):
        self.__is_files_to_merge_deletion_enabled = is_files_to_merge_deletion_enabled
        return self

    def merged_file_chunk_size_in_mb(self, merged_file_chunk_size_in_mb):
        self.__merged_file_chunk_size_in_mb = merged_file_chunk_size_in_mb
        return self

    def build(self):
        return S3FilesMerger(self.__bucket_name,
                             self.__merged_file_full_filename,
                             self.__files_to_merge_initial_name,
                             self.__files_to_merge_full_path,
                             self.__is_success_files_deletion_enabled,
                             self.__is_files_to_merge_deletion_enabled,
                             self.__merged_file_chunk_size_in_mb)


class S3FilesMerger:

    builder = S3FilesMergerBuilder()

    def __init__(self,
                 bucket_name,
                 merged_file_full_filename,
                 files_to_merge_initial_name,
                 files_to_merge_full_path=BUCKET_ROOT,
                 is_success_files_deletion_enabled=IS_SUCCESS_FILES_DELETION_ENABLED,
                 is_files_to_merge_deletion_enabled=IS_FILES_TO_MERGE_DELETION_ENABLED,
                 merged_file_chunk_size_in_mb=MERGED_FILE_CHUNK_DEFAULT_SIZE_IN_MB):

        self.__bucket_name = bucket_name
        self.__merged_file_full_filename = merged_file_full_filename
        self.__files_to_merge_initial_name = files_to_merge_initial_name
        self.__files_to_merge_full_path = files_to_merge_full_path
        self.__is_success_files_deletion_enabled = is_success_files_deletion_enabled
        self.__is_files_to_merge_deletion_enabled = is_files_to_merge_deletion_enabled
        self.__merged_file_chunk_size_in_mb = merged_file_chunk_size_in_mb

        self.__client = None

    def merge(self):
        self.__client = boto3.client('s3')

        self.__validate_bucket()
        self.__validate_merged_file_full_filename()

        self.__add_separator_to_files_to_merge_path_if_absent()
        files_to_merge = self.__validate_files_to_merge_path()

        merged_file_extension = self.__extract_extension_from_full_filename()
        self.__merge_files(files_to_merge, merged_file_extension)

        if self.__is_success_files_deletion_enabled:
            self.__delete_success_files()

        self.__client.close()

    def __validate_bucket(self):
        self.__check_if_bucket_name_is_informed(self.__bucket_name)
        self.__check_if_bucket_exists(self.__bucket_name)

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

    def __validate_merged_file_full_filename(self):
        if not self.__merged_file_full_filename:
            raise ValueError('Full filename not informed for the merged file!')

    def __add_separator_to_files_to_merge_path_if_absent(self):
        self.__files_to_merge_full_path = f'{self.__files_to_merge_full_path}{PATH_SEPARATOR}' \
            if self.__files_to_merge_full_path and not self.__files_to_merge_full_path.endswith(PATH_SEPARATOR) \
            else self.__files_to_merge_full_path

    def __validate_files_to_merge_path(self):
        paths = self.__client.list_objects_v2(Bucket=self.__bucket_name, Prefix=self.__files_to_merge_full_path)
        if CONTENTS not in paths:
            raise FileNotFoundException('Files to merge path not found!')
        return paths

    def __extract_extension_from_full_filename(self) -> str:
        return self.__merged_file_full_filename.split(DOT)[-1]

    def __merge_files(self, files_to_merge, merged_file_extension):
        merged_file = ''
        total_files_to_merge_with_initial_name = 0

        for file_content in files_to_merge[CONTENTS]:
            file_to_merge_name = self.__extract_file_name_from_full_filename(file_content[KEY])
            file_to_merge_full_filename = f'{self.__files_to_merge_full_path}{file_to_merge_name}'

            if file_to_merge_name.startswith(self.__files_to_merge_initial_name):
                total_files_to_merge_with_initial_name += 1

                if file_to_merge_name.endswith(merged_file_extension):
                    file_to_merge = self.__client.get_object(Bucket=self.__bucket_name, Key=file_to_merge_full_filename)
                    merged_file = self.__merge_files_line_by_line(file_to_merge, merged_file)

                    if file_content == files_to_merge[CONTENTS][-1]:
                        merged_file = merged_file[:-1]

                    self.__upload_file_to_bucket(merged_file)

                if self.__is_files_to_merge_deletion_enabled:
                    self.__client.delete_object(Bucket=self.__bucket_name, Key=file_to_merge_full_filename)

        if not total_files_to_merge_with_initial_name:
            raise FileNotFoundException(f'Files to merge not found for the initial name '
                                        f'"{self.__files_to_merge_initial_name}"!')

        return merged_file

    @staticmethod
    def __extract_file_name_from_full_filename(full_filename: str) -> str:
        return full_filename.split(PATH_SEPARATOR)[-1]

    @staticmethod
    def __merge_files_line_by_line(file_to_merge: dict, merged_file: str) -> str:
        for file_line_to_merge in file_to_merge[BODY].iter_lines():
            line_to_merge = file_line_to_merge.decode()
            merged_file += f'{line_to_merge}{LINE_BREAK}'

        return merged_file

    def __upload_file_to_bucket(self, merged_file: str):
        transfer_config = TransferConfig(multipart_threshold=self.__merged_file_chunk_size_in_mb,
                                         multipart_chunksize=self.__merged_file_chunk_size_in_mb)

        with BytesIO(merged_file.encode()) as f:
            self.__client.upload_fileobj(Bucket=self.__bucket_name, Key=f'{self.__merged_file_full_filename}', Fileobj=f,
                                         Config=transfer_config)

    def __delete_success_files(self):
        success_files = [SUCCESS_NO_EXTENSION, SUCCESS_WITH_CRC_EXTENSION]
        for f in success_files:
            self.__client.delete_object(Bucket=self.__bucket_name, Key=f'{self.__files_to_merge_full_path}{f}')


class BucketNotFoundException(Exception):
    def __init__(self):
        super().__init__('Bucket not found!')


class FileNotFoundException(Exception):
    pass
