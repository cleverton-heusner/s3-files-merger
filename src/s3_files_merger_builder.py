from src.constants import Constant


class S3FilesMergerBuilder:

    def __init__(self):
        self.__bucket_name = None
        self.__merged_file_full_filename = None
        self.__files_to_merge_initial_name = None
        self.__files_to_merge_full_path = Constant.BUCKET_ROOT
        self.__is_success_files_deletion_enabled = Constant.IS_SUCCESS_FILES_DELETION_ENABLED
        self.__is_files_to_merge_deletion_enabled = Constant.IS_FILES_TO_MERGE_DELETION_ENABLED
        self.__merged_file_chunk_size_in_mb = Constant.MERGED_FILE_CHUNK_DEFAULT_SIZE_IN_MB

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
        from s3_files_merger import S3FilesMerger
        return S3FilesMerger(self.__bucket_name,
                             self.__merged_file_full_filename,
                             self.__files_to_merge_initial_name,
                             self.__files_to_merge_full_path,
                             self.__is_success_files_deletion_enabled,
                             self.__is_files_to_merge_deletion_enabled,
                             self.__merged_file_chunk_size_in_mb)
