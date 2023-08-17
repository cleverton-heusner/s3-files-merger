import boto3
import moto
import os
import unittest

from os.path import sep, join, dirname
from s3_files_merger import S3FilesMerger, BucketNotFoundException, FileNotFoundException


@moto.mock_s3
class S3FilesMergerTest(unittest.TestCase):
    BUCKET_NAME = 'files-to-merge'
    datasets_path = join(dirname(__file__), 'datasets') + sep

    @classmethod
    def setUpClass(cls):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    def setUp(self):
        self.s3 = boto3.client("s3")
        self.s3.create_bucket(Bucket=S3FilesMergerTest.BUCKET_NAME)

    def test_when_text_files_to_merge_and_new_file_in_bucket_root(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_new_file_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_total_files_in_bucket = 1

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, file)

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='')
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key='legal.txt')
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])

        self.assertEqual(expected_new_file_content, actual_new_file_content)
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_text_files_to_merge_in_bucket_root_and_new_file_in_sub_folder(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_new_file_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_path_content = ''
        expected_total_files_in_bucket = 2

        new_file_path = 'new_file_folder/'
        new_file_key = 'new_file_folder/legal.txt'

        self.s3.put_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=new_file_path)
        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, file)

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension=new_file_key,
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='')
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=new_file_key)
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        self.assertEqual(expected_new_file_content, actual_new_file_content)

        new_file_path = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=new_file_path)
        actual_path_content = new_file_path['Body'].read().decode('utf-8')
        self.assertEqual(actual_path_content, expected_path_content)

        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_text_files_to_merge_in_sub_folder_and_new_file_in_bucket_root(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_new_file_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_path_content = ''
        expected_total_files_in_bucket = 2

        files_to_merge_sub_folder = 'files_to_merge_folder/'
        new_file_key = 'legal.txt'

        self.s3.put_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=files_to_merge_sub_folder)
        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, f'{files_to_merge_sub_folder}{file}')

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension=new_file_key,
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path=files_to_merge_sub_folder)
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=new_file_key)
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        self.assertEqual(expected_new_file_content, actual_new_file_content)

        files_to_merge_sub_folder = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME,
                                                         Key=files_to_merge_sub_folder)
        actual_path_content = files_to_merge_sub_folder['Body'].read().decode('utf-8')
        self.assertEqual(actual_path_content, expected_path_content)

        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_text_files_to_merge_and_new_file_in_sub_folder(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_new_file_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_path_content = ''
        expected_total_files_in_bucket = 2

        sub_folder = 'sub_folder/'
        new_file_key = 'sub_folder/legal.txt'

        self.s3.put_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=sub_folder)
        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, f'{sub_folder}{file}')

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension=new_file_key,
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path=sub_folder)
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=new_file_key)
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        self.assertEqual(expected_new_file_content, actual_new_file_content)

        files_to_merge_sub_folder = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=sub_folder)
        actual_path_content = files_to_merge_sub_folder['Body'].read().decode('utf-8')
        self.assertEqual(actual_path_content, expected_path_content)

        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_blanks_between_lines_in_text_files_to_merge(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}blanks_between_lines{sep}'
        expected_new_file_content = 'hello there\n\nhi me\n\nhi two\n\neai\n\nhi here'
        expected_total_files_in_bucket = 1

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, file)

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='')
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key='legal.txt')
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])

        self.assertEqual(expected_new_file_content, actual_new_file_content)
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_success_files_must_not_be_deleted_after_merge(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_new_file_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_total_files_in_bucket = 3

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, file)

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='',
                                        is_success_files_deletion_enabled=False)
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key='legal.txt')
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])

        self.assertEqual(expected_new_file_content, actual_new_file_content)
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_file_to_merge_path_is_not_ending_with_separator(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_new_file_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_path_content = ''
        expected_total_files_in_bucket = 2

        sub_folder = 'sub_folder/'
        new_file_key = 'sub_folder/legal.txt'

        self.s3.put_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=sub_folder)
        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, f'{sub_folder}{file}')

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension=new_file_key,
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='sub_folder')
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=new_file_key)
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        self.assertEqual(expected_new_file_content, actual_new_file_content)

        files_to_merge_sub_folder = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key=sub_folder)
        actual_path_content = files_to_merge_sub_folder['Body'].read().decode('utf-8')
        self.assertEqual(actual_path_content, expected_path_content)

        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_files_to_merge_must_not_be_deleted_after_merge(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_new_file_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_total_files_in_bucket = 5

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, file)

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='',
                                        is_files_to_merge_deletion_enabled=False)
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key='legal.txt')
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])

        self.assertEqual(expected_new_file_content, actual_new_file_content)
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_text_files_to_merge_and_new_file_in_bucket_root_and_files_to_merge_path_not_informed(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_new_file_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_total_files_in_bucket = 1

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, file)

        # Act
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='part-')
        s3_files_merger.merge()

        # Assert
        new_file = self.s3.get_object(Bucket=S3FilesMergerTest.BUCKET_NAME, Key='legal.txt')
        actual_new_file_content = new_file['Body'].read().decode('utf-8')
        actual_total_files_in_bucket = len(self.s3.list_objects_v2(Bucket=S3FilesMergerTest.BUCKET_NAME)['Contents'])

        self.assertEqual(expected_new_file_content, actual_new_file_content)
        self.assertEqual(expected_total_files_in_bucket, actual_total_files_in_bucket)

    def test_when_bucket_not_found(self):
        # Arrange
        expected_bucket_not_found_message = 'Bucket not found!'

        # Act / Assert
        s3_files_merger = S3FilesMerger(bucket_name='not_found_bucket',
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='')
        with self.assertRaises(BucketNotFoundException) as context:
            s3_files_merger.merge()

        actual_bucket_not_found_message = str(context.exception)
        self.assertEqual(expected_bucket_not_found_message, actual_bucket_not_found_message)

    def test_when_files_to_merge_path_not_found(self):
        # Arrange
        expected_files_to_merge_path_not_found_message = 'Files to merge path not found!'

        # Act / Assert
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='not_found_path/')
        with self.assertRaises(FileNotFoundException) as context:
            s3_files_merger.merge()

        actual_files_to_merge_path_not_found_message = str(context.exception)
        self.assertEqual(expected_files_to_merge_path_not_found_message, actual_files_to_merge_path_not_found_message)

    def test_when_not_found_files_to_merge_with_initial_name_informed(self):
        # Arrange
        dataset_path = f'{S3FilesMergerTest.datasets_path}base{sep}'
        expected_not_existing_initial_name_message = 'Files to merge not found with the initial name ' \
                                                     '"not-existing-initial-name"!'

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3FilesMergerTest.BUCKET_NAME, file)

        # Act / Assert
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='not-existing-initial-name',
                                        files_to_merge_path='')
        with self.assertRaises(FileNotFoundException) as context:
            s3_files_merger.merge()

        actual_not_existing_initial_name_message = str(context.exception)
        self.assertEqual(expected_not_existing_initial_name_message, actual_not_existing_initial_name_message)

    def test_when_bucket_name_is_empty(self):
        # Arrange
        expected_bucket_not_informed_message = 'Bucket not informed!'

        # Act / Assert
        s3_files_merger = S3FilesMerger(bucket_name='',
                                        file_name_with_extension='legal.txt',
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='')
        with self.assertRaises(ValueError) as context:
            s3_files_merger.merge()

        actual_bucket_not_informed_message = str(context.exception)
        self.assertEqual(expected_bucket_not_informed_message, actual_bucket_not_informed_message)

    def test_when_file_key_is_empty(self):
        # Arrange
        expected_file_key_not_informed_message = 'File key not informed!'

        # Act / Assert
        s3_files_merger = S3FilesMerger(bucket_name=S3FilesMergerTest.BUCKET_NAME,
                                        file_name_with_extension='',
                                        files_to_merge_initial_name='part-',
                                        files_to_merge_path='')
        with self.assertRaises(ValueError) as context:
            s3_files_merger.merge()

        actual_key_key_not_informed_message = str(context.exception)
        self.assertEqual(expected_file_key_not_informed_message, actual_key_key_not_informed_message)
