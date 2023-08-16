import boto3
import moto
import os
import unittest

from os.path import sep, join, dirname
from s3_objects_merger import S3ObjectsMerger, BucketNotFoundException, BucketNotInformedException, \
    ObjectKeyNotInformedException, PrefixNotFoundException


@moto.mock_s3
class S3ObjectsMergerTest(unittest.TestCase):
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
        self.s3.create_bucket(Bucket=S3ObjectsMergerTest.BUCKET_NAME)

    def test_when_text_objects_to_merge_and_new_object_in_bucket_root(self):
        # Arrange
        dataset_path = f'{S3ObjectsMergerTest.datasets_path}base{sep}'
        expected_new_object_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_total_objects_in_bucket = 1

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3ObjectsMergerTest.BUCKET_NAME, file)

        # Act
        objects_merger = S3ObjectsMerger()
        objects_merger.merge(bucket_name=S3ObjectsMergerTest.BUCKET_NAME,
                             new_object_key='legal.txt',
                             objects_to_merge_initial_name='part-',
                             objects_to_merge_prefix='')

        # Assert
        new_object = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key='legal.txt')
        actual_new_object_content = new_object['Body'].read().decode('utf-8')
        actual_total_objects_in_bucket = len(self.s3.list_objects_v2(Bucket=S3ObjectsMergerTest.BUCKET_NAME)['Contents'])

        self.assertEqual(expected_new_object_content, actual_new_object_content)
        self.assertEqual(expected_total_objects_in_bucket, actual_total_objects_in_bucket)

    def test_when_text_objects_to_merge_in_bucket_root_and_new_object_in_sub_folder(self):
        # Arrange
        dataset_path = f'{S3ObjectsMergerTest.datasets_path}base{sep}'
        expected_new_object_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_prefix_content = ''
        expected_total_objects_in_bucket = 2

        new_object_prefix = 'new_object_folder/'
        new_object_key = 'new_object_folder/legal.txt'

        self.s3.put_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key=new_object_prefix)
        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3ObjectsMergerTest.BUCKET_NAME, file)

        # Act
        objects_merger = S3ObjectsMerger()
        objects_merger.merge(bucket_name=S3ObjectsMergerTest.BUCKET_NAME,
                             new_object_key=new_object_key,
                             objects_to_merge_initial_name='part-',
                             objects_to_merge_prefix='')

        # Assert
        new_object = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key=new_object_key)
        actual_new_object_content = new_object['Body'].read().decode('utf-8')
        self.assertEqual(expected_new_object_content, actual_new_object_content)

        new_object_prefix = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key=new_object_prefix)
        actual_prefix_content = new_object_prefix['Body'].read().decode('utf-8')
        self.assertEqual(actual_prefix_content, expected_prefix_content)

        actual_total_objects_in_bucket = len(self.s3.list_objects_v2(Bucket=S3ObjectsMergerTest.BUCKET_NAME)['Contents'])
        self.assertEqual(expected_total_objects_in_bucket, actual_total_objects_in_bucket)

    def test_when_text_objects_to_merge_in_sub_folder_and_new_object_in_bucket_root(self):
        # Arrange
        dataset_path = f'{S3ObjectsMergerTest.datasets_path}base{sep}'
        expected_new_object_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_prefix_content = ''
        expected_total_objects_in_bucket = 2

        objects_to_merge_sub_folder = 'objects_to_merge_folder/'
        new_object_key = 'legal.txt'

        self.s3.put_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key=objects_to_merge_sub_folder)
        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3ObjectsMergerTest.BUCKET_NAME, f'{objects_to_merge_sub_folder}{file}')

        # Act
        objects_merger = S3ObjectsMerger()
        objects_merger.merge(bucket_name=S3ObjectsMergerTest.BUCKET_NAME,
                             new_object_key=new_object_key,
                             objects_to_merge_initial_name='part-',
                             objects_to_merge_prefix=objects_to_merge_sub_folder)

        # Assert
        new_object = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key=new_object_key)
        actual_new_object_content = new_object['Body'].read().decode('utf-8')
        self.assertEqual(expected_new_object_content, actual_new_object_content)

        objects_to_merge_sub_folder = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME,
                                                         Key=objects_to_merge_sub_folder)
        actual_prefix_content = objects_to_merge_sub_folder['Body'].read().decode('utf-8')
        self.assertEqual(actual_prefix_content, expected_prefix_content)

        actual_total_objects_in_bucket = len(self.s3.list_objects_v2(Bucket=S3ObjectsMergerTest.BUCKET_NAME)['Contents'])
        self.assertEqual(expected_total_objects_in_bucket, actual_total_objects_in_bucket)

    def test_when_text_objects_to_merge_and_new_object_in_sub_folder(self):
        # Arrange
        dataset_path = f'{S3ObjectsMergerTest.datasets_path}base{sep}'
        expected_new_object_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_prefix_content = ''
        expected_total_objects_in_bucket = 2

        sub_folder = 'sub_folder/'
        new_object_key = 'sub_folder/legal.txt'

        self.s3.put_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key=sub_folder)
        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3ObjectsMergerTest.BUCKET_NAME, f'{sub_folder}{file}')

        # Act
        objects_merger = S3ObjectsMerger()
        objects_merger.merge(bucket_name=S3ObjectsMergerTest.BUCKET_NAME,
                             new_object_key=new_object_key,
                             objects_to_merge_initial_name='part-',
                             objects_to_merge_prefix=sub_folder)

        # Assert
        new_object = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key=new_object_key)
        actual_new_object_content = new_object['Body'].read().decode('utf-8')
        self.assertEqual(expected_new_object_content, actual_new_object_content)

        objects_to_merge_sub_folder = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key=sub_folder)
        actual_prefix_content = objects_to_merge_sub_folder['Body'].read().decode('utf-8')
        self.assertEqual(actual_prefix_content, expected_prefix_content)

        actual_total_objects_in_bucket = len(self.s3.list_objects_v2(Bucket=S3ObjectsMergerTest.BUCKET_NAME)['Contents'])
        self.assertEqual(expected_total_objects_in_bucket, actual_total_objects_in_bucket)

    def test_when_blanks_between_lines_in_text_objects_to_merge(self):
        # Arrange
        dataset_path = f'{S3ObjectsMergerTest.datasets_path}blanks_between_lines{sep}'
        expected_new_object_content = 'hello there\n\nhi me\n\nhi two\n\neai\n\nhi here'
        expected_total_objects_in_bucket = 1

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3ObjectsMergerTest.BUCKET_NAME, file)

        # Act
        objects_merger = S3ObjectsMerger()
        objects_merger.merge(bucket_name=S3ObjectsMergerTest.BUCKET_NAME,
                             new_object_key='legal.txt',
                             objects_to_merge_initial_name='part-',
                             objects_to_merge_prefix='')

        # Assert
        new_object = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key='legal.txt')
        actual_new_object_content = new_object['Body'].read().decode('utf-8')
        actual_total_objects_in_bucket = len(self.s3.list_objects_v2(Bucket=S3ObjectsMergerTest.BUCKET_NAME)['Contents'])

        self.assertEqual(expected_new_object_content, actual_new_object_content)
        self.assertEqual(expected_total_objects_in_bucket, actual_total_objects_in_bucket)

    def test_when_success_files_must_not_be_deleted_after_merge(self):
        # Arrange
        dataset_path = f'{S3ObjectsMergerTest.datasets_path}base{sep}'
        expected_new_object_content = 'hello there\nhi me\nhi two\neai\nhi here'
        expected_total_objects_in_bucket = 3

        files_to_send_to_s3 = ['part-00000.txt', 'part-00000.crc', 'part-00001.txt', 'part-00001.crc', '_SUCCESS',
                               '._SUCCESS.crc']
        for file in files_to_send_to_s3:
            with open(f'{dataset_path}{file}', 'rb') as f:
                self.s3.upload_fileobj(f, S3ObjectsMergerTest.BUCKET_NAME, file)

        # Act
        objects_merger = S3ObjectsMerger()
        objects_merger.merge(bucket_name=S3ObjectsMergerTest.BUCKET_NAME,
                             new_object_key='legal.txt',
                             objects_to_merge_initial_name='part-',
                             objects_to_merge_prefix='',
                             is_success_files_deletion_enabled=False)

        # Assert
        new_object = self.s3.get_object(Bucket=S3ObjectsMergerTest.BUCKET_NAME, Key='legal.txt')
        actual_new_object_content = new_object['Body'].read().decode('utf-8')
        actual_total_objects_in_bucket = len(self.s3.list_objects_v2(Bucket=S3ObjectsMergerTest.BUCKET_NAME)['Contents'])

        self.assertEqual(expected_new_object_content, actual_new_object_content)
        self.assertEqual(expected_total_objects_in_bucket, actual_total_objects_in_bucket)

    def test_when_bucket_not_found(self):
        # Arrange
        expected_bucket_not_found_message = 'Bucket not found!'

        # Act / Assert
        objects_merger = S3ObjectsMerger()
        with self.assertRaises(BucketNotFoundException) as context:
            objects_merger.merge(bucket_name='not_found_bucket',
                                 new_object_key='legal.txt',
                                 objects_to_merge_initial_name='part-',
                                 objects_to_merge_prefix='')

        actual_bucket_not_found_message = str(context.exception)
        self.assertEqual(expected_bucket_not_found_message, actual_bucket_not_found_message)

    def test_when_prefix_not_found(self):
        # Arrange
        expected_prefix_not_found_message = 'Prefix not found!'

        # Act / Assert
        objects_merger = S3ObjectsMerger()
        with self.assertRaises(PrefixNotFoundException) as context:
            objects_merger.merge(bucket_name=S3ObjectsMergerTest.BUCKET_NAME,
                                 new_object_key='legal.txt',
                                 objects_to_merge_initial_name='part-',
                                 objects_to_merge_prefix='not_found_prefix')

        actual_prefix_not_found_message = str(context.exception)
        self.assertEqual(expected_prefix_not_found_message, actual_prefix_not_found_message)

    def test_when_bucket_name_is_empty(self):
        # Arrange
        expected_bucket_not_informed_message = 'Bucket not informed!'

        # Act / Assert
        objects_merger = S3ObjectsMerger()
        with self.assertRaises(BucketNotInformedException) as context:
            objects_merger.merge(bucket_name='',
                                 new_object_key='legal.txt',
                                 objects_to_merge_initial_name='part-',
                                 objects_to_merge_prefix='')

        actual_bucket_not_informed_message = str(context.exception)
        self.assertEqual(expected_bucket_not_informed_message, actual_bucket_not_informed_message)

    def test_when_object_key_is_empty(self):
        # Arrange
        expected_object_key_not_informed_message = 'Object key not informed!'

        # Act / Assert
        objects_merger = S3ObjectsMerger()
        with self.assertRaises(ObjectKeyNotInformedException) as context:
            objects_merger.merge(bucket_name=S3ObjectsMergerTest.BUCKET_NAME,
                                 new_object_key='',
                                 objects_to_merge_initial_name='part-',
                                 objects_to_merge_prefix='')

        actual_object_key_not_informed_message = str(context.exception)
        self.assertEqual(expected_object_key_not_informed_message, actual_object_key_not_informed_message)
