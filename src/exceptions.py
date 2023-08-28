class BucketNotFoundException(Exception):
    def __init__(self):
        super().__init__('Bucket not found!')


class FileNotFoundException(Exception):
    pass
