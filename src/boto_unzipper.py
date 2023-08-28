import boto3
import os
import zipfile


def get_client():
    """ Boto does not work when it is an imported from a zipfile. This unzips it to a tmp dir and
     provides session access with the correctly-configured data search paths """
    def get_zip_path(path):
        if path == os.path.sep:
            return None
        elif zipfile.is_zipfile(path):
            return path
        else:
            return get_zip_path(os.path.dirname(path))

    base_extract_path = "/tmp/boto_zip_cache"
    sess = boto3.Session()
    kwargs = {}
    zp = get_zip_path(boto3.__file__)

    if zp:
        z = zipfile.ZipFile(zp)
        for n in z.namelist():
            if n.startswith("boto"):
                z.extract(member=n, path=base_extract_path)

        extra_paths = [os.path.join(base_extract_path, p) for p in ["botocore/data", "boto3/data"]]
        sess._loader._search_paths.extend(extra_paths)
        kwargs.update(dict(
            use_ssl=True,
            verify=os.path.join(base_extract_path, "botocore/vendored/requests/cacert.pem")
        ))
    return lambda name: sess.resource(name, **kwargs), lambda name: sess.client(name, **kwargs)
