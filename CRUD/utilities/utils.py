import csv
import json
import os

from oauth2client.service_account import ServiceAccountCredentials


def jsonfile_to_dict(url):
    """
    :param url: str/url of a json file
    :return: dictionary
    """
    with open(url) as file:
        file = file.read()
        to_dict = json.loads(file)
    return to_dict


def get_blob(bucket, file_name=None):
    return bucket.blob(file_name)


def get_credentials(credentials_url):
    """
    gets credentials in json and returns as a dictionary
    :return: dictionary
    """
    try:
        credentials_dict = jsonfile_to_dict(credentials_url)
        credentials = ServiceAccountCredentials. \
            from_json_keyfile_dict(credentials_dict)
        return credentials

    except Exception as f:
        raise ConnectionError(f)

    except NotImplementedError as f:
        raise NotImplementedError(f)


def get_hash(bucket, file_name):
    blobs = list(bucket.list_blobs())
    old_hash = [blob.md5_hash for blob in blobs if blob.name ==
                file_name]
    return old_hash


def check_if_updated(blob, old_hash, new_hash):
    time_now = datetime.now()

    if old_hash != new_hash:
        metadata = {'status': 'updated'}
        blob.metadata = metadata
        blob.patch()