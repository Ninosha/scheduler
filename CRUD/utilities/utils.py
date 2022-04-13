import csv
import json
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from VARS import logs_path


def jsonfile_to_dict(url):
    """
    :param url: str/url of a json file
    :return: dictionary
    """
    with open(url) as file:
        file = file.read()
        to_dict = json.loads(file)
    return to_dict


def check_if_updated(file_name, old_hash, new_hash):
    time_now = datetime.now()
    if old_hash != new_hash:
        data = {file_name: {"time":
                str(time_now), "status": "updated"}}

        return data
    else:
        data = {file_name: {"time":
                str(time_now), "status": "not updated"}}

        return data


def get_hash(bucket, file_name):
    blobs = list(bucket.list_blobs())
    file_hash = [blob.md5_hash for blob in blobs if blob.name ==
                 file_name]
    return file_hash


def read_json(path):
    with open(path, "r") as file:
        return json.load(file)


def write_json(path, data):
    with open(path, "w") as file:
        return json.dump(data, file)


def logs_json(data):
    try:
        history_dict = read_json(logs_path)
        history_dict.update(data)
        write_json(logs_path, history_dict)
    except Exception as f:
        write_json(logs_path, data)


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
