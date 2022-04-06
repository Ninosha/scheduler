import csv
import json
import os
from datetime import datetime
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


def check_if_updated(file_name, old_hash, new_hash):
    time_now = datetime.now()
    if old_hash != new_hash:
        data = [file_name,
                str(time_now), "updated"]
        return data


def get_hash(bucket, file_name):
    blobs = list(bucket.list_blobs())
    file_hash = [blob.md5_hash for blob in blobs if blob.name ==
                 file_name]
    return file_hash


def logs_csv(data):
    path = "/home/ninosha/Desktop/GCP_task_scheduler" \
           "/listener_data/logs.csv"
    with open(path, 'a+', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        columns = ["file", "time", "status", "hash"]
        if os.stat(path).st_size != 0:
            print(os.stat(path).st_size)
            writer.writerow(data)
        else:
            writer.writerow(columns)
            writer.writerow(data)


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
