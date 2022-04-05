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


def logs_csv(data):
    path = "/home/ninosha/Desktop/crud_task/listener_data/logs.csv"

    with open(path, 'a+', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        columns = ["file", "time", "status"]
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
