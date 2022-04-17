import json
from oauth2client.service_account import ServiceAccountCredentials


def jsonfile_to_dict(url):
    """
    function converts json to dictionary
    :param url: str/url of a json file
    :return: dictionary
    """
    with open(url) as file:
        file = file.read()
        to_dict = json.loads(file)
    return to_dict


def check_if_updated(blob, old_hash, new_hash):
    """
    function checks if blob was updated with hashes,
    if updated, adds blob metadata status - updated
    if not updated, adds status - not updated

    :param blob: blob obj
    :param old_hash: str/blob.md5_hash
    :param new_hash: str/blob.md5_hash
    :return: str/message
    """

    if old_hash != new_hash:
        metadata = {"status": "updated"}
        blob.metadata = metadata
        blob.patch()

        return "metadata status: updated"

    else:
        metadata = {"status": "not updated"}
        blob.metadata = metadata
        blob.patch()

        return "metadata status: not updated"


def get_hash(bucket, file_name):
    """
    function returns hash of a file
    :param bucket: bucket obj
    :param file_name: str/filename
    :return: str/file hash
    """

    blobs = list(bucket.list_blobs())
    file_hash = [blob.md5_hash for blob in blobs if blob.name ==
                 file_name]

    return file_hash


def get_blob(bucket, file_name=None):
    """
    function returns blob object
    :param bucket: bucket obj
    :param file_name: str/None
    :return: blob obj
    """
    return bucket.blob(file_name, chunk_size=3221225472)


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
