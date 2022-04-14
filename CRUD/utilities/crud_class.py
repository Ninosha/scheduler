from CRUD.utilities.utils import get_blob, get_hash, check_if_updated


class CRUDFuncs:
    """
    class has functions to operate on blob, create file in bucket,
    read/download, update and delete
    """

    def __init__(self, bucket):
        """
        """
        self.bucket = bucket

    def creat_file(self, file_name, file_path):
        """
        uploads file from file path to bucket

        :param file_name: str/file name
        :param file_path: str/file path
        :return: message if operation was successful, else - error
        """
        try:
            blobs_list = list(self.bucket.list_blobs())
            if file_name in blobs_list:
                raise f"{file_name} already exists in {self.bucket}"
            else:
                get_blob(self.bucket, file_name).upload_from_filename(
                    file_path)
                return f"{file_name} was uploaded"

        except FileNotFoundError as f:
            raise FileNotFoundError(f)

        except NotImplementedError as f:
            raise NotImplementedError(f)

    def read_file(self, file_name, path_to_download):
        """
        downloads requested file to passed path

        :param file_name: str/file name
        :param path_to_download: string url
        :return: message if operation was successful, else - error
        """
        try:
            get_blob(self.bucket, file_name). \
                download_to_filename(f"{path_to_download}/"
                                     f"{file_name}")

            return f"{file_name} was read"

        except FileNotFoundError as f:
            raise FileNotFoundError(f)

        except NotImplementedError as f:
            raise NotImplementedError(f)

    def update_file(self, file_name, filepath):
        """
        updates file from file path to bucket
        :param file_name: str/file name
        :param filepath: str/file path
        :return: message if operation was successful, else - error
        """

        blob = get_blob(self.bucket, file_name)

        try:
            old_hash = get_hash(self.bucket, file_name)

            self.bucket.delete_blob(file_name)
            blob.upload_from_filename(filepath)

            new_hash = get_hash(self.bucket, file_name)

            check_if_updated(blob, old_hash, new_hash)
            print(blob)
            return f"{file_name} was updated"

        except FileNotFoundError as f:
            raise FileNotFoundError(f)

        except NotImplementedError as f:
            raise NotImplementedError(f)

    def delete_file(self, file_name):
        """
        deletes file with filename
        :param file_name: str/file name
        :return: message if operation was successful, else - error
        """
        try:
            self.bucket.delete_blob(file_name)

            return f"{file_name} was deleted"

        except FileNotFoundError as f:
            raise FileNotFoundError(f)

        except NotImplementedError as f:
            raise NotImplementedError(f)
