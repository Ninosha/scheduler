from gcloud import storage
from utilities.crud_class import CRUDFuncs
from utilities.utils import get_credentials, get_blob


class CRUD:
    """
    class connects client, bucket, blob, and crud classes
    """

    def __init__(self, credentials_url, project_name,
                 bucket_name):
        """
        :param credentials_url: credentials path/str
        :param project_name: str/project name
        :param bucket_name: str/bucket name
        """

        self.project_name = project_name
        self.bucket_name = bucket_name
        self.credentials_url = credentials_url

    @property
    def crud_obj(self):
        """
        function passes wraps up parameters to initialize crud object
        :return: crud object from CRUDFuncs class
        """
        try:
            client = storage.Client(
                credentials=get_credentials(self.credentials_url),
                project=self.project_name
            )

        except NotImplementedError as f:
            raise NotImplementedError(f)

        bucket = client.get_bucket(self.bucket_name)

        crud_obj = CRUDFuncs(bucket)

        return crud_obj
