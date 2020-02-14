from os import path
import pathlib

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.requests_hook import RequestsHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

class LaunchLibraryOperator(BaseOperator):

    template_fields = ('_endpoint', '_query_params', '_result_path', '_result_filename')

    @apply_defaults
    def __init__(self,
                 request_conn_id: str,
                 gcs_conn_id: str = None,
                 endpoint: str,
                 params: dict = None,
                 result_bucket: str = None,
                 result_path: str,
                 result_filename: str,
                 do_xcom_push=False
                 * args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._request_conn_id = request_conn_id
        if gcs_conn_id:
            self._gcs_conn_id = gcs_conn_id
        self._endpoint = endpoint
        self._params = params
        self._result_bucket = result_bucket
        self._result_path = result_path
        self._result_filename = result_filename

    def execute(self, context):
        request_hook = RequestsHook(conn_id=self._request_conn_id)

        response = request_hook.request(
            method='GET',
            url=self._endpoint,
            params=self._params
        )

        gcp_storage_hook = GoogleCloudStorageHook()

        full_path = path.join(self._result_path, f'ds={context['ds']}', self._result_filename)

        full_io_path = path.join('/tmp/', full_path)

        pathlib.Path(full_io_path).mkdir(parents=True, exist_ok=True)

        with open(full_io_path, "w") as f:
            print(f"Writing to file {f.name}")
            f.write(response.text)

        if self._result_bucket:
            response = gcp_storage_hook.upload(
                bucket=self._result_bucket,
                object=full_path,
                filename=full_io_path
            )

        return 'OK'
