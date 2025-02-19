import logging
import io
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)


def load_json_data(model, file_dir, fn, storage_account, adls2):
    """Loads Pydantic model as json files into ADLS2."""
    file_name = f"{fn}.json"
    json_data = model.model_dump_json(indent=4)
    json_to_bytes = io.BytesIO(json_data.encode("utf-8"))
    with json_to_bytes as file_obj:
        upload_to_adls2(adls2, file_obj, storage_account, file_dir, file_name)


def load_parquet_data(data, file_dir, fn, storage_account, adls2):
    """Loads parquet data directly into ADLS2 storage_account."""
    file_name = f"{fn}.parquet"
    with data as file_obj:
        upload_to_adls2(adls2, file_obj, storage_account, file_dir, file_name)


def upload_to_adls2(adls2, data, storage_account, container, file_path):
    try:
        adls2_client = DataLakeServiceClient(
            account_url=f'https://{storage_account}.dfs.core.windows.net',
            credential=adls2
        )
        file_system_client = adls2_client.get_file_system_client(file_system=container)
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(data, overwrite=True)
        logger.info(
            f"File '{container}/{file_path}' uploaded to ADLS2 storage_account '{storage_account}' successfully!"
        )
    except Exception as e:
        logger.error(f"Error uploading file: {e}")