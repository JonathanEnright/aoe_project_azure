import logging
import io
from utils import upload_to_adls2


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
