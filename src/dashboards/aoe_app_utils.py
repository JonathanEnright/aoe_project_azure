import streamlit as st
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
import io
import gzip
from streamlit import session_state as ss
import os
from dotenv import load_dotenv

# Load .env only if not on Cloud
if not os.environ.get('STREAMLIT_CLOUD_RUN', 'False').lower() == 'true':
    load_dotenv() 


# ----------------------------------------------------------------------------------------------------------------------
def download_file_from_adls2(adls2_credential, storage_account, container, file_path):
    try:
        # Create a DataLakeServiceClient
        adls2_client = DataLakeServiceClient(
            account_url=f'https://{storage_account}.dfs.core.windows.net',
            credential=adls2_credential
        )
        
        # Get the file system client (container)
        file_system_client = adls2_client.get_file_system_client(file_system=container)
        
        # Get the file client (FIX: use get_file_client() instead of get_path_client())
        file_client = file_system_client.get_file_client(file_path)
        
        # Download the file content
        download = file_client.download_file()
        file_content = download.readall()
        
        # Decompress the file content
        with gzip.GzipFile(fileobj=io.BytesIO(file_content)) as f:
            decompressed_content = f.read()
        
        # Load the decompressed file content into a pandas DataFrame
        df = pd.read_csv(io.StringIO(decompressed_content.decode('utf-8')))
        return df
    except Exception as e:
        st.error(f"Error downloading file: {e}")
        return None



@st.cache_data
def get_data(storage_account, container, file_path):
    """
    Try to connect to adls2 using either .env variables or streamlit secrets
    If successful, call the download function to read it as a dataframe
    """    

    try:
        adls2_credential = ClientSecretCredential(
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )
    except:
        # Retrieve secrets from Streamlit's secrets.toml
        client_id = st.secrets["azure_client_id"]
        tenant_id = st.secrets["azure_tenant_id"]
        client_secret = st.secrets["azure_client_secret"]

        # Create a credential object for Azure authentication
        adls2_credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

    
    # Download the file and load it into a DataFrame
    df = download_file_from_adls2(adls2_credential, storage_account, container, file_path)
    print("file downloaded")
    df['selected'] = True
    if df is not None:
        pass
        # st.toast("Successfuly read data!")
    else:
        st.error("Unable to read data.")
    return df
# ----------------------------------------------------------------------------------------------------------------------


def initialize_state(categorical_filters, prefix):
    '''Set session_state for filter variables and counter.'''
    for col in categorical_filters:
        if f"{prefix}_{col}_query" not in st.session_state:
            st.session_state[f"{prefix}_{col}_query"] = []

    if "counter" not in st.session_state:
        st.session_state.counter = 0


def reset_state_callback():
    '''Reset session state for filter variables when button clicked.'''
    st.session_state.counter = 1 + st.session_state.counter
    for key in st.session_state.keys():
        # if key.endswith("_query"):
        st.session_state[key] = []

def query_data(df: pd.DataFrame, categorical_filters, prefix) -> pd.DataFrame:
    '''Filter the DataFrame based on session state selections.'''
    for col in categorical_filters:
        if st.session_state[f"{prefix}_{col}_query"]:
            df["selected"] &= df[col].isin(st.session_state[f"{prefix}_{col}_query"])
    return df
