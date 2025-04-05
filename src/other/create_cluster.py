import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    AzureAttributes,
    AzureAvailability,
    DataSecurityMode,
    Library,
    PythonPyPiLibrary,
    RuntimeEngine,
)
from dotenv import dotenv_values, load_dotenv, set_key

# Prompt the user for confirmation
user_input = (
    input(
        "This script create a Databricks cluster and install libraries on it. "
        "To apply changes you will need to close and re-open the terminal. "
        "Do you want to proceed? (yes/no): "
    )
    .strip()
    .lower()
)

if user_input != "yes":
    print("Script execution canceled.")
    sys.exit(0)

# Load environment variables from your .env file
env_vars = dotenv_values(".env")

# Initialize the Databricks WorkspaceClient
client = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST_URL"), token=os.getenv("DATABRICKS_PAT")
)

# Define the cluster configuration as a dictionary
cluster_config = {
    "cluster_name": "Aoe_Project_Cluster",
    "spark_version": "15.4.x-scala2.12",
    "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode",
    },
    "azure_attributes": AzureAttributes(
        first_on_demand=1,
        availability=AzureAvailability.ON_DEMAND_AZURE,
        spot_bid_max_price=-1,
    ),
    "node_type_id": "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
    "custom_tags": {"ResourceClass": "SingleNode"},
    "spark_env_vars": env_vars,
    "autotermination_minutes": 20,
    "enable_elastic_disk": True,
    "init_scripts": [],
    "single_user_name": "jonathan.enright@live.com.au",
    "enable_local_disk_encryption": False,
    "data_security_mode": DataSecurityMode.SINGLE_USER,
    "runtime_engine": RuntimeEngine.STANDARD,
    "num_workers": 0,
    "apply_policy_default_values": False,
}

# Create the cluster using the ClustersAPI
try:
    cluster_response = client.clusters.create(**cluster_config)
    id = cluster_response.cluster_id
    print(f"Cluster created successfully! Cluster ID: {id}")
    # Update the .env file with the new cluster ID
    set_key(".env", "DATABRICKS_CLUSTER_ID", id)
except Exception as e:
    print(f"Error creating cluster: {e}")


libraries = [
    Library(pypi=PythonPyPiLibrary(package="pydantic==2.6.2")),
    Library(pypi=PythonPyPiLibrary(package="azure-storage-file-datalake")),
    Library(pypi=PythonPyPiLibrary(package="python-dotenv")),
    Library(pypi=PythonPyPiLibrary(package="azure-identity==1.19.0")),
    Library(pypi=PythonPyPiLibrary(package="databricks-sdk==0.29.0")),
]

# Install the libraries on the cluster
try:
    client.libraries.install(cluster_id=id, libraries=libraries)
    print("Libraries installed successfully.")
except Exception as e:
    print(f"Error installing libraries: {e}")

# Reload the environment variables to ensure the updated DATABRICKS_CLUSTER_ID is used
load_dotenv(".env")

# Success message
print(
    "Script completed successfully. You will need to close this terminal to apply changes."
)
