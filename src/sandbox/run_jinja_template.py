# Step0: Import jinia & all modules to be used in any of the transformation scripts.
import os

from jinja2 import Environment, FileSystemLoader, pass_context

from src.common.base_utils import create_databricks_session
from src.common.logging_config import setup_logging
from src.common.transform_utils import write_to_table

logger = setup_logging()


dev_env_catalog = "aoe_dev"
# Step1: Set context variables. Ideally this would be dynamic, based on target environment.
# In a large project, all source/target scripts (models) will have a defined schema assoiated with them that can be invoked here.
# In this example, we will mock this up with the below dict.
table_lookup = {
    "matches_br": {
        "name": "matches_br",
        "schema": "bronze",
        "catalog": dev_env_catalog,
    },
    "players_sr": {
        "name": "players_sr",
        "schema": "silver",
        "catalog": dev_env_catalog,
    },
    "example_script": {
        "name": "example_script",
        "schema": "silver",
        "catalog": dev_env_catalog,
    },
}

transformed_df = {}

# Mock chosing to run a speicified table.
example_target = table_lookup["example_script"]
target_file = f"{example_target['name']}.py"
target_table = (
    f"{example_target['catalog']}.{example_target['schema']}.{example_target['name']}"
)


# Step2: Update the 'reff' macro to include catalog & schema context.
@pass_context
def reff(context, table):
    table_info = table_lookup.get(table, {})
    catalog = table_info.get("catalog", "")
    schema = table_info.get("schema", "")
    return f"{catalog}.{schema}.{table}"


# Step3: Create a jinja env using the current directory.
script_dir = os.path.dirname(os.path.abspath(__file__))
jinja_env = Environment(loader=FileSystemLoader(script_dir))

# Step4: Register the 'reff' macro globally in the jinja env.
jinja_env.globals["reff"] = reff
print(jinja_env)

# Step5: Read the python transformation file, populating the reff macro with the fully qualified name.
template = jinja_env.get_template(target_file)
content = template.render()
print(content)


# Step6: Connect to databricks
logger.info(f"Script '{os.path.basename(__file__)}' complete.")
spark = create_databricks_session()

# Step7: Execute the transformation instructions from rendered script
logger.info(f"Executing transformations defined in {target_file}.")
exec(content)

# Step 8: Save the resulting dataframe [transformed_df] into Databricks Unity catalog
write_to_table(transformed_df, target_table)
logger.info(f"Script '{os.path.basename(__file__)}' complete.")
