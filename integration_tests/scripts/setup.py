# Databricks notebook source
# MAGIC %pip install --upgrade pip

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt

# COMMAND ----------

import os
os.environ["pipeline"] = "databricks"

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CommonConfig,
    EnvConfig,
    GreatExpectationsConfig,
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN

# COMMAND ----------

# DBTITLE 1,Parameters
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")
repos_path = dbutils.widgets.get(name="repos_path")
dlt_pipeline_id = dbutils.widgets.get(name="dlt_pipeline_id")
env = dbutils.widgets.get(name="env")


common_conf = CommonConfig.from_file("../../conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"../../conf/{env}/conf.json")

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)
#azure_conn_str = dbutils.secrets.get(scope=kv, key=env_conf.azure_connection_string_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)

#gx_config = GreatExpectationsConfig(azure_conn_str)

#base_config = BaseConfig.from_confs(env_conf, common_conf, gx_config)
base_config = BaseConfig.from_confs(env_conf, common_conf)
base_config.set_unique_id(unique_repo_branch_id)
base_config.set_unique_id_schema(unique_repo_branch_id_schema)

repos_confidential_data_path = f"file:/Workspace/{repos_path.strip('/')}/integration_tests/data/confidential/adf_dev"

repos_hana_confidential_data_path = f"{repos_confidential_data_path}/DS_HANA_CDD"

# COMMAND ----------

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

# DBTITLE 1,Configuration
uc_catalog = base_config.get_uc_catalog_name()

raw_schema = base_config.get_uc_raw_schema()
euh_schema = base_config.get_uc_euh_schema()

landing_non_confidential_folder_path = base_config.get_landing_folder_path()
landing_confidential_folder_path = base_config.get_landing_folder_path(True)

raw_folder_path = base_config.get_raw_folder_path()
euh_folder_path = base_config.get_euh_folder_path()

print("unity catalog: ", uc_catalog)
print("raw schema: ", raw_schema)
print("euh schema: ", euh_schema)
print("landing cofidential folder path: ", landing_confidential_folder_path)
print("raw folder path: ", raw_folder_path)
print("euh folder path: ", euh_folder_path)

# COMMAND ----------

# MAGIC %md ## Delete UC schemas and tables

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{raw_schema}` CASCADE")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{euh_schema}` CASCADE")

# COMMAND ----------

# MAGIC %md ## Delete ADLS folders

# COMMAND ----------

# DBTITLE 1,Delete landing confidential folder
if env == "dev":
    dbutils.fs.rm(landing_confidential_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete raw folder
if env == "dev":
    dbutils.fs.rm(raw_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete EUH folder
if env == "dev":
    dbutils.fs.rm(euh_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete DLT pipeline metadata
if env == "dev":
    dbutils.fs.rm(f"dbfs:/pipelines/{dlt_pipeline_id}", recurse=True)

# COMMAND ----------

# MAGIC %md ## Add files to the landing layer

# COMMAND ----------

confidential_data_folders = [folder.path.strip("/") for folder in dbutils.fs.ls(repos_confidential_data_path)]

if not confidential_data_folders:
    raise Exception("Integration tests data folder is empty")

# COMMAND ----------

if repos_hana_confidential_data_path in confidential_data_folders:
    dbutils.fs.cp(repos_hana_confidential_data_path, landing_confidential_folder_path, recurse=True)
    print("Confidential hana data copied")
else:
    print("No confidential hana data in integration tests data folder.")

# COMMAND ----------

# DBTITLE 1,Create UC schemas
if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{raw_schema}`")

# COMMAND ----------

if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{euh_schema}`")
