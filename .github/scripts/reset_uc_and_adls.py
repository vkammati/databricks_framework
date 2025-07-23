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
print("unique_repo_branch_id : ", unique_repo_branch_id)
print("unique_repo_branch_id_schema : ", unique_repo_branch_id_schema)
print("repos_path : ", repos_path)
print("dlt_pipeline_id : ", dlt_pipeline_id)
print("env : ", env)


common_conf = CommonConfig.from_file("../../conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"../../conf/{env}/conf.json")

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)


base_config = BaseConfig.from_confs(env_conf, common_conf)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)
if unique_repo_branch_id_schema:
    base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

# DBTITLE 1,Configuration
uc_catalog = base_config.get_uc_catalog_name()
print("uc_catalog : ", uc_catalog)

raw_schema = base_config.get_uc_raw_schema()
print("raw_schema : ", raw_schema)
euh_schema = base_config.get_uc_euh_schema()
print("euh_schema : ", euh_schema)

landing_non_confidential_folder_path = base_config.get_landing_folder_path()
print("landing_non_confidential_folder_path : ", landing_non_confidential_folder_path)
landing_confidential_folder_path = base_config.get_landing_folder_path(True)
print("landing_confidential_folder_path : ", landing_confidential_folder_path)
bw_landing_non_confidential_folder_path = base_config.get_bw_landing_folder_path()
print("bw_landing_non_confidential_folder_path : ", bw_landing_non_confidential_folder_path)
bw_landing_confidential_folder_path = base_config.get_bw_landing_folder_path(True)
print("bw_landing_confidential_folder_path : ", bw_landing_confidential_folder_path)
raw_folder_path = base_config.get_raw_folder_path()
print("raw_folder_path : ", raw_folder_path)
euh_folder_path = base_config.get_euh_folder_path()
print("euh_folder_path : ", euh_folder_path)

# COMMAND ----------

# MAGIC %md ## Drop schemas and tables

# COMMAND ----------

def drop_all_tables_in_schema(uc_catalog_name,uc_schema_name):
    print("")
    print("uc_catalog_name : ",uc_catalog_name)
    print("uc_schema_name : ",uc_schema_name)
    df_schema_table = spark.sql(f"SHOW TABLES in `{uc_catalog_name}`.`{uc_schema_name}`").select("tableName")
    table_list = [row[0] for row in df_schema_table.select('tableName').collect()]
    if not table_list  :
        print(uc_schema_name , " || The schema is empty , NO Tables to delete ")
    else :
        for row in table_list:
            table_name = row
            print(f"Dropping table `{uc_catalog_name}`.`{uc_schema_name}`.{table_name}")
            spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog_name}`.`{uc_schema_name}`.{table_name}")

# COMMAND ----------

if env == "dev":
    print(f"Dropping schema AND all tables :  `{uc_catalog}`.`{raw_schema}`")
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{raw_schema}` CASCADE")

# COMMAND ----------

if env == "dev":
    print(f"Dropping schema AND all tables :  `{uc_catalog}`.`{euh_schema}`")
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{euh_schema}` CASCADE")

# COMMAND ----------

# MAGIC %md ## Delete ADLS folders

# COMMAND ----------

# DBTITLE 1,Delete HANA landing non-confidential folder
# in dev we're using an intermediate folder per branch where we will deploy integration tests data
# in tst/pre/prd, the data is already here and there is no intermediate folder. We should not delete anything.
if env == "dev":
    dbutils.fs.rm(landing_non_confidential_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete BW landing non-confidential folder
# in dev we're using an intermediate folder per branch where we will deploy integration tests data
# in tst/pre/prd, the data is already here and there is no intermediate folder. We should not delete anything.
if env == "dev":
    dbutils.fs.rm(bw_landing_non_confidential_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete HANA landing confidential folder
# in dev we're using an intermediate folder per branch where we will deploy integration tests data
# in tst/pre/prd, the data is already here and there is no intermediate folder. We should not delete anything.
if env == "dev":
    dbutils.fs.rm(landing_confidential_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete BW landing confidential folder
# in dev we're using an intermediate folder per branch where we will deploy integration tests data
# in tst/pre/prd, the data is already here and there is no intermediate folder. We should not delete anything.
if env == "dev":
    dbutils.fs.rm(bw_landing_confidential_folder_path, recurse=True)

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
    if dlt_pipeline_id:
        dbutils.fs.rm(f"dbfs:/pipelines/{dlt_pipeline_id}", recurse=True)
    else:
        print("No pipeline id found.")
