# Databricks notebook source
pip install msal

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

# DBTITLE 1,Import Functions
from pyspark.sql.functions import col,max,lit
from pyspark.sql.types import DecimalType
from delta.tables import *
from pyspark.sql import functions as f

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CollibraConfig,
    CommonConfig,
    EnvConfig
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
from datta_pipeline_library.edc.collibra import fetch_business_metadata
from datta_pipeline_library.helpers.uc import (
    get_catalog_name,
    get_raw_schema_name,
    get_euh_schema_name,
    get_eh_schema_name,
)

# COMMAND ----------

# DBTITLE 1,Reading Input Parameters From ADF Using Widgets
env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id = unique_repo_branch_id.replace("datta","")
target_path = dbutils.widgets.get("target_path")
table_name = dbutils.widgets.get("table_name")
config_table_name = dbutils.widgets.get("config_table_name")
process_id=dbutils.widgets.get("process_id")
table_classification=dbutils.widgets.get("table_classification")

print("unique_repo_branch_id:",unique_repo_branch_id)

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json") 
print("common_conf")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

base_config = BaseConfig.from_confs(env_conf, common_conf)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)

# COMMAND ----------

kv = env_conf.kv_key
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)
edc_user_id = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_id_key)
edc_user_pwd = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_pwd_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)
configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)
uc_raw_schema = base_config.get_uc_raw_schema()
print("uc_raw_schema : ",uc_raw_schema)

landing_folder_path = base_config.get_landing_folder_path()
print("landing_folder_path : ",landing_folder_path)
raw_folder_path = base_config.get_raw_folder_path()
print("raw_folder_path : ",raw_folder_path)

# COMMAND ----------

# DBTITLE 1,Selecting Max of LAST_DTM And Creating Source Table
if env=="dev":
  landing_folder_path = landing_folder_path.replace("internal/adf_dev/DS_HANA_DDD", target_path).replace("internal/adf_dev/DS_HANA_CDD", target_path)
  print("dev landing_folder_path : ",landing_folder_path)

elif env=="tst":
  landing_folder_path = landing_folder_path.replace("internal/adf_tst/DS_HANA_CDD", target_path)
  print("tst landing_folder_path : ",landing_folder_path)
elif env=="pre":
  landing_folder_path = landing_folder_path.replace("internal/adf_pre/DS_HANA_ZDD", target_path)
  print("pre landing_folder_path : ",landing_folder_path)
elif env=="prd":
  landing_folder_path = landing_folder_path.replace("internal/adf_prd/DS_HANA_PDD", target_path)
  print("prd landing_folder_path : ",landing_folder_path)
else:
  print("please provide the environment variable") 

df_landing = spark.read.parquet(landing_folder_path)
df_max_lastDTM = df_landing.select(max(df_landing.LAST_DTM).alias("LAST_DTM"))

df_max_lastDTM = df_max_lastDTM.withColumn("LAST_DTM", df_max_lastDTM.LAST_DTM.cast(DecimalType(15,0)))

final_df = df_max_lastDTM.withColumn("TABLE_NAME", lit(table_name)).withColumn("LOAD_TYPE", lit("DELTA_LOAD_HANA")).withColumn("process_id",lit(process_id)).withColumn("table_classification",lit(table_classification))
display(final_df)


# COMMAND ----------

# DBTITLE 1,Merging Source and Target Table to Update Target Table (WaterMarkTable)
target_table = DeltaTable.forName(spark, f"`{uc_catalog_name}`.`{uc_raw_schema}`.{config_table_name}")
display(target_table)

(target_table.alias('target_wm_table')
  .merge(final_df.alias('source_table'), "target_wm_table.table_name = source_table.TABLE_NAME and target_wm_table.load_type = source_table.LOAD_TYPE and target_wm_table.process_id = source_table.process_id and target_wm_table.table_classification = source_table.table_classification")
  .whenMatchedUpdate(set = {"target_wm_table.LAST_DTM" : "source_table.LAST_DTM","target_wm_table.row_updated_date" : f.current_timestamp()})
  .execute())
