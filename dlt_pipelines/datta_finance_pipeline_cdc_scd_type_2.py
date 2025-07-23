# Databricks notebook source
import os
import sys

only_repos_dirs = set(['/'.join(p.split('/')[0:5]) for p in sys.path if p.startswith("/Workspace/Repos/")])
[sys.path.append(p) for p in only_repos_dirs if p not in sys.path]

os.environ["pipeline"] = "databricks"

# COMMAND ----------



# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CollibraConfig,
    CommonConfig,
    EnvConfig,
    GreatExpectationsConfig,
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
from datta_pipeline_library.helpers.uc import (
    get_catalog_name,
    get_raw_schema_name,
    get_euh_schema_name,
    get_eh_schema_name,
)
from datta_pipeline_library.transformation.landing_raw_scd.execute import execute_landing_raw
from datta_pipeline_library.transformation.raw_euh_scd.execute import execute_raw_euh

# COMMAND ----------

# TODO: NB: there are 2 other env parameters set at the pipeline level:
#  - "pipeline.register_uc_job_id"
#  - "pipeline.host"
#  those parameters are used in function register_table
env = spark.conf.get("pipeline.env")
unique_repo_branch_id = spark.conf.get("pipeline.unique_repo_branch_id")
repos_path = spark.conf.get("pipeline.repos_path")


# COMMAND ----------

# DBTITLE 1,Read conf files
common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)
edc_user_id = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_id_key)
edc_user_pwd = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_pwd_key)
azure_conn_str = dbutils.secrets.get(scope=kv, key=env_conf.azure_connection_string_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)

collibra_config = CollibraConfig(edc_user_id, edc_user_pwd, env_conf.api_url)
collibra_config.read_json_string_from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/collibra_json_string.json")

gx_config = GreatExpectationsConfig(azure_conn_str)

base_config = BaseConfig.from_confs(env_conf, common_conf, gx_config)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)

# COMMAND ----------

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

execute_landing_raw(spn, base_config)
execute_raw_euh(spn, base_config)
