# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

pip install requests msal cryptography

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CollibraConfig,
    CommonConfig,
    EnvConfig
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
workspace_url = dbutils.widgets.get(name="workspace_url")
job_id = dbutils.widgets.get(name="job_id")

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json") 
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

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

spnvalue = spn.generate_ad_token()
print(spnvalue)

# COMMAND ----------

import requests, json, datetime

def running_job(workspace_url,spnvalue,job_id):
    adb_token = spnvalue
    workspace_url = workspace_url
    queries_end_point = "/api/2.1/jobs/run-now"
    job_id= {"job_id":job_id}

    headers = {"Authorization": "Bearer {}".format(adb_token)}
    response = requests.post("{}{}".format(workspace_url,queries_end_point), headers=headers, data=json.dumps(job_id))
    response_json = response.json()
    print("response_json",response_json)

# COMMAND ----------

running_job(workspace_url,spnvalue,job_id)
