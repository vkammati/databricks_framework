# Databricks notebook source
pip install msal

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

import os
import sys
from pyspark.sql import functions as f

only_repos_dirs = set(['/'.join(p.split('/')[0:5]) for p in sys.path if p.startswith("/Workspace/Repos/")])
[sys.path.append(p) for p in only_repos_dirs if p not in sys.path]

os.environ["pipeline"] = "databricks"

# COMMAND ----------

import json
import time
def create_tags(tbl_tags):
    tags_dict = json.loads(f"\"{tbl_tags}\"").replace("{", "").replace("}", "")
    tags_final=tags_dict.replace(":", "=")
    print(tags_final)
    return tags_final

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
    get_euh_schema_name
)

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")


common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

base_config = BaseConfig.from_confs(env_conf, common_conf)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)
    
if unique_repo_branch_id_schema:
    base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)
edc_user_id = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_id_key)
edc_user_pwd = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_pwd_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)

# collibra_config = CollibraConfig(edc_user_id, edc_user_pwd, env_conf.api_url)
# collibra_config.read_json_string_from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/collibra_json_string.json")

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)
uc_raw_schema = base_config.get_uc_raw_schema()
print("uc_raw_schema : ",uc_raw_schema)
uc_euh_schema = base_config.get_uc_euh_schema()
print("uc_euh_schema : ",uc_euh_schema)


raw_folder_path = base_config.get_raw_folder_path()
print("raw_folder_path : ",raw_folder_path)
euh_folder_path = base_config.get_euh_folder_path()
print("euh_folder_path : ",euh_folder_path)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)
tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)


# COMMAND ----------

def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# COMMAND ----------

# assignPermission This function assigns Permission to all the tables created
def assignPermission(catalog,schema,table_name,tbl_owner,tbl_read):
    spark.sql(f"ALTER table `{catalog}`.`{schema}`.{table_name} owner to `{tbl_owner}`")
    print("Table Owner is assigned")
    spark.sql(f"GRANT ALL PRIVILEGES ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_owner}`")
    print("All privileges access given to tbl owner")
    spark.sql(f"GRANT SELECT ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_read}`")
    print("Reader access granted")

# COMMAND ----------

def assignTagsTable(catalog,schema,table,tags):
    print(tags)
    final_tags=create_tags(tags)
    spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.`{table}` SET TBLPROPERTIES({final_tags})")

# COMMAND ----------

def create_UC_table(path,uc_catalog_name,uc_schema_name,data_layer, table_list1):
    print("")
    print("uc_catalog_name : ",uc_catalog_name)
    print("uc_schema_name : ",uc_schema_name)
    print("inside1")
    df_schema_table = spark.sql(f"SHOW TABLES in `{uc_catalog_name}`.`{uc_schema_name}`").select("tableName")
    print("inside")
    table_list3 = [x.lower() for x in table_list1]
    table_list2 = [row[0] for row in df_schema_table.select('tableName').collect()]
    table_list = [i for i in table_list3 if i in table_list2]
    file_list = []
    tables_path_list = [path+"/"+j+"/" for j in table_list1 if file_exists(path+"/"+j+"/")]

    # commented this part to resovle INVALID_PARAMETER_VALUE: There are more than 1001 files error
    # for i in dbutils.fs.ls(path):

    #     print("")
    #     table_name = i[0]
    #     file_list.append(table_name)

    final_file_list = [i for i in tables_path_list if (i.split("/")[-2]) in table_list1]
    
    for i in final_file_list:

        print("")
        table_name = (i.split("/")[-2]).lower()
        table_path = i
        print("table_name : " , table_name)
        print("table_path : " , table_path)
        if file_exists(table_path+ "_delta_log") is True:
            print(table_path + "_delta_log")
            if table_name in table_list:
                print("Table already exists in UC : ", table_name )
                print("Syncing the metadata of the table")
                spark.sql(f"MSCK REPAIR TABLE `{uc_catalog_name}`.`{uc_schema_name}`.{table_name} SYNC METADATA")
                # if data_layer=="euh":
                #     (table_expr,table_tags)=fetch_business_metadata(table_name,collibra_config)
                #     assignTagsTable(uc_catalog_name,uc_schema_name,table_name,create_tags(table_tags).replace(":", "="))
                #dropTable(uc_catalog_name,uc_schema_name,table_name)                
            else:
                print("New Delta Table in data lake, creating new table in UC : " ,table_name)
                spark.sql(f"CREATE TABLE IF NOT EXISTS `{uc_catalog_name}`.`{uc_schema_name}`.{table_name} USING delta LOCATION '{table_path}'")
                #createTable(uc_catalog_name,uc_schema_name,table_name,table_path)
                assignPermission(uc_catalog_name,uc_schema_name,table_name,tbl_owner_grp,tbl_read_grp)
                # if data_layer=="euh":
                #     (table_expr,table_tags)=fetch_business_metadata(table_name,collibra_config)
                #     assignTagsTable(uc_catalog_name,uc_schema_name,table_name,create_tags(table_tags).replace(":", "="))

# COMMAND ----------

dlt_table_list = ['BKPF_STN', 'BKPF_TSAP', 'BSEG_TSAP','BSEG_STN','GLIDXA_STN','GLIDXA_TSAP','ZSTVA_PE_DEX_DTL_STN','ZSTVA_PE_MC_DTL_STN']
print("creating RAW tables in UC")
create_UC_table(raw_folder_path,uc_catalog_name,uc_raw_schema,"raw", dlt_table_list)
print("creating EUH tables in UC")
create_UC_table(euh_folder_path,uc_catalog_name,uc_euh_schema,"euh", dlt_table_list)
