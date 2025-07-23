# Databricks notebook source
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC use catalog `reference_products-unitycatalog-$env`;
# MAGIC
# MAGIC use `curated-dnastats`;
# MAGIC
# MAGIC drop table if exists costmanagement;
# MAGIC drop table if exists costresources;
# MAGIC drop table if exists date;
# MAGIC drop table if exists managedresourcesparentdetails;
# MAGIC drop table if exists resourcegroups;
# MAGIC drop table if exists resourcelist;
# MAGIC drop table if exists resourcetags;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `reference_products-unitycatalog-$env`;
# MAGIC use `eh-itirm-it_application_service`;
# MAGIC
# MAGIC drop table if exists costanalysis;
# MAGIC drop table if exists currencyconversion;
# MAGIC drop table if exists date;
# MAGIC drop table if exists resourcelist;
# MAGIC drop table if exists resourcetypealias;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `reference_products-unitycatalog-$env`;
# MAGIC use `euh-dna_platform_statistics`;
# MAGIC
# MAGIC drop table if exists azure_resourcegroups         ;
# MAGIC drop table if exists azure_resources              ;
# MAGIC drop table if exists azure_subscriptions          ;
# MAGIC drop table if exists business                     ;
# MAGIC drop table if exists cost_management              ;
# MAGIC drop table if exists currencyconversion           ;
# MAGIC drop table if exists date                         ;
# MAGIC drop table if exists environment                  ;
# MAGIC drop table if exists project                      ;
# MAGIC drop table if exists resourcetypealias            ;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `reference_products-unitycatalog-$env`;
# MAGIC use `raw-dna_platform_statistics`;
# MAGIC
# MAGIC drop table if exists azure_resourcegroups               ;
# MAGIC drop table if exists azure_resources                    ;
# MAGIC drop table if exists azure_subscriptions                ;
# MAGIC drop table if exists business                           ;
# MAGIC drop table if exists cost_management                    ;
# MAGIC drop table if exists currencyconversion                 ;
# MAGIC drop table if exists date                               ;
# MAGIC drop table if exists environment                        ;
# MAGIC --drop table invalid_raw_azure_resourcegroups   ;
# MAGIC drop table if exists project                            ;
# MAGIC drop table if exists resourcetypealias                  ;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `reference_products-unitycatalog-$env`;
# MAGIC
# MAGIC drop schema `raw-dna_platform_statistics` cascade ;
# MAGIC drop schema `euh-dna_platform_statistics` cascade ;
# MAGIC drop schema `eh-itirm-it_application_service` cascade ;
# MAGIC drop schema `curated-dnastats` cascade

# COMMAND ----------


