# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Catalog Creation
# MAGIC
# MAGIC #### Catalog :
# MAGIC dnastats
# MAGIC
# MAGIC #### Schema :
# MAGIC ######. raw-dna_platform_statistics
# MAGIC ######. euh-dna_platform_statistics
# MAGIC ######. eh-itirm-it_application_service
# MAGIC ######. curated-dnastats

# COMMAND ----------

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS `reference_products-unitycatalog-$env` ;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `reference_products-unitycatalog-$env`;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `raw-dna_platform_statistics`;
# MAGIC CREATE SCHEMA IF NOT EXISTS `euh-dna_platform_statistics`;
# MAGIC CREATE SCHEMA IF NOT EXISTS `eh-itirm-it_application_service`;
# MAGIC CREATE SCHEMA IF NOT EXISTS `curated-dnastats`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES
# MAGIC  ON CATALOG `reference_products-unitycatalog-$env` TO `AZ-DNA-GRP-ZZ-113-UC-RS-BUSINESS-ADMIN-DNASTATS` ;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER CATALOG `reference_products-unitycatalog-$env` OWNER TO `AZ-DNA-GRP-ZZ-113-UC-RS-BUSINESS-ADMIN-DNASTATS`;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `reference_products-unitycatalog-$env`;
# MAGIC ALTER SCHEMA `raw-dna_platform_statistics` OWNER TO `AZ-DNA-GRP-ZZ-113-UC-RS-BUSINESS-ADMIN-DNASTATS`;
# MAGIC ALTER SCHEMA `euh-dna_platform_statistics` OWNER TO `AZ-DNA-GRP-ZZ-113-UC-RS-BUSINESS-ADMIN-DNASTATS`;
# MAGIC ALTER SCHEMA `eh-itirm-it_application_service` OWNER TO `AZ-DNA-GRP-ZZ-113-UC-RS-BUSINESS-ADMIN-DNASTATS`;
# MAGIC ALTER SCHEMA `curated-dnastats` OWNER TO `AZ-DNA-GRP-ZZ-113-UC-RS-BUSINESS-ADMIN-DNASTATS`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG on catalog `reference_products-unitycatalog-$env` TO `AZ-DNA-GRP-ZZ-113-UC-RS-PROJECT-USER-READER-REFERENCE_PRODUCTS`;
# MAGIC GRANT USE SCHEMA on catalog `reference_products-unitycatalog-$env` TO `AZ-DNA-GRP-ZZ-113-UC-RS-PROJECT-USER-READER-REFERENCE_PRODUCTS`;
# MAGIC GRANT SELECT on catalog `reference_products-unitycatalog-$env` TO `AZ-DNA-GRP-ZZ-113-UC-RS-PROJECT-USER-READER-REFERENCE_PRODUCTS`;
