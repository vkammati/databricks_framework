# Databricks notebook source
# MAGIC %pip install --upgrade pip

# COMMAND ----------

requirements_filepath = spark.conf.get("pipeline.requirements_filepath")

# NB: using `with open(...) as f:` raises error "IndentationError: expected an indented block"
requirements_file = open("/Workspace/" + requirements_filepath)
libraries = requirements_file.read().splitlines()
libraries_str = " ".join(libraries)
requirements_file.close()

%pip install $libraries_str
