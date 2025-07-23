# Installing libraries when working with DLT

In this project, we are using two types of Python libraries:
1. The `reference_pipeline_library` library we created and that contains all business and helper functions
2. External libraries, e.g. pyspark or pytest, that we install from external package repositories.

When working with Databricks DLT, each type has its own installation mechanism.

## Installing a local library

NB: by "local" we mean a library that is in the same project folder as the DLT pipeline notebooks. For the reference pipeline, this is the `reference_pipeline_library` library.

The simplest way to install a local library is to use the integration between Databricks Repos and Databricks DLT and append the Repos project path to the system path at the top of the DLT pipeline notebook (at least before importing the local library). That way, all local libraries are available (aka installed) by default:

Example code:
```python
import sys
sys.path.append("/Workspace/Repos/path/to/the/project/folder")

# this will work
import reference_pipeline_library
```
Working this way prevents us from having to deal with Python wheels and their associated overhead. Interestingly, this also drastically simplifies the CI/CD pipeline.

You can see this install and import in action in the first two cells of the [reference pipeline notebook](../../dlt_pipelines/reference_pipeline_main.py).

NB: a new feature will be released on Databricks very soon that will remove the need to use `sys.path.append`.

## Installing external libraries

We have to install external libraries in two scenarios:
1. In a terminal before running unit tests
2. Right before the DLT pipeline runs so that we can import and use those external libraries in the DLT pipeline

The first scenario impacts the second one, so let's tackle them in order.

### In a terminal to run unit tests

We use the classical way of installing external libraries for any Python project: we put all dependencies with their pinned versions in a `requirements.txt` file and we install them via `pip`:
```bash
pip install -r requirements.txt
```

This is exactly what is done in the CI/CD before running unit tests. You can do the same thing when working on the code locally with an IDE for instance.

### For a DLT pipeline

To install external libraries for a DLT pipeline, we have to use the `%pip install` magic command. According to the [DLT documentation](https://learn.microsoft.com/en-us/azure/databricks/workflows/delta-live-tables/delta-live-tables-python-ref#--python-libraries):
> When an update starts, Delta Live Tables runs all cells containing a `%pip install` command before running any table definitions. Every Python notebook included in the pipeline has access to all installed libraries.

That is why we use that command in the dedicated notebook [install_requirements](../../dlt_pipelines/install_requirements.py). To avoid duplicating code and not installing the latest dependencies and/or versions, we reuse the `requirements.txt` file created before:
```python
requirements_filepath = spark.conf.get("pipeline.requirements_filepath")

# NB: using `with open(...) as f:` raises error "IndentationError: expected an indented block"
requirements_file = open(requirements_filepath)
libraries = requirements_file.read().splitlines()
libraries_str = " ".join(libraries)
requirements_file.close()

%pip install $libraries_str
```

NB: we currently have to use a Spark configuration parameter `pipeline.requirements_filepath` that is added dynamically to the DLT pipeline configuration to be able to retrieve the absolute `requirements.txt` file path.
