"""
Landing to Raw Module
"""
from typing import Optional
from datta_pipeline_library.helpers.spn import AzureSPN
from datta_pipeline_library.core.base_config import BaseConfig
from datta_pipeline_library.core.lif import dlt_raw_stream, dlt_raw_batch

def landing_raw(
        spn: AzureSPN,
        base_config: BaseConfig,
        dataset_name: str,
        input_schema: str,
        transform: object,
        input_format: str = "json",
        partitions: Optional[list] = None,
        ge_rules: Optional[list] = None,
        confidentiality: Optional[list] = None):
    """
    This function takes the data from landing using autoloader creates a DLT View,
    then reads the data from the DLT view and loads the data into raw schema
    Whole approach is done in Streaming mode as the data in raw gets appended everytime when new dataset is landed

    Autoloader documentation:
    https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/dlt#auto-loader-syntax-for-dlt

    :param transform: simple transform function as input for landing view
    :param partitions: partitions column
    :param ge_rules: great expectation rule
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param dataset_name: name of the dataset which to be loaded
    :param input_schema: Structype schema
    :param input_format: Input format "csv" or "json"
    :param ge_rules: list of expectations for the given dataset
    """
    catalog = base_config.get_uc_catalog_name()
    uc_raw_schema = base_config.get_uc_raw_schema()

    dirlst = base_config.get_dataset_config(dataset_name,confidentiality)
    (landing_view_name, landing_data_path) = dirlst['landing_view_name'], dirlst['landing_data_path']
    (raw_table_name, raw_data_path) = dirlst['raw_table_name'], dirlst['raw_data_path']
    (archive_table_name, archive_table_path) = dirlst['archive_raw_table_name'], dirlst['archive_raw_data_path']

    print("core/landing_raw.py landing_data_path",landing_data_path)
    print("core/landing_raw.py landing_view_name",landing_view_name)
    dlt_raw_stream(landing_view_name, landing_data_path, raw_data_path, raw_table_name,
                   {"eds.quality": "raw"}, partitions, input_format,
                   input_schema, ge_rules, transform, base_config, archive_table_name, archive_table_path)

def landing_raw_aecorsoft(
        spn: AzureSPN,
        base_config: BaseConfig,
        dataset_name: str,
        input_schema: str,
        transform: object,
        input_format: str = "json",
        partitions: Optional[list] = None,
        ge_rules: Optional[list] = None,
        confidentiality: Optional[list] = None,
        raw_source_name: Optional[list] = None):
    """
    This function takes the data from landing using autoloader creates a DLT View,
    then reads the data from the DLT view and loads the data into raw schema
    Whole approach is done in Streaming mode as the data in raw gets appended everytime when new dataset is landed

    Autoloader documentation:
    https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/dlt#auto-loader-syntax-for-dlt

    :param transform: simple transform function as input for landing view
    :param partitions: partitions column
    :param ge_rules: great expectation rule
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param dataset_name: name of the dataset which to be loaded
    :param input_schema: Structype schema
    :param input_format: Input format "csv" or "json"
    :param ge_rules: list of expectations for the given dataset
    """
    catalog = base_config.get_uc_catalog_name()
    uc_raw_schema = base_config.get_uc_raw_schema()

    dirlst = base_config.get_dataset_config(dataset_name,confidentiality)
    (aecorsoft_landing_view_name, aecorsoft_landing_path) = dirlst['aecorsoft_landing_view_name'], dirlst['aecorsoft_landing_data_path']
    
    (raw_table_name, raw_data_path) = raw_source_name+'_'+dirlst['raw_table_name'].replace('raw_',''), dirlst['aecorsoft_raw_data_path']
    (archive_table_name, archive_table_path) = dirlst['archive_raw_table_name'], dirlst['archive_raw_data_path']
    
    print("core/landing_raw.py landing_raw_aecorsoft() aecorsoft_landing_path",aecorsoft_landing_path)
    print("core/landing_raw.py landing_raw_aecorsoft() aecorsoft_landing_view_name",aecorsoft_landing_view_name)
    print("core/landing_raw.py landing_raw_aecorsoft() dataset_name",dataset_name)
    print("core/landing_raw.py landing_raw_aecorsoft() raw_table_name",raw_table_name)
    print("core/landing_raw.py landing_raw_aecorsoft() raw_data_path",raw_data_path)

    dlt_raw_stream(aecorsoft_landing_view_name, aecorsoft_landing_path, raw_data_path, raw_table_name,
                   {"eds.quality": "raw"}, partitions, input_format,
                   input_schema, ge_rules, transform, base_config, archive_table_name, archive_table_path)

def landing_raw_bw(
        spn: AzureSPN,
        base_config: BaseConfig,
        dataset_name: str,
        input_schema: str,
        transform: object,
        input_format: str = "json",
        partitions: Optional[list] = None,
        ge_rules: Optional[list] = None,
        confidentiality: Optional[list] = None):
    """
    This function takes the data from landing using autoloader creates a DLT View,
    then reads the data from the DLT view and loads the data into raw schema
    Whole approach is done in Streaming mode as the data in raw gets appended everytime when new dataset is landed

    Autoloader documentation:
    https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/dlt#auto-loader-syntax-for-dlt

    :param transform: simple transform function as input for landing view
    :param partitions: partitions column
    :param ge_rules: great expectation rule
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param dataset_name: name of the dataset which to be loaded
    :param input_schema: Structype schema
    :param input_format: Input format "csv" or "json"
    :param ge_rules: list of expectations for the given dataset
    """
    catalog = base_config.get_uc_catalog_name()
    uc_raw_schema = base_config.get_uc_raw_schema()

    dirlst = base_config.get_dataset_config(dataset_name,confidentiality)
    (bw_landing_view_name, bw_landing_path) = dirlst['bw_landing_view_name'], dirlst['bw_landing_data_path']
    (raw_table_name, raw_data_path) = dirlst['raw_table_name'], dirlst['raw_data_path']

    print("core/landing_raw.py bw_landing_path",bw_landing_path)
    print("core/landing_raw.py bw_landing_view_name",bw_landing_view_name)

    dlt_raw_batch(bw_landing_view_name, bw_landing_path, raw_data_path, raw_table_name,
                   {"eds.quality": "raw"}, partitions, input_format,
                   input_schema, ge_rules, transform, base_config)

def landing_raw_aecorsoft_pooltable(
        spn: AzureSPN,
        base_config: BaseConfig,
        dataset_name: str,
        input_schema: str,
        transform: object,
        input_format: str = "json",
        partitions: Optional[list] = None,
        ge_rules: Optional[list] = None,
        confidentiality: Optional[list] = None,
        raw_source_name: Optional[list] = None):
    """
    This function takes the data from landing using autoloader creates a DLT View,
    then reads the data from the DLT view and loads the data into raw schema
    Whole approach is done in Streaming mode as the data in raw gets appended everytime when new dataset is landed

    Autoloader documentation:
    https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/dlt#auto-loader-syntax-for-dlt

    :param transform: simple transform function as input for landing view
    :param partitions: partitions column
    :param ge_rules: great expectation rule
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param dataset_name: name of the dataset which to be loaded
    :param input_schema: Structype schema
    :param input_format: Input format "csv" or "json"
    :param ge_rules: list of expectations for the given dataset
    """
    catalog = base_config.get_uc_catalog_name()
    uc_raw_schema = base_config.get_uc_raw_schema()

    dirlst = base_config.get_dataset_config(dataset_name,confidentiality)
    (aecorsoft_landing_view_name, aecorsoft_landing_path) = dirlst['aecorsoft_landing_view_name'], dirlst['aecorsoft_landing_data_path'].replace('DELTA','INIT')
    
    (raw_table_name, raw_data_path) = raw_source_name+'_'+dirlst['raw_table_name'].replace('raw_',''), dirlst['aecorsoft_raw_data_path']
    (archive_table_name, archive_table_path) = dirlst['archive_raw_table_name'], dirlst['archive_raw_data_path']
    
    print("core/landing_raw.py landing_raw_aecorsoft() aecorsoft_landing_path",aecorsoft_landing_path)
    print("core/landing_raw.py landing_raw_aecorsoft() aecorsoft_landing_view_name",aecorsoft_landing_view_name)
    print("core/landing_raw.py landing_raw_aecorsoft() dataset_name",dataset_name)
    print("core/landing_raw.py landing_raw_aecorsoft() raw_table_name",raw_table_name)
    print("core/landing_raw.py landing_raw_aecorsoft() raw_data_path",raw_data_path)

    dlt_raw_stream(aecorsoft_landing_view_name, aecorsoft_landing_path, raw_data_path, raw_table_name,
                   {"eds.quality": "raw"}, partitions, input_format,
                   input_schema, ge_rules, transform, base_config, archive_table_name, archive_table_path)

def landing_raw_batch(
    spn: AzureSPN,
    base_config: BaseConfig,
    dataset_name: str,
    input_schema: str,
    transform: object,
    input_format: str = "json",
    partitions: Optional[list] = None,
    ge_rules: Optional[list] = None,
    confidentiality: Optional[list] = None):

    """
    This function takes the data from landing using autoloader creates a DLT View,
    then reads the data from the DLT view and loads the data into raw schema
    Whole approach is done in batch mode as the data in raw gets replaced everytime when new dataset is landed

    Autoloader documentation:
    https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/dlt#auto-loader-syntax-for-dlt

    :param partitions: partition column name
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param dataset_name: name of the dataset which to be loaded
    :param data_format: Input format "csv" or "json"
    """
    catalog = base_config.get_uc_catalog_name()
    uc_raw_schema = base_config.get_uc_raw_schema()

    dirlst = base_config.get_dataset_config(dataset_name,confidentiality)
    (landing_view_name, landing_data_path) = dirlst['landing_view_name'], dirlst['landing_data_path']
    (raw_table_name, raw_data_path) = dirlst['raw_table_name'], dirlst['raw_data_path']

    print("core/landing_raw.py landing_data_path",landing_data_path)
    print("core/landing_raw.py landing_view_name",landing_view_name)
    dlt_raw_batch(landing_view_name, landing_data_path, raw_data_path, raw_table_name,
                   {"eds.quality": "raw"}, partitions, input_format,
                   input_schema, ge_rules, transform, base_config)

    print("Inside core/landing_raw.py batch function")