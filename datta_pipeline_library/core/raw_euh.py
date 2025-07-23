"""
RAW to EUH Module
"""
from datta_pipeline_library.core.lif import dlt_stream, dlt_batch, dlt_apply_changes, dlt_stream_delta
import datetime

default_tag = {"eds.quality": "euh"}


def raw_euh(spn, base_config, func_transform, dataset_name, partitions=None, tags=None, schema_expr=None):
    """
    functions will take following parameters and create dlt table and call the register unity catalog function
    this function read the new incoming data from source in raw and appends the data into target table in euh

    :param schema_expr: schema or column expression used to transform column names and skip columns
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param func_transform: function which takes DataFrame as input and returns DataFrame as output
    :param dataset_name: name of the dataset which to be loaded
    :param partitions: column list in which table will be partitioned
    :param tags: collibra metadata tags
    :return: None
    """
    catalog = base_config.get_uc_catalog_name()
    euh_db_schema = base_config.get_uc_euh_schema()

    dirlst = base_config.get_dataset_config(dataset_name)
    (raw_table_name, raw_data_path) = dirlst['raw_table_name'], dirlst['raw_data_path']
    (euh_table_name, euh_data_path) = dirlst['euh_table_name'], dirlst['euh_data_path']

    if partitions is None:
        partitions = []

    if tags is None or tags == "":
        tags = default_tag

    dlt_stream(raw_table_name, euh_table_name, euh_data_path, func_transform, partitions, tags, schema_expr)

def raw_euh_aecorsoft_delta(spn, base_config, func_transform, dataset_name, key, sequence_by, scd_type, partitions=None, tags=None, schema_expr=None):
    """
    Thi function is used to Merge Aecorsoft delta data with HANA records
    functions will take following parameters and create dlt table and call the register unity catalog function
    this function read the new incoming data from source in raw and appends the data into target table in euh

    :param schema_expr: schema or column expression used to transform column names and skip columns
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param func_transform: function which takes DataFrame as input and returns DataFrame as output
    :param dataset_name: name of the dataset which to be loaded
    :param key: primary key columns of table
    :param sequence_by: column that has date time for a row
    :param partitions: column list in which table will be partitioned
    :param tags: collibra metadata tags
    :return: None
    """
    catalog = base_config.get_uc_catalog_name()
    euh_db_schema = base_config.get_uc_euh_schema()

    dirlst = base_config.get_dataset_config(dataset_name)
    (raw_table_name, raw_data_path) = dirlst['raw_table_name'], dirlst['aecorsoft_raw_data_path']
    (euh_table_name, euh_data_path) = dirlst['euh_table_name'], dirlst['euh_data_path']
    
    raw_table_name=raw_table_name.replace("raw_", "Aecorsoft_")

    if partitions is None:
        partitions = []

    if tags is None or tags == "":
        tags = default_tag
        assign_value = False
    else:
        assign_value = True

    view_name = str(raw_table_name) +str(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    print("core/raw_euh.py view_name",view_name)
    print("core/raw_euh.py raw_table_name",raw_table_name)
    print("core/raw_euh.py euh_table_name",euh_table_name)
    print("core/raw_euh.py dataset_name",dataset_name)
    print("core/raw_euh.py euh_data_path",euh_data_path)
    print("core/raw_euh.py raw_data_path",raw_data_path)
    print("core/raw_euh.py func_transform",func_transform)
    print("core/raw_euh.py schema_expr",schema_expr)
    print("core/raw_euh.py assign_value",assign_value)
    print("core/raw_euh.py tags",tags)

    dlt_stream_delta(view_name, raw_table_name, euh_table_name, euh_data_path, func_transform, partitions, tags, schema_expr, key,sequence_by, scd_type)

def raw_euh_merge(spn, base_config, func_transform, dataset_name, key, sequence_key, scd_type, partitions=None, tags=None, schema_expr=None):
    """
    functions will take following parameters and create dlt table 
    this function read the new incoming data from source in raw and merges the data into target table in euh

    :param schema_expr: schema or column expression used to transform column names and skip columns
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param func_transform: function which takes DataFrame as input and returns DataFrame as output
    :param dataset_name: name of the dataset which to be loaded
    :param partitions: column list in which table will be partitioned
    :param tags: collibra metadata tags
    :return: None
    """
    catalog = base_config.get_uc_catalog_name()
    euh_db_schema = base_config.get_uc_euh_schema()

    dirlst = base_config.get_dataset_config(dataset_name)
    (raw_table_name, raw_data_path) = dirlst['raw_table_name'], dirlst['raw_data_path']
    (euh_table_name, euh_data_path) = dirlst['euh_table_name'], dirlst['euh_data_path']

    if partitions is None:
        partitions = []

    if tags is None or tags == "":
        tags = default_tag
    view_name = str(raw_table_name) +str(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    dlt_stream_delta(view_name, raw_table_name, euh_table_name, euh_data_path, func_transform, partitions, tags, schema_expr, key, sequence_key, scd_type)


def raw_euh_batch(spn, base_config, func_transform, dataset_name, partitions=None, tags=None):
    """
    functions will take following parameters and create dlt table and call the register unity catalog function
    this function read the new incoming data from source in raw and replaces the data into target table in euh

    :param spn: Azure SPN used to call the Databricks REST API
    :param func_transform: function which takes DataFrame as input and returns DataFrame as output
    :param dataset_name: name of the dataset which to be loaded
    :param base_config: BaseConfig object
    :param partitions: column list in which table will be partitioned
    :param tags: EDC table and column tags
    :return: None
    """
    catalog = base_config.get_uc_catalog_name()
    euh_db_schema = base_config.get_uc_euh_schema()

    dirlst = base_config.get_dataset_config(dataset_name)
    (raw_table_name, raw_data_path) = dirlst['raw_table_name'], dirlst['raw_data_path']
    (euh_table_name, euh_data_path) = dirlst['euh_table_name'], dirlst['euh_data_path']

    if partitions is None:
        partitions = []

    if tags is None or tags == "":
        tags = default_tag

    dlt_batch(raw_table_name, euh_table_name, euh_data_path, func_transform, partitions, tags)

def raw_euh_scd(spn, base_config, dataset_name, scd_key, scd_type, sequence_by=None, delete_columnname=None, partitions=None, tags=None):
    """

    @param spn: Azure SPN used to call the Databricks REST API
    @param base_config: BaseConfig object
    @param dataset_name: name of the dataset which to be loaded
    @param scd_key: primary keys for capturing SCD
    @param scd_type: Type 1 or Type 2 SCD
    @param partitions: partition column for table
    @param tags: any custom tags to be used as table properties
    """
    catalog = base_config.get_uc_catalog_name()
    euh_db_schema = base_config.get_uc_euh_schema()

    dirlst = base_config.get_dataset_config(dataset_name)
    (raw_table_name, raw_data_path) = dirlst['raw_table_name'], dirlst['raw_data_path']
    (euh_table_name, euh_data_path) = dirlst['euh_table_name'], dirlst['euh_data_path']

    if partitions is None:
        partitions = []

    if tags is None or tags == "":
        tags = default_tag

    dlt_apply_changes(raw_table_name, euh_table_name, euh_data_path, scd_key, scd_type, sequence_by, delete_columnname, tags, partitions)
