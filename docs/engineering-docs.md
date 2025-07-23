### `base_config(dataset_name: str) -> dict`

Function takes input dataset name and spits out python dictionary,
containing of path and table names for each layer

`dataset_name`: input dataset name

`return`: dictionary with all path necessary path and table name configurations



### `eh_curated_static(transform, source_dataset_name, target_dataset_name, partition=None, tags=None)`


Function takes following inputs and reads the data from eh layer
and writes output to curated layer in batch by applying specific transformation

`transform`: transform function takes DataFrame and returns DataFrame

`source_dataset_name`: dictionary table name and transform function

`target_dataset_name`: dataset or table name to save in the target

`partition`: column list in which table will be partitioned

`tags`: EDC table and column tags

`return`: None

### `eh_curated(tbl_transform_map, transform, target_dataset_name, partition=None, tags=None)`

Function reads data from every table in the dictionary key and applies specified transformation function.
This helps in creating a curated layer by combining multiple tables from eh applying transformation
and then combining all of them and load the data all done in batch mode as full overwrite mode.

`tbl_transform_map`: dictionary where the keys are table names and
values are transformation functions to be applied to the table

`transform`: final transform function combining the various tables and writing final output

`target_dataset_name`: dataset or table name to save in the target

`partition`: column list in which table will be partitioned

`tags`: EDC table and column tags

`return`: None

### `euh_eh_static(transform, target_dataset_name, partition=None, tags=None)`

Function takes following inputs and reads the data from euh layer
and writes output to eh layer in batch by applying specific transformation
 
`transform`: transform function takes DataFrame and returns DataFrame

`target_dataset_name`: dataset or table name to save in the target

`partition`: column list in which table will be partitioned

`tags`: EDC table and column tags

`return`: None

### `euh_eh(tbl_transform_map, transform, target_dataset_name, partition=None, tags=None)`

Function reads data from every table in the dictionary key and applies specified transformation function.
This helps in creating eh layer by combining multiple tables from eh applying transformation
and then combining all of them and load the data all done in batch mode as full overwrite mode.

`tbl_transform_map`: dictionary where the keys are table names and
values are transformation functions to be applied to the table

`transform`: final transform function combining the various tables and writing final output

`target_dataset_name`: dataset or table name to save in the target

`partition`: column list in which table will be partitioned

`tags`: EDC table and column tags

`return`: None

### `landing_raw_batch(dataset_name, data_format='csv')`

This function takes the data from landing using autoloader creates a DLT View,
then reads the data from the DLT view and loads the data into raw schema
Whole approach is done in batch mode as the data in raw gets replaced everytime when new dataset is landed

Autoloader documentation: https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/dlt#auto-loader-syntax-for-dlt

`dataset_name`: name of the dataset which to be loaded

`data_format`: Input format "csv" or "json"

`return`: None

### `landing_raw(dataset_name, input_schema, input_format="json")`

This function takes the data from landing using autoloader creates a DLT View,
then reads the data from the DLT view and loads the data into raw schema
Whole approach is done in Streaming mode as the data in raw gets appended everytime when new dataset is landed

Autoloader documentation: https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/dlt#auto-loader-syntax-for-dlt

`dataset_name`: name of the dataset which to be loaded
`input_schema`: Structype
`input_format`: Input format "csv" or "json"
`return`: None

### `dlt_batch(src_table_name, tgt_table_name, data_path, func_transform, partitions, tags)`

Function takes src dlt table name as input and applies transformation
and saves the data in target as dlt table as batch operation

`src_table_name`: Name of the Source DLT Table

`tgt_table_name`: Name of the Target DLT Table

`data_path`: Path for the Target DLT Table

`func_transform`: Transformation Function

`partitions`: Partition column information as List

`tags`: Table Tags to be stored as Properties

### `dlt_raw_batch(view_name, input_data_path, target_tbl_path, table_name, tbl_prop, input_format)`

Function table landing path as input and moves data towards raw layer
`view_name`: landing view name

`input_data_path`: landing data path

`target_tbl_path`: target data path

`table_name`: target table name

`tbl_prop`: table tags

`input_format`: input format for autoloader to read like `json` or `csv`

`return`: DataFrame

### `dlt_raw_stream(view_name, input_data_path, target_tbl_path, table_name, tbl_prop, partition, input_format`


### `dlt_stream(src_table_name, tgt_table_name, data_path, func_transform, partitions, tags)`

Function takes src dlt table name as input and applies transformation
and saves the data in target as dlt table as stream operation

`src_table_name`: Name of the Source DLT Table

`tgt_table_name`: Name of the Target DLT Table

`data_path`: Path for the Target DLT Table

`func_transform`: Transformation Function

`partitions`: Partition column information as List

`tags`: Table Tags to be stored as Properties

`return`: DataFrame

### `dlt_transform(tbl_transform_map, func_transform, key_prefix, tgt_tbl_name, tgt_data_path, partition, tags)`

Function takes table and transformation function reads those DLT tables from source
and applies corresponding transformation and uses `func_transform` to combine them into final DLT target table.

`tbl_transform_map`: dictionary where the keys are table names and
values are transformation functions to be applied to the table

`func_transform`: final transform function combining the various tables and writing final output

`key_prefix`: prefix of the table name's DLT Technical field

`tgt_tbl_name`: Target Table Name

`tgt_data_path`: Target Table Path

`partition`: Partition column information as List

`tags`: Table Tags to be stored as Properties

`return`: DataFrame

### `raw_euh_batch(func_transform, dataset_name, partitions=None, tags=None)`

functions will take following parameters and create dlt table and call the register unity catalog function
this function read the new incoming data from source in raw and replaces the data into target table in euh

`func_transform`: function which takes DataFrame as input and returns DataFrame as output

`dataset_name`: name of the dataset which to be loaded

`partitions`: column list in which table will be partitioned

`tags`: EDC table and column tags

`return`: None

### `raw_euh(func_transform, dataset_name, partitions=None, tags=None)`

functions will take following parameters and create dlt table and call the register unity catalog function
this function read the new incoming data from source in raw and appends the data into target table in euh

`func_transform`: function which takes DataFrame as input and returns DataFrame as output

`dataset_name`: name of the dataset which to be loaded

`partitions`: column list in which table will be partitioned

`tags`: collibra

`return`: None

### `read_static_dlt(tbl: str) -> DataFrame`

Takes DLT table name as input and returns a DLT time
`tbl`: Name of the Table
`return`: DLT Table


### `register_table(schema, table_name, path, tags_str="{'eds.dataQuality':'raw'}", assign_tags="False")`

This functions takes following inputs and triggers a databricks job to register the table created by the DLT pipeline,
in Unity Catalog and assigns appropriate permission as required.

`schema`: Database or Schema Name where the table has to be created

`table_name`: Name of the Table to be created

`path`: Location or Path where the table is stored

`tags_str`: optional TagString which gets passed along from Enterprise Data Catalog

`assign_tags`: Flag defaulted to False which allows you to re-apply the tags if necessary

`return`: run_id: run id of the job

`rtype`: Long or Bigint


### `transform_basic(df: DataFrame) -> DataFrame`

Function Takes in a dataframe and returns back the same Spark dataframe,
this is wrapper function mostly used for static/lookup files
where there is no transformation required between layers.

`df`: DataFrame

`return`: DataFrame

### `clmn_expr(clmn_name)`

Takes the column value removes special characters like `[]` or `{}`
and replaces `ColumnTagValue=` with `CV.`

`clmn_name`: Column Name

`return`: column expression

#### `concat_col(prefix, y)`

contacts each column value with target column by replacing the CV prefix.
For example `ColumnName` has value `state` which is prefix here
and `ColumnTags` has value  `CV.ConfidentialTechnicalInformation` after the udf runs
output: `state.ConfidentialTechnicalInformation` as target column value.

`prefix`: prefix string

`y`: input string

`return`: replaces Static prefix CV with prefix String

#### `convert(lst)`

Convert a list to a dictionary that maps elements at even indices to elements at the following odd indices
as key value pairs

`lst`:  A list of elements

`return`: dict: A dictionary that maps the elements at even indices of `lst` to the
elements at the following odd indices.

#### `create_schema(df)`

Dataframe containing column_type, nullability,alias_name,column_name to generate schema StructType
`df`: Dataframe

`return`: Dataframe

#### `create_table_tags(df)`

Function takes a DataFrame has input and combines various attributes as final Table Tags.

`df`: Input Dataframe which takes tag values and flatten them store in
corresponding columns with column name as prefix for the tags

`return`: DataFrame


#### `fetch_metadata_json(tbl_name, user, pwd)`

function takes table name as input and return a dictionary of enriched metadata from EDC
`tbl_name`: table name as input

`user`: Collibra User id

`pwd`: Collibra Password

`return`: dictionary of enriched metadata for the particular table ['ColumnName', 'ColumnDataConfidentiality',
'ColumnPersonalData', 'ColumnDataAggregationRisk', 'ConfidentialTechnicalInformation']


#### `flatten(l)`

Flatten a list of list into a single list.

`l`: A list of list
`return`: A Single List containing all the elements of the input lists

#### `get_tags(df)`

Get a dictionary of tags from a input dataframe

`df`: A DataFrame with `tags` column

`return`: A dictionary of tags as key value pair with tag name and tag value.

#### `retrieve_table(inter_dict, tbl_name)`

Takes an input dictionary and returns the ['TableName', 'TableDisplayName', 'TableContainsColumn']
keys for a particular input table name

`inter_dict`: List of dictonary for multiple table with keys
['LifDataProductName', 'LifDataProductBusinessName', 'LifDataProductType',
'LifDataProductDataConfidentiality', 'LifDataProductStorageLocation', 'LifDataProductContainsTable']

`tbl_name`: input table name to filter.

`return`: list of dictionary containing keys ['TableName', 'TableDisplayName', 'TableContainsColumn']