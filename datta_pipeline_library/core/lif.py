"""
Core DLT Module
"""
from datta_pipeline_library.core.spark_init import spark
from pyspark.sql.functions import lit, col, expr, coalesce, when, trim
from datta_pipeline_library.data_quality.gx.core import execute_validation
from datta_pipeline_library.core.get_sequence_column import get_delete_columnname
from datta_pipeline_library.core.utils import read_table, get_ingested_dt
from datta_pipeline_library.helpers.spn import AzureSPN
from datta_pipeline_library.core.base_config import BaseConfig
from datta_pipeline_library.core.transform_to_timestamp import transform_to_timestamp 
from pyspark.sql.types import StringType

def dlt_raw_stream(view_name, input_data_path, target_tbl_path, table_name, tbl_prop, partition,
                   input_format, input_schema, ge_rules, transform, base_config: BaseConfig, archive_table_name, archive_table_path):
    """
    
    This Function is used in  DATTA proeject - Ashok 13/09/2023

    Function reads the table using autoloader for the specified schema creates a DLT view
    which is used as source to load the data into raw layer, while reading the data into DLT view
    we add few operational columns `yrmono` which is derived from the
    input file path we take the appropriate year and month value

    :rtype: None
    :param transform: transform function
    :param ge_rules: expectations rule
    :param view_name: Name of the landing view
    :param input_data_path: input data path
    :param target_tbl_path: output data path
    :param table_name: output table name
    :param tbl_prop: table properties or tags
    :param partition: column on which the table will be partitioned on
    :param input_format: input for the autoloader to read like `json` or `csv` etc
    :param input_schema: schema of the input data to be read
    :param base_config: BaseConfig object
    :return: None
    """
    import dlt

    @dlt.create_view(
        name=view_name
    )
    def landing_view():
        """
        Autoloader using incremental listing
        https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/directory-listing-mode
        :return: Dataframe
        """
        input_df = (spark
                    .readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", input_format)
                    .option("cloudFiles.useIncrementalListing", "auto")
                    .schema(input_schema)
                    .load(input_data_path))
        print("input_df:",input_df)

        # soft delete changes
        full_load_source = spark.conf.get("pipeline.full_load_source")
        delta_load_source = spark.conf.get("pipeline.delta_load_source")
        data_load_type = spark.conf.get("pipeline.load_type")

        if (data_load_type == 'INIT' and full_load_source == 'AECORSOFT'):
            input_df = input_df.withColumn("OPFLAG", when(col("OPFLAG").isNull() | (trim(col("OPFLAG")) == ""), lit("I")).otherwise(col("OPFLAG")))
        
        if (data_load_type == 'INIT' and full_load_source == 'HANA'):
            input_df = input_df.withColumn("LAST_ACTION_CD", when(col("LAST_ACTION_CD").isNull() | (trim(col("LAST_ACTION_CD")) == ""), lit("I")).otherwise(col("LAST_ACTION_CD")))
        
        if (load_type == 'DELTA' and delta_load_source == 'AECORSOFT'):
            input_df = input_df.withColumn("OPFLAG", when((col("OPFLAG").isNull() )| (trim(col("OPFLAG")) == ""), lit("I")).otherwise(col("OPFLAG")))

        # based on the input format is parquet, modifying the logic
        return (transform(input_df)
                .withColumn("ingested_at", lit(get_ingested_dt()))
                
                )


    # Added for storing archived records in separate table - Ashok
    delta_load_source = spark.conf.get("pipeline.delta_load_source")
    load_type = spark.conf.get("pipeline.load_type")
    
    if (load_type == 'DELTA' and delta_load_source == 'AECORSOFT'):
        #archive_table_name = f"{table_name}"+"_archive"
        #archive_target_tbl_path = f"{target_tbl_path}"+"_archive"
        print("Ashok archive table name :: " ,archive_table_name)
        print("Ashok archive table path :: " ,archive_table_path)
        @dlt.table(
            name=archive_table_name,
            table_properties=tbl_prop,  # Needs to define the name of the key
            partition_cols=partition,
            path=archive_table_path,
        )
        def archive_data():
            #return dlt.readStream(view_name).where("opflag='A'")            
            if(table_name in ('Aecorsoft_DS1HM_MT_BB01','Aecorsoft_DS1HM_MT_BB')):
                return dlt.readStream(view_name).where("opflag ='D'")
            else:
                return dlt.readStream(view_name).where("opflag ='A'")
            
            #return spark.table(view_name).where("opflag='A'")
        
   
    # end of storing archived records modification 

    @dlt.table(
        name=table_name,
        table_properties=tbl_prop,  # Needs to define the name of the key
        partition_cols=partition,
        path=target_tbl_path,
    )
    def raw_table():
        """
        :return: Dataframe
        """
        return dlt.readStream(view_name)

    if ge_rules is not None:
        @dlt.create_view(
            name=f"ge_{view_name}"
        )
        def exception_view():
            """

            :return: Dataframe
            """
            success_percent_threshold = 100
            dataset_name = table_name.replace("raw_", "")
            # [TODO] add sampling as a parameter
            df = read_table(table_name).sample(0.05)
            execute_validation(df, dataset_name, ge_rules, success_percent_threshold, base_config)
            return df


def dlt_raw_batch(view_name, input_data_path, target_tbl_path, table_name, tbl_prop, partition,
                   input_format, input_schema, ge_rules, transform, base_config: BaseConfig):
    """
    This Function is used in  DATTA proeject for loading always full load tables, Currently these tables are like control tables like BW tables, DATE, TCURR_PATTERN, TIME_PATTERN etc. No tables from Aecorsoft Source - Ashok 13/09/2023

    Function table landing path as input and moves data towards raw layer
    :param partition: Partition column name
    :param view_name: landing view name
    :param input_data_path: landing data path
    :param target_tbl_path: target data path
    :param table_name: target table name
    :param tbl_prop: table tags
    :param input_format: input format for autoloader to read like `json` or `csv`
    :return: None
    """
    import dlt
    @dlt.create_view(
        name=view_name
    )
    def landing_view():
        """

        :return: Dataframe
        """
        return (spark
                .read
                .format(input_format)
                .option("header", "True")
                .load(input_data_path)
                .withColumn("ingested_at", lit(get_ingested_dt()))
                )

    @dlt.table(
        name=table_name,
        table_properties=tbl_prop,  # Needs to define the name of the key
        partition_cols=partition,
        path=target_tbl_path,
    )
    def raw_table():
        """

        :return: Dataframe
        """
        return (dlt
                .read(view_name))


def dlt_stream(src_table_name, tgt_table_name, data_path, func_transform, partitions, tags, schema_expr):
    """
    This Function is not used in  DATTA proeject - Ashok 13/09/2023 
    
    Function takes src dlt table name as input and applies transformation
    and saves the data in target as dlt table as stream operation
    :param src_table_name: Name of the Source DLT Table
    :param tgt_table_name: Name of the Target DLT Table
    :param data_path: Path for the Target DLT Table
    :param func_transform: Transformation Function
    :param partitions: Partition column information as List
    :param tags: Table Tags to be stored as Properties
    :param schema_expr: select column list
    :return: DataFrame
    """
    import dlt
    @dlt.create_table(
        name=tgt_table_name,
        comment="The cleaned dna stats subscription's usage, partitioned by processDate",
        table_properties=tags,
        partition_cols=partitions,
        path=data_path
    )
    def dna_stream_load():
        """

        :return: Dataframe
        """
        df = dlt.readStream(src_table_name)
        if schema_expr is not None:
            df = df.selectExpr(schema_expr)

        df = df.transform(func_transform)
        print("df:",df)

        return (df.withColumn("ingested_at", lit(get_ingested_dt()))
                .withColumn("AEDATTM", lit(None).cast("timestamp"))
                .withColumn("OPFLAG", lit(None).cast(StringType())))

def dlt_stream_delta(src_view_name, src_table_name, tgt_table_name, data_path, func_transform, partitions, tags, schema_expr, key, sequence_by, scd_type):
    """
    This Function is used in  DATTA proeject - Ashok 13/09/2023
    Function takes src dlt table name as input and applies transformation
    and saves the data in target as dlt table as stream operation
    :param src_table_name: Name of the Source DLT Table
    :param tgt_table_name: Name of the Target DLT Table
    :param data_path: Path for the Target DLT Table
    :param func_transform: Transformation Function
    :param partitions: Partition column information as List
    :param tags: Table Tags to be stored as Properties
    :param schema_expr: select column list
    :return: DataFrame
    """
   
    import dlt
    '''@dlt.create_table(
        name=tgt_table_name,
        comment="The cleaned dna stats subscription's usage, partitioned by processDate",
        table_properties=tags,
        partition_cols=partitions,
        path=data_path
    )'''
    full_load_source = spark.conf.get("pipeline.full_load_source")
    delta_load_source = spark.conf.get("pipeline.delta_load_source")
    load_type = spark.conf.get("pipeline.load_type")
    delete_columnname = get_delete_columnname(load_type, full_load_source, delta_load_source)

    @dlt.create_view(
        name=src_view_name
        )
    def dna_stream_load():
        """
        :return: Dataframe
        """
        df = dlt.readStream(src_table_name)
        if schema_expr is not None:
            df = df.selectExpr(schema_expr)

        df = df.transform(func_transform)
        df= transform_to_timestamp(df)

        print("df:",df)

        if ((load_type == 'INIT' and full_load_source == 'HANA') or (load_type == 'DELTA' and 
         delta_load_source == 'HANA')): 
            return (df.withColumn("ingested_at", lit(get_ingested_dt()))
                .withColumn("AEDATTM", lit(None).cast("timestamp"))
                .withColumn("OPFLAG", lit(None).cast(StringType())))
        elif((load_type == 'INIT' and full_load_source == 'AECORSOFT') or (load_type == 'DELTA' and delta_load_source == 'AECORSOFT')):
            if(src_table_name in ('Aecorsoft_DS1HM_MT_BB01','Aecorsoft_DS1HM_MT_BB')):
                df = df.filter(coalesce(df.OPFLAG,lit('I')) != 'D')
            else:
                df = df.filter(coalesce(df.OPFLAG,lit('I')) != 'A') # added this to Ignore Archive records  23/01/2024
            return df.withColumn("ingested_at", lit(get_ingested_dt()))

    
    dlt_apply_changes(src_view_name, tgt_table_name, data_path, key, scd_type, tags, partitions, sequence_by, delete_columnname)
    print("Euh dlt stream delta process completed successfully.")

def dlt_batch(src_table_name, tgt_table_name, data_path, func_transform, partitions, tags):
    """
    
    This Function is used in  DATTA proeject for loading always full load tables, Currently these tables are like control tables like BW tables, DATE, TCURR_PATTERN, TIME_PATTERN etc. No tables from Aecorsoft Source - Ashok 13/09/2023

    Function takes src dlt table name as input and applies transformation
    and saves the data in target as dlt table as batch operation
    :param src_table_name: Name of the Source DLT Table
    :param tgt_table_name: Name of the Target DLT Table
    :param data_path: Path for the Target DLT Table
    :param func_transform: Transformation Function
    :param partitions: Partition column information as List
    :param tags: Table Tags to be stored as Properties
    :return: DataFrame"""
    import dlt
    @dlt.create_table(
        name=tgt_table_name,
        comment="The cleaned dna stats subscription's usage, partitioned by processDate",
        table_properties=tags,
        partition_cols=partitions,
        path=data_path
    )
    def dna_batch_load():
        """

        :return: dataframe
        """
        print("core/lif.py dlt_batch fucntion src_table_name:::",src_table_name)
        print("core/lif.py dlt_batch fucntion func_transform:::",func_transform)
        return (dlt.read(src_table_name)
                .transform(func_transform)
                .withColumn("ingested_at", lit(get_ingested_dt()))
                .withColumn("AEDATTM", lit(None).cast("timestamp"))
                .withColumn("OPFLAG", lit(None).cast(StringType()))
                )

def dlt_transform(tbl_transform_map, func_transform, key_prefix, tgt_tbl_name, tgt_data_path, partition, tags):
    """
    Function takes table and transformation function reads those DLT tables from source
    and applies corresponding transformation and uses `func_transform` to combine them into final DLT target table.
    #[TODO] Add an exmaple to explain this function

    :param tbl_transform_map: dictionary where the keys are table names and
                values are transformation functions to be applied to the table
    :param func_transform: final transform function combining the various tables and writing final output
    :param key_prefix: prefix of the table name's DLT Technical field
    :param tgt_tbl_name: Target Table Name
    :param tgt_data_path: Target Table Path
    :param partition: Partition column information as List
    :param tags: Table Tags to be stored as Properties
    :return: DataFrame
    """
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") 
    import dlt

    @dlt.create_table(
        name=tgt_tbl_name,
        comment="The cleaned dna stats subscription's usage, partitioned by processDate",
        table_properties=tags,
        partition_cols=partition,
        path=tgt_data_path
    )
    def dna_combine_load():
        """
        :return: Dataframe
        """
        # Reading From Source Dataframe Dict EUH
        transform_dict = {}
        for table_name, transform_int in tbl_transform_map.items():
            print("core/lif.py dlt_transform function table_name::",table_name)
            print("core/lif.py dlt_transform function transform_int::",transform_int)
            table = dlt.read(f"{key_prefix}_{table_name}")
            print("core/lif.py dlt_transform function table::", table)
            transform_dict[table_name] = transform_int(table)

        print("core/lif.py dlt_transform fucntion transform_dict values:::",transform_dict)
        print("core/lif.py dlt_transform fucntion func_transform values:::",func_transform)

        return func_transform(transform_dict).withColumn("ingested_at", lit(get_ingested_dt()))

    
def dlt_transform_stream(tbl_transform_map, func_transform, key_prefix, tgt_tbl_name, tgt_data_path, partition, tags):
    """
    Function takes table and transformation function reads those DLT tables from source
    and applies corresponding transformation and uses `func_transform` to combine them into final DLT target table.
    #[TODO] Add an exmaple to explain this function

    :param tbl_transform_map: dictionary where the keys are table names and
                values are transformation functions to be applied to the table
    :param func_transform: final transform function combining the various tables and writing final output
    :param key_prefix: prefix of the table name's DLT Technical field
    :param tgt_tbl_name: Target Table Name
    :param tgt_data_path: Target Table Path
    :param partition: Partition column information as List
    :param tags: Table Tags to be stored as Properties
    :return: DataFrame
    """
    import dlt

    @dlt.create_table(
        name=tgt_tbl_name,
        comment="The cleaned dna stats subscription's usage, partitioned by processDate",
        table_properties=tags,
        partition_cols=partition,
        path=tgt_data_path
    )
    def dna_combine_load():
        """
        :return: Dataframe
        """
        # Reading From Source Dataframe Dict EUH
        transform_dict = {}
        for table_name, transform_int in tbl_transform_map.items():
            print("core/lif.py dlt_transform function table_name::",table_name)
            print("core/lif.py dlt_transform function transform_int::",transform_int)
            table = dlt.readStream(f"{key_prefix}_{table_name}")
            print("core/lif.py dlt_transform function table::", table)
            transform_dict[table_name] = transform_int(table)
        print("core/lif.py dlt_transform fucntion transform_dict values:::",transform_dict)
        print("core/lif.py dlt_transform fucntion func_transform values:::",func_transform)

        return func_transform(transform_dict).withColumn("ingested_at", lit(get_ingested_dt()))
    

def dlt_apply_changes(src_tbl_name, tgt_tbl_name, tgt_data_path, keys, scd_type, tags, partition, sequence_by, delete_columnname):
    """
    DLT Streaming Table
    :param src_tbl_name: Source Table name
    :param tgt_tbl_name: Target table name
    :param tgt_data_path: Target Data path
    :param keys: primary key against which SCD needs to be implemented
    :param scd_type: Type 1 or Type 2 SCD needs to be maintained
    :param tags: tags used while creating table
    :param partition: partition column if applicable.
    """
    import dlt
    del_expr = f"{delete_columnname}"+"='D'"
    print("del_expr : ", del_expr)
    print("sequence_by : ",sequence_by)
    print("tgt_tbl_name :", tgt_tbl_name)
    dlt.create_streaming_live_table(tgt_tbl_name,
                                    comment=None,
                                    table_properties=tags,
                                    partition_cols=partition,
                                    path=tgt_data_path)

    dlt.apply_changes(
        target=tgt_tbl_name,
        source=src_tbl_name,
        keys=keys,
        sequence_by=sequence_by,
        ignore_null_updates = False,
        #apply_as_deletes=expr(del_expr),
        stored_as_scd_type=scd_type
    )
