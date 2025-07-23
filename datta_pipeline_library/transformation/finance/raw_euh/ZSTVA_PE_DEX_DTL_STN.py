#ZSTVA_PE_DEX_DTL_STN Transform RAW to EUH


from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, regexp_replace, col, from_unixtime, split, unix_timestamp, trim
from datta_pipeline_library.core.get_sequence_column import get_sequence_column
from datta_pipeline_library.core.spark_init import spark

ZSTVA_PE_DEX_DTL_STN_key=['MANDT', 'DTLNUM']
delta_load_source = spark.conf.get("pipeline.delta_load_source")
ZSTVA_PE_DEX_DTL_STN_sequence_by = get_sequence_column(delta_load_source)
ZSTVA_PE_DEX_DTL_STN_scd_type = 1

def ZSTVA_PE_DEX_DTL_STN(df):

    #Cost Management Transform
    #:param df: Dataframe
    #:return: Dataframe

    df = df.withColumn("MANDT",trim(col("MANDT"))).withColumn("DTLNUM",trim(col("DTLNUM")))
    
    print("Inside transformation/raw_euh/ZSTVA_PE_DEX_DTL_STN.py",df)
    return (df)