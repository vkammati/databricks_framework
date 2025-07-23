#BKPF_STN Transform RAW to EUH


from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, regexp_replace, col, from_unixtime, split, unix_timestamp, trim
from datta_pipeline_library.core.get_sequence_column import get_sequence_column
from datta_pipeline_library.core.spark_init import spark

BKPF_STN_key=['MANDT', 'BUKRS','BELNR','GJAHR']
delta_load_source = spark.conf.get("pipeline.delta_load_source")
BKPF_STN_sequence_by = get_sequence_column(delta_load_source)
BKPF_STN_scd_type = 1

def BKPF_STN(df):

    #Cost Management Transform
    #:param df: Dataframe
    #:return: Dataframe

    df = df.withColumn("MANDT",trim(col("MANDT"))).withColumn("BUKRS",trim(col("BUKRS"))).withColumn("BELNR",trim(col("BELNR"))).withColumn("GJAHR",trim(col("GJAHR")))
    
    print("Inside transformation/raw_euh/BKPF_STN.py",df)
    return (df)