#BSEG_TSAP Transform RAW to EUH


from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, regexp_replace, col, from_unixtime, split, unix_timestamp, trim
from datta_pipeline_library.core.get_sequence_column import get_sequence_column
from datta_pipeline_library.core.spark_init import spark

BSEG_TSAP_key=['MANDT', 'BUKRS','BELNR','GJAHR','BUZEI']
delta_load_source = spark.conf.get("pipeline.delta_load_source")
BSEG_TSAP_sequence_by = get_sequence_column(delta_load_source)
BSEG_TSAP_scd_type = 1

def BSEG_TSAP(df):

    #Cost Management Transform
    #:param df: Dataframe
    #:return: Dataframe

    df = df.withColumn("MANDT",trim(col("MANDT"))).withColumn("BUKRS",trim(col("BUKRS"))).withColumn("BELNR",trim(col("BELNR"))).withColumn("GJAHR",trim(col("GJAHR"))).withColumn("BUZEI",trim(col("BUZEI")))
    
    print("Inside transformation/raw_euh/BSEG_TSAP.py",df)
    return (df)