#GLIDXA_TSAP Transform RAW to EUH


from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, regexp_replace, col, from_unixtime, split, unix_timestamp, trim
from datta_pipeline_library.core.get_sequence_column import get_sequence_column
from datta_pipeline_library.core.spark_init import spark

GLIDXA_TSAP_key=['RCLNT', 'AWREF','AWTYP','AWORG','RLDNR','DOCCT','RYEAR','DOCNR']
delta_load_source = spark.conf.get("pipeline.delta_load_source")
GLIDXA_TSAP_sequence_by = get_sequence_column(delta_load_source)
GLIDXA_TSAP_scd_type = 1

def GLIDXA_TSAP(df):

    #Cost Management Transform
    #:param df: Dataframe
    #:return: Dataframe

    df = df.withColumn("RCLNT",trim(col("RCLNT"))).withColumn("AWREF",trim(col("AWREF"))).withColumn("AWTYP",trim(col("AWTYP"))).withColumn("AWORG",trim(col("AWORG"))).withColumn("RLDNR",trim(col("RLDNR"))).withColumn("DOCCT",trim(col("DOCCT"))).withColumn("RYEAR",trim(col("RYEAR"))).withColumn("DOCNR",trim(col("DOCNR")))
    
    print("Inside transformation/raw_euh/GLIDXA_TSAP.py",df)
    return (df)