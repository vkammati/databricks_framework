"""
DS1HM_MT_BB Transform RAW to EUH
"""

from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, regexp_replace, col, from_unixtime, split, unix_timestamp, trim
from datta_pipeline_library.core.get_sequence_column import get_sequence_column
from datta_pipeline_library.core.spark_init import spark

DS1HM_MT_BB_key=["MANDT","RYEAR","RBUKRS","RLDNR","DOCCT","DOCNR","DOCLN"]
delta_load_source = spark.conf.get("pipeline.delta_load_source")
DS1HM_MT_BB_sequence_by = get_sequence_column(delta_load_source)
DS1HM_MT_BB_scd_type = 1

def DS1HM_MT_BB(df):
    """
    Cost Management Transform
    :param df: Dataframe
    :return: Dataframe
    """
    df = df.withColumn("GL_SIRID",trim(col("GL_SIRID")))\
        .withColumn("DOCCT",trim(col("DOCCT")))\
        .withColumn("MANDT",trim(col("MANDT")))\
        .withColumn("RYEAR",trim(col("RYEAR")))\
        .withColumn("RBUKRS",trim(col("RBUKRS")))\
        .withColumn("RLDNR",trim(col("RLDNR")))\
        .withColumn("DOCNR",trim(col("DOCNR")))\
        .withColumn("DOCLN",trim(col("DOCLN")))

    print("Inside transformation/raw_euh/DS1HM_MT_BB.py",df)
    return (df)


