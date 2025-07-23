"""
DS1HM_MT_BB01 Transform RAW to EUH
"""

from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, regexp_replace, col, from_unixtime, split, unix_timestamp,to_date,coalesce,date_format,trim
from datta_pipeline_library.core.get_sequence_column import get_sequence_column
from datta_pipeline_library.core.spark_init import spark

DS1HM_MT_BB01_key=["MANDT","RYEAR","RBUKRS","RLDNR","DOCCT","DOCNR","DOCLN"]
delta_load_source = spark.conf.get("pipeline.delta_load_source")
DS1HM_MT_BB01_sequence_by = get_sequence_column(delta_load_source)
DS1HM_MT_BB01_scd_type = 1

def DS1HM_MT_BB01(df):
    """
    Cost Management Transform
    :param df: Dataframe
    :return: Dataframe
    """
    df=df.withColumn("DDATFRM", to_date(coalesce(date_format(to_date('DDATFRM', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("DDATFRM", "yyyy-MM-dd"))))\
        .withColumn("DDATTO", to_date(coalesce(date_format(to_date('DDATTO', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("DDATTO", "yyyy-MM-dd"))))\
        .withColumn("ZXBLDAT", to_date(coalesce(date_format(to_date('ZXBLDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("ZXBLDAT", "yyyy-MM-dd"))))

    df = df.withColumn("RLDNR",trim(col("RLDNR")))\
        .withColumn("RYEAR",trim(col("RYEAR")))\
        .withColumn("DOCNR",trim(col("DOCNR")))\
        .withColumn("DOCLN",trim(col("DOCLN")))\
        .withColumn("RBUKRS",trim(col("RBUKRS")))\
        .withColumn("MANDT",trim(col("MANDT")))\
        .withColumn("DOCCT",trim(col("DOCCT")))

    print("Inside transformation/raw_euh/DS1HM_MT_BB01.py",df)
    return (df)


