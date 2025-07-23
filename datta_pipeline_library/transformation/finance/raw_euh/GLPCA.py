"""
GLPCA Transform RAW to EUH
"""

from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, regexp_replace, col, from_unixtime, split, unix_timestamp,to_date, coalesce, date_format,to_timestamp,trim
from datta_pipeline_library.core.get_sequence_column import get_sequence_column
from datta_pipeline_library.core.spark_init import spark

GLPCA_key=["RCLNT", "GL_SIRID"]
delta_load_source = spark.conf.get("pipeline.delta_load_source")
GLPCA_sequence_by = get_sequence_column(delta_load_source)
GLPCA_scd_type = 1

def GLPCA(df):
    """
    Cost Management Transform
    :param df: Dataframe
    :return: Dataframe
    """
    df=df.withColumn("CPUDT", to_date(coalesce(date_format(to_date('CPUDT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("CPUDT", "yyyy-MM-dd"))))\
        .withColumn("BLDAT", to_date(coalesce(date_format(to_date('BLDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("BLDAT", "yyyy-MM-dd"))))\
        .withColumn("BUDAT", to_date(coalesce(date_format(to_date('BUDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("BUDAT", "yyyy-MM-dd"))))\
        .withColumn("WSDAT", to_date(coalesce(date_format(to_date('WSDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("WSDAT", "yyyy-MM-dd"))))\
        .withColumn("DABRZ", to_date(coalesce(date_format(to_date('DABRZ', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("DABRZ", "yyyy-MM-dd"))))\
        .withColumn("VALUT", to_date(coalesce(date_format(to_date('VALUT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("VALUT", "yyyy-MM-dd"))))
    df=df.withColumn("CPUTM",coalesce(date_format(to_timestamp(col('CPUTM'),'HHmmss'),'HH:mm:ss'),date_format(col('CPUTM'),'HH:mm:ss')))

    df = df.withColumn("GL_SIRID",trim(col("GL_SIRID")))\
        .withColumn("DOCCT",trim(col("DOCCT")))\
        .withColumn("RLDNR",trim(col("RLDNR")))\
        .withColumn("RYEAR",trim(col("RYEAR")))\
        .withColumn("DOCNR",trim(col("DOCNR")))\
        .withColumn("DOCLN",trim(col("DOCLN")))\
        .withColumn("RBUKRS",trim(col("RBUKRS")))\
        .withColumn("RCLNT",trim(col("RCLNT")))\
        .withColumn("REFDOCNR",trim(col("REFDOCNR")))

    print("Inside transformation/raw_euh/GLPCA.py",df)
    return (df)


