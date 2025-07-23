"""
BKPF Transform RAW to EUH
"""
from datta_pipeline_library.core.spark_init import spark

from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json, regexp_replace, col, from_unixtime, split, unix_timestamp,to_date,coalesce,date_format,to_timestamp,trim
from datta_pipeline_library.core.get_sequence_column import get_sequence_column


BKPF_key=["MANDT", "BUKRS","BELNR","GJAHR"]
delta_load_source = spark.conf.get("pipeline.delta_load_source")
BKPF_sequence_by = get_sequence_column(delta_load_source)
BKPF_scd_type = 1

def BKPF(df):
    """
    Cost Management Transform
    :param df: Dataframe
    :return: Dataframe
    """
    df=df.withColumn("BLDAT", to_date(coalesce(date_format(to_date('BLDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("BLDAT", "yyyy-MM-dd")))) \
        .withColumn("BUDAT", to_date(coalesce(date_format(to_date('BUDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("BUDAT", "yyyy-MM-dd")))) \
        .withColumn("CPUDT", to_date(coalesce(date_format(to_date('CPUDT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("CPUDT", "yyyy-MM-dd")))) \
        .withColumn("AEDAT", to_date(coalesce(date_format(to_date('AEDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("AEDAT", "yyyy-MM-dd")))) \
        .withColumn("UPDDT", to_date(coalesce(date_format(to_date('UPDDT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("UPDDT", "yyyy-MM-dd")))) \
        .withColumn("WWERT", to_date(coalesce(date_format(to_date('WWERT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("WWERT", "yyyy-MM-dd")))) \
        .withColumn("STODT", to_date(coalesce(date_format(to_date('STODT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("STODT", "yyyy-MM-dd")))) \
        .withColumn("PPDAT", to_date(coalesce(date_format(to_date('PPDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("PPDAT", "yyyy-MM-dd")))) \
        .withColumn("REINDAT", to_date(coalesce(date_format(to_date('REINDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("REINDAT", "yyyy-MM-dd")))) \
        .withColumn("VATDATE", to_date(coalesce(date_format(to_date('VATDATE', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("VATDATE", "yyyy-MM-dd")))) \
        .withColumn("RESUBMISSION", to_date(coalesce(date_format(to_date('RESUBMISSION', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("RESUBMISSION", "yyyy-MM-dd")))) \
        .withColumn("INTDATE", to_date(coalesce(date_format(to_date('INTDATE', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("INTDATE", "yyyy-MM-dd")))) \
        .withColumn("PSOBT", to_date(coalesce(date_format(to_date('PSOBT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("PSOBT", "yyyy-MM-dd")))) \
        .withColumn("PSODT", to_date(coalesce(date_format(to_date('PSODT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("PSODT", "yyyy-MM-dd")))) \
        .withColumn("OFFSET_REFER_DAT", to_date(coalesce(date_format(to_date('OFFSET_REFER_DAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("OFFSET_REFER_DAT", "yyyy-MM-dd")))) \
        .withColumn("PYBASDAT", to_date(coalesce(date_format(to_date('PYBASDAT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("PYBASDAT", "yyyy-MM-dd")))) \
        .withColumn("INWARDDT_HD", to_date(coalesce(date_format(to_date('INWARDDT_HD', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("INWARDDT_HD", "yyyy-MM-dd")))) \
        .withColumn("ZCPUDT", to_date(coalesce(date_format(to_date('ZCPUDT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("ZCPUDT", "yyyy-MM-dd")))) \
        .withColumn("ZPAYDUDT", to_date(coalesce(date_format(to_date('ZPAYDUDT', 'yyyyMMdd'), 'yyyy-MM-dd'), to_date("ZPAYDUDT", "yyyy-MM-dd"))))
    df=df.withColumn("CPUTM",coalesce(date_format(to_timestamp(col('CPUTM'),'HHmmss'),'HH:mm:ss'),date_format(col('CPUTM'),'HH:mm:ss'))) \
        .withColumn("PPTME",coalesce(date_format(to_timestamp(col('PPTME'),'HHmmss'),'HH:mm:ss'),date_format(col('PPTME'),'HH:mm:ss'))) \
        .withColumn("PSOTM",coalesce(date_format(to_timestamp(col('PSOTM'),'HHmmss'),'HH:mm:ss'),date_format(col('PSOTM'),'HH:mm:ss'))) \
        .withColumn("ZCPUTM",coalesce(date_format(to_timestamp(col('ZCPUTM'),'HHmmss'),'HH:mm:ss'),date_format(col('ZCPUTM'),'HH:mm:ss')))

    df = df.withColumn("GJAHR",trim(col("GJAHR")))\
        .withColumn("MANDT",trim(col("MANDT")))\
        .withColumn("BUKRS",trim(col("BUKRS")))\
        .withColumn("BELNR",trim(col("BELNR")))
                
                
    print("Inside transformation/raw_euh/BKPF.py",df)
    return (df)