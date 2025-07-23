
from pyspark.sql.types import StringType, DecimalType
from pyspark.sql.functions import *
from datta_pipeline_library.core.spark_init import spark

def transform_to_timestamp(df):
    delta_load_source = spark.conf.get("pipeline.delta_load_source")
    full_load_source = spark.conf.get("pipeline.full_load_source")
    load_type = spark.conf.get("pipeline.load_type")
    
    if load_type == 'INIT':
        if full_load_source=='HANA':
            df=df.withColumn('LAST_DTM',col('LAST_DTM').cast(DecimalType(15,0)).cast(StringType()))
            df=df.withColumn('LAST_DTM',to_timestamp(col('LAST_DTM'),'yyyyMMddHHmmss'))
        elif full_load_source=='AECORSOFT':
            df = df.withColumn("AEDATTM",df.AEDATTM.cast("timestamp"))
    elif load_type=='DELTA':
        if delta_load_source=='HANA':
            df=df.withColumn('LAST_DTM',col('LAST_DTM').cast(DecimalType(15,0)).cast(StringType()))
            df=df.withColumn('LAST_DTM',to_timestamp(col('LAST_DTM'),'yyyyMMddHHmmss'))
        elif delta_load_source=='AECORSOFT':
            df = df.withColumn("AEDATTM",df.AEDATTM.cast("timestamp"))
    else:
        raise Exception("Provide proper delta load source name to get the sequence column")

    return df