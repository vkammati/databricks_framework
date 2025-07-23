  
#GLIDXA_STN Transform Load Landing to Raw



import logging

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DecimalType, DoubleType, BinaryType
from datta_pipeline_library.core.landing_raw import landing_raw,landing_raw_batch,landing_raw_aecorsoft
from datta_pipeline_library.core.transform_basic import transform_yrmono 

def load_GLIDXA_STN(spn, base_config):
        
    #:param spn: Azure SPN used to call the Databricks REST API
    #:param base_config: BaseConfig object
        
    logging.info("Started data load for GLIDXA_STN from Landing to Raw")

    cost_schema = (StructType([
        StructField("RCLNT", StringType(), True),
StructField("AWREF", StringType(), True),
StructField("AWTYP", StringType(), True),
StructField("AWORG", StringType(), True),
StructField("RLDNR", StringType(), True),
StructField("DOCCT", StringType(), True),
StructField("RYEAR", StringType(), True),
StructField("DOCNR", StringType(), True),
StructField("BUDAT", StringType(), True),
StructField("RRCTY", StringType(), True),
StructField("RVERS", StringType(), True),
StructField("RCOMP", StringType(), True),
StructField("BUKRS", StringType(), True),
StructField("DOCTY", StringType(), True),
StructField("BELNR", StringType(), True),
StructField("SYSTEM_ID", StringType(), True),
StructField("LAST_DTM", DecimalType(15,0), True),
StructField("LAST_ACTION_CD", StringType(), True),

    ])
    )
    dataset_name = "GLIDXA_STN"
    input_format = "parquet"
    partitions = None
    ge_rules = None
    confidentiality=False

    landing_raw(spn, base_config, dataset_name, cost_schema, transform_yrmono, input_format, partitions, ge_rules,confidentiality)
    
    logging.info("Completed data load for GLIDXA_STN from Landing to Raw")

        