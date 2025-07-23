"""
Aecorsoft DS1HM_MT_BB01 Transform Load Landing to Raw
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DecimalType, DoubleType, BinaryType
from datta_pipeline_library.core.landing_raw import landing_raw,landing_raw_batch,landing_raw_aecorsoft
from datta_pipeline_library.core.transform_basic import transform_yrmono

def load_Aecorsoft_DS1HM_MT_BB01(spn, base_config):
    """
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    """
    logging.info("Started data load for DS1HM_MT_BB01 from Landing to Raw")

    cost_schema = StructType(
    [	StructField("AEDATTM",StringType(), True),
        StructField("OPFLAG",StringType(), True),
		StructField("MANDT", StringType(), True),
        StructField("RYEAR", StringType(), True),
        StructField("RBUKRS", StringType(), True),
        StructField("RLDNR", StringType(), True),
        StructField("DOCCT", StringType(), True),
        StructField("DOCNR", StringType(), True),
        StructField("DOCLN", StringType(), True),
        StructField("LFMON", StringType(), True),
        StructField("KONNR", StringType(), True),
        StructField("VGBEL", StringType(), True),
        StructField("REFDOCNR", StringType(), True),
        StructField("FKBER", StringType(), True),
        StructField("TIMSTP", StringType(), True),
        StructField("PARCEL_ID", StringType(), True),
        StructField("KTPNR", StringType(), True),
        StructField("VGPOS", StringType(), True),
        StructField("ZSTI_REFNO", StringType(), True),
        StructField("ZSTI_REFITEM", StringType(), True),
        StructField("ZSTI_REVNO", StringType(), True),
        StructField("DEALNO", StringType(), True),
        StructField("DEALTYPE", StringType(), True),
        StructField("DEALGROUP", StringType(), True),
        StructField("DDATFRM", StringType(), True),
        StructField("DDATTO", StringType(), True),
        StructField("ZXBLDAT", StringType(), True),
        StructField("OID_EXTBOL", StringType(), True),
        StructField("RACCT", StringType(), True),
        StructField("BGI_CARGOID", StringType(), True),
        StructField("BGI_DELIVERYID", StringType(), True),
        StructField("BGI_PARCELID", StringType(), True),
        StructField("VOL_QTY_GAL", DecimalType(13, 3), True),
        StructField("VOL_UOM_GAL", StringType(), True),
        StructField("VOL_QTY_BB6", DecimalType(13, 3), True),
        StructField("VOL_UOM_BB6", StringType(), True),
        StructField("ZACT_TYPE", StringType(), True),
	]
)

    dataset_name = "DS1HM_MT_BB01"
    raw_source_name='Aecorsoft'
    input_format = "parquet"
    partitions = None
    ge_rules = None
    confidentiality= True
    landing_raw_aecorsoft(spn, base_config, dataset_name, cost_schema, transform_yrmono, input_format, partitions, ge_rules,confidentiality,raw_source_name)
    logging.info("Completed data load for Aecorsoft DS1HM_MT_BB01 from Landing to Raw")
