"""
DS1HM_MT_BB Transform Load Landing to Raw
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DecimalType, DoubleType, BinaryType
from datta_pipeline_library.core.landing_raw import landing_raw,landing_raw_batch,landing_raw_aecorsoft
from datta_pipeline_library.core.transform_basic import transform_yrmono


def load_DS1HM_MT_BB(spn, base_config):
    """
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    """
    logging.info("Started data load for DS1HM_MT_BB from Landing to Raw")

    cost_schema = StructType(
    [
        StructField("MANDT", StringType(), True),
        StructField("RYEAR", StringType(), True),
        StructField("RBUKRS", StringType(), True),
        StructField("RLDNR", StringType(), True),
        StructField("DOCCT", StringType(), True),
        StructField("DOCNR", StringType(), True),
        StructField("DOCLN", StringType(), True),
        StructField("GL_SIRID", StringType(), True),
        StructField("LFGJA", StringType(), True),
        StructField("LFMON", StringType(), True),
        StructField("REFACTIV", StringType(), True),
        StructField("RHOART", StringType(), True),
        StructField("REFDOCNR", StringType(), True),
        StructField("REFRYEAR", StringType(), True),
        StructField("REFDOCLN", StringType(), True),
        StructField("BWART", StringType(), True),
        StructField("SHKZG", StringType(), True),
        StructField("MGRP", StringType(), True),
        StructField("KEYFIG", StringType(), True),
        StructField("MB_FLG", StringType(), True),
        StructField("EXGTYP", StringType(), True),
        StructField("EXGNUM", StringType(), True),
        StructField("EBELN", StringType(), True),
        StructField("EBELP", StringType(), True),
        StructField("LIFNR", StringType(), True),
        StructField("GR1", StringType(), True),
        StructField("GR2", StringType(), True),
        StructField("GR3", StringType(), True),
        StructField("PRPRD", StringType(), True),
        StructField("KAUFN", StringType(), True),
        StructField("KDPOS", StringType(), True),
        StructField("KUNAG", StringType(), True),
        StructField("VBELN", StringType(), True),
        StructField("POSNR", StringType(), True),
        StructField("SHTYPE", StringType(), True),
        StructField("SHNUMBER", StringType(), True),
        StructField("RSNUM", StringType(), True),
        StructField("RSPOS", StringType(), True),
        StructField("NOMTK", StringType(), True),
        StructField("NOMIT", StringType(), True),
        StructField("DOCIND", StringType(), True),
        StructField("MBLNR", StringType(), True),
        StructField("MJAHR", StringType(), True),
        StructField("ZEILE", StringType(), True),
        StructField("FKBER", StringType(), True),
        StructField("KTGRD", StringType(), True),
        StructField("ZDMFR", StringType(), True),
        StructField("BUKRS", StringType(), True),
        StructField("LAND1", StringType(), True),
        StructField("PTYPE1", StringType(), True),
        StructField("WERKS", StringType(), True),
        StructField("BZIRK", StringType(), True),
        StructField("MATNR", StringType(), True),
        StructField("PRCTR", StringType(), True),
        StructField("HMPRCTR", StringType(), True),
        StructField("UMBKS", StringType(), True),
        StructField("UMWRK", StringType(), True),
        StructField("PTYPE2", StringType(), True),
        StructField("UMMAT", StringType(), True),
        StructField("BSARK", StringType(), True),
        StructField("OIC_MOT", StringType(), True),
        StructField("VSBED", StringType(), True),
        StructField("OIC_AORGIN", StringType(), True),
        StructField("OIC_ADESTN", StringType(), True),
        StructField("WGT_UOM_KG", StringType(), True),
        StructField("WGT_QTY", DecimalType(13, 3), True),
        StructField("VOL_UOM_L15", StringType(), True),
        StructField("VOL_QTY", DecimalType(13, 3), True),
        StructField("MEINS", StringType(), True),
        StructField("LFIMG", DecimalType(13, 3), True),
        StructField("ERFME", StringType(), True),
        StructField("ERFMG", DecimalType(13, 3), True),
        StructField("QTYFLAG", StringType(), True),
        StructField("TIMSTP", StringType(), True),
        StructField("SYSTEM_ID", StringType(), True),
        StructField("LAST_ACTION_CD", StringType(), True),
        StructField("LAST_DTM", DecimalType(15, 0), True),
    ]
)
    dataset_name = "DS1HM_MT_BB"
    input_format = "parquet"
    partitions = None
    ge_rules = None
    confidentiality=True
    landing_raw(spn, base_config, dataset_name, cost_schema, transform_yrmono, input_format, partitions, ge_rules,confidentiality)
    logging.info("Completed data load for DS1HM_MT_BB from Landing to Raw")
