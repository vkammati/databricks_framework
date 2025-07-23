"""
GLPCA Transform Load Landing to Raw
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DecimalType, DoubleType, BinaryType
from datta_pipeline_library.core.landing_raw import landing_raw,landing_raw_batch,landing_raw_aecorsoft
from datta_pipeline_library.core.transform_basic import transform_yrmono


def load_GLPCA(spn, base_config):
    """
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    """
    logging.info("Started data load for GLPCA from Landing to Raw")

    cost_schema = (StructType(
    [
        StructField("RCLNT", StringType(), True),
        StructField("GL_SIRID", StringType(), True),
        StructField("RLDNR", StringType(), True),
        StructField("RRCTY", StringType(), True),
        StructField("RVERS", StringType(), True),
        StructField("RYEAR", StringType(), True),
        StructField("RTCUR", StringType(), True),
        StructField("RUNIT", StringType(), True),
        StructField("DRCRK", StringType(), True),
        StructField("POPER", StringType(), True),
        StructField("DOCCT", StringType(), True),
        StructField("DOCNR", StringType(), True),
        StructField("DOCLN", StringType(), True),
        StructField("RBUKRS", StringType(), True),
        StructField("RPRCTR", StringType(), True),
        StructField("RHOART", StringType(), True),
        StructField("RFAREA", StringType(), True),
        StructField("KOKRS", StringType(), True),
        StructField("RACCT", StringType(), True),
        StructField("HRKFT", StringType(), True),
        StructField("RASSC", StringType(), True),
        StructField("EPRCTR", StringType(), True),
        StructField("ACTIV", StringType(), True),
        StructField("AFABE", StringType(), True),
        StructField("OCLNT", StringType(), True),
        StructField("SBUKRS", StringType(), True),
        StructField("SPRCTR", StringType(), True),
        StructField("SHOART", StringType(), True),
        StructField("SFAREA", StringType(), True),
        StructField("TSL", DecimalType(15, 2), True),
        StructField("HSL", DecimalType(15, 2), True),
        StructField("KSL", DecimalType(15, 2), True),
        StructField("MSL", DecimalType(15, 3), True),
        StructField("CPUDT", StringType(), True),
        StructField("CPUTM", StringType(), True),
        StructField("USNAM", StringType(), True),
        StructField("SGTXT", StringType(), True),
        StructField("AUTOM", StringType(), True),
        StructField("DOCTY", StringType(), True),
        StructField("BLDAT", StringType(), True),
        StructField("BUDAT", StringType(), True),
        StructField("WSDAT", StringType(), True),
        StructField("REFDOCNR", StringType(), True),
        StructField("REFRYEAR", StringType(), True),
        StructField("REFDOCLN", StringType(), True),
        StructField("REFDOCCT", StringType(), True),
        StructField("REFACTIV", StringType(), True),
        StructField("AWTYP", StringType(), True),
        StructField("AWORG", StringType(), True),
        StructField("WERKS", StringType(), True),
        StructField("GSBER", StringType(), True),
        StructField("KOSTL", StringType(), True),
        StructField("LSTAR", StringType(), True),
        StructField("AUFNR", StringType(), True),
        StructField("AUFPL", StringType(), True),
        StructField("ANLN1", StringType(), True),
        StructField("ANLN2", StringType(), True),
        StructField("MATNR", StringType(), True),
        StructField("BWKEY", StringType(), True),
        StructField("BWTAR", StringType(), True),
        StructField("ANBWA", StringType(), True),
        StructField("KUNNR", StringType(), True),
        StructField("LIFNR", StringType(), True),
        StructField("RMVCT", StringType(), True),
        StructField("EBELN", StringType(), True),
        StructField("EBELP", StringType(), True),
        StructField("KSTRG", StringType(), True),
        StructField("ERKRS", StringType(), True),
        StructField("PAOBJNR", StringType(), True),
        StructField("PASUBNR", StringType(), True),
        StructField("PS_PSP_PNR", StringType(), True),
        StructField("KDAUF", StringType(), True),
        StructField("KDPOS", StringType(), True),
        StructField("FKART", StringType(), True),
        StructField("VKORG", StringType(), True),
        StructField("VTWEG", StringType(), True),
        StructField("AUBEL", StringType(), True),
        StructField("AUPOS", StringType(), True),
        StructField("SPART", StringType(), True),
        StructField("VBELN", StringType(), True),
        StructField("POSNR", StringType(), True),
        StructField("VKGRP", StringType(), True),
        StructField("VKBUR", StringType(), True),
        StructField("VBUND", StringType(), True),
        StructField("LOGSYS", StringType(), True),
        StructField("ALEBN", StringType(), True),
        StructField("AWSYS", StringType(), True),
        StructField("VERSA", StringType(), True),
        StructField("STFLG", StringType(), True),
        StructField("STOKZ", StringType(), True),
        StructField("STAGR", StringType(), True),
        StructField("GRTYP", StringType(), True),
        StructField("REP_MATNR", StringType(), True),
        StructField("CO_PRZNR", StringType(), True),
        StructField("IMKEY", StringType(), True),
        StructField("DABRZ", StringType(), True),
        StructField("VALUT", StringType(), True),
        StructField("RSCOPE", StringType(), True),
        StructField("AWREF_REV", StringType(), True),
        StructField("AWORG_REV", StringType(), True),
        StructField("BWART", StringType(), True),
        StructField("BLART", StringType(), True),
        StructField("ZZRBLGP", StringType(), True),
        StructField("SYSTEM_ID", StringType(), True),
        StructField("LAST_ACTION_CD", StringType(), True),
        StructField("LAST_DTM", DecimalType(15, 0), True),
    ]
)
)
    dataset_name = "GLPCA"
    input_format = "parquet"
    partitions = None
    ge_rules = None
    confidentiality=True
    landing_raw(spn, base_config, dataset_name, cost_schema, transform_yrmono, input_format, partitions, ge_rules,confidentiality)
    logging.info("Completed data load for GLPCA from Landing to Raw")
