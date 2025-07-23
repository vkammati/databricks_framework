  
#ZSTVA_PE_DEX_DTL_STN Transform Load Landing to Raw



import logging

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DecimalType, DoubleType, BinaryType
from datta_pipeline_library.core.landing_raw import landing_raw,landing_raw_batch,landing_raw_aecorsoft
from datta_pipeline_library.core.transform_basic import transform_yrmono 

def load_ZSTVA_PE_DEX_DTL_STN(spn, base_config):
        
    #:param spn: Azure SPN used to call the Databricks REST API
    #:param base_config: BaseConfig object
        
    logging.info("Started data load for ZSTVA_PE_DEX_DTL_STN from Landing to Raw")

    cost_schema = (StructType([
        StructField("MANDT", StringType(), True),
StructField("DTLNUM", StringType(), True),
StructField("SPBUP", StringType(), True),
StructField("TRCO", StringType(), True),
StructField("PGP_OWNER", StringType(), True),
StructField("PGP_NAME", StringType(), True),
StructField("DGP_NUMBER", StringType(), True),
StructField("CONT", StringType(), True),
StructField("PRCL", StringType(), True),
StructField("COST_TYPE", StringType(), True),
StructField("COST_CODE", StringType(), True),
StructField("BUKRS", StringType(), True),
StructField("GSBER", StringType(), True),
StructField("PGP_OWNER_MNEM", StringType(), True),
StructField("CLASSTYPE", StringType(), True),
StructField("POSTING_TYPE", StringType(), True),
StructField("BELNR", StringType(), True),
StructField("REPL_BELNR", StringType(), True),
StructField("VBELN", StringType(), True),
StructField("REPL_VBELN", StringType(), True),
StructField("BUZEI", StringType(), True),
StructField("CLASS_SUBTYPE", StringType(), True),
StructField("PERIOD", StringType(), True),
StructField("GRADE", StringType(), True),
StructField("MATNR", StringType(), True),
StructField("BUY_SELL", StringType(), True),
StructField("BLDATE", StringType(), True),
StructField("QTY", DecimalType(16,4), True),
StructField("QTY_UNIT", StringType(), True),
StructField("DUEDT", StringType(), True),
StructField("COST_CPARTY_MNEM", StringType(), True),
StructField("COUNTERPARTY", StringType(), True),
StructField("UNIT_COST_VAL", DecimalType(13,4), True),
StructField("UNIT_UOM", StringType(), True),
StructField("UNIT_CURR", StringType(), True),
StructField("USD_VALUE", DecimalType(16,4), True),
StructField("USD_VALUE_ORIG", DecimalType(16,4), True),
StructField("CURR_VALUE", DecimalType(16,2), True),
StructField("CURR_VALUE_ORIG", DecimalType(16,2), True),
StructField("CURR_CODE", StringType(), True),
StructField("WRAPIND", StringType(), True),
StructField("DATAE", StringType(), True),
StructField("TRD_LAST_CHANGED", DecimalType(15,0), True),
StructField("OWNER", StringType(), True),
StructField("CLIENT", StringType(), True),
StructField("CLIENTCOMP", StringType(), True),
StructField("TTDATE", StringType(), True),
StructField("FIX_FLOAT", StringType(), True),
StructField("EXCHANGE", StringType(), True),
StructField("INT_EXT", StringType(), True),
StructField("PRCENDDT", StringType(), True),
StructField("BBL_VOL", DecimalType(15,4), True),
StructField("MT_AMT", DecimalType(15,4), True),
StructField("CONTSTAT", StringType(), True),
StructField("MATURITY_DATE", StringType(), True),
StructField("ALLOC_WGTN", DecimalType(10,4), True),
StructField("STL_CURR", StringType(), True),
StructField("CALL_PUT", StringType(), True),
StructField("STRIKE_PRC", DecimalType(13,4), True),
StructField("PREMIUM", DecimalType(16,4), True),
StructField("PREMIUM_CUKY", StringType(), True),
StructField("PREMIUM_UOM", StringType(), True),
StructField("STATUS", StringType(), True),
StructField("BLOCK_CODE", StringType(), True),
StructField("ACTUALISED", StringType(), True),
StructField("STLIND", StringType(), True),
StructField("TRDTYPE", StringType(), True),
StructField("CPARTY_INT_EXT", StringType(), True),
StructField("MANUAL_POST", StringType(), True),
StructField("MANUAL_RSN", StringType(), True),
StructField("IMAREX_REF_TXT", StringType(), True),
StructField("ATL_BL_OR_DC_DAT", StringType(), True),
StructField("LSTERM_FLAG", StringType(), True),
StructField("REC_LAST_CHANGED", DecimalType(15,0), True),
StructField("VESNAME", StringType(), True),
StructField("UNLOCKED_STL_IND", StringType(), True),
StructField("REV_DUMMY", StringType(), True),
StructField("DIFF_FORM_IND", StringType(), True),
StructField("FORMULA_NAME", StringType(), True),
StructField("FORMULA_VERSION", IntegerType(), True),
StructField("WET_OR_DRY_IND", StringType(), True),
StructField("MT_EQUIV_AMT", DecimalType(13,4), True),
StructField("BBL_EQUIV_VOL", DecimalType(13,4), True),
StructField("OIL_TYPE_CODE", StringType(), True),
StructField("INV_TYPE_IND", StringType(), True),
StructField("INV_LOC_CODE", StringType(), True),
StructField("CONT_DATE", StringType(), True),
StructField("NON_USD_VALUE", DecimalType(17,4), True),
StructField("COST_STATUS_CODE", StringType(), True),
StructField("LDG_LOC_CODE", StringType(), True),
StructField("DCH_LOC_CODE", StringType(), True),
StructField("SYSTEM_ID", StringType(), True),
StructField("LAST_DTM", DecimalType(15,0), True),
StructField("LAST_ACTION_CD", StringType(), True),

    ])
    )
    dataset_name = "ZSTVA_PE_DEX_DTL_STN"
    input_format = "parquet"
    partitions = None
    ge_rules = None
    confidentiality=False

    landing_raw(spn, base_config, dataset_name, cost_schema, transform_yrmono, input_format, partitions, ge_rules,confidentiality)
    
    logging.info("Completed data load for ZSTVA_PE_DEX_DTL_STN from Landing to Raw")

        