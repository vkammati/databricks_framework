"""
Load all tables RAW to EUH
"""
import time
import json
from datta_pipeline_library.core.spark_init import spark

from datta_pipeline_library.core.raw_euh import raw_euh, raw_euh_aecorsoft_delta, raw_euh_merge,raw_euh_batch
from datta_pipeline_library.core.transform_basic import transform_basic

from datta_pipeline_library.transformation.finance.raw_euh.GLPCA import GLPCA, GLPCA_key, GLPCA_sequence_by, GLPCA_scd_type
from datta_pipeline_library.transformation.finance.raw_euh.DS1HM_MT_BB import DS1HM_MT_BB, DS1HM_MT_BB_key, DS1HM_MT_BB_sequence_by, DS1HM_MT_BB_scd_type
from datta_pipeline_library.transformation.finance.raw_euh.BKPF import BKPF, BKPF_key, BKPF_sequence_by, BKPF_scd_type
from datta_pipeline_library.transformation.finance.raw_euh.DS1HM_MT_BB01 import DS1HM_MT_BB01, DS1HM_MT_BB01_key,DS1HM_MT_BB01_sequence_by,DS1HM_MT_BB01_scd_type
from datta_pipeline_library.edc.collibra import fetch_business_metadata

#Minerva
from datta_pipeline_library.transformation.finance.raw_euh.BSEG_STN import BSEG_STN, BSEG_STN_key, BSEG_STN_sequence_by, BSEG_STN_scd_type

from datta_pipeline_library.transformation.finance.raw_euh.BSEG_TSAP import BSEG_TSAP, BSEG_TSAP_key, BSEG_TSAP_sequence_by, BSEG_TSAP_scd_type

from datta_pipeline_library.transformation.finance.raw_euh.BKPF_STN import BKPF_STN, BKPF_STN_key, BKPF_STN_sequence_by, BKPF_STN_scd_type

from datta_pipeline_library.transformation.finance.raw_euh.BKPF_TSAP import BKPF_TSAP, BKPF_TSAP_key, BKPF_TSAP_sequence_by, BKPF_TSAP_scd_type

from datta_pipeline_library.transformation.finance.raw_euh.GLIDXA_STN import GLIDXA_STN, GLIDXA_STN_key, GLIDXA_STN_sequence_by, GLIDXA_STN_scd_type

from datta_pipeline_library.transformation.finance.raw_euh.GLIDXA_TSAP import GLIDXA_TSAP, GLIDXA_TSAP_key, GLIDXA_TSAP_sequence_by, GLIDXA_TSAP_scd_type

from datta_pipeline_library.transformation.finance.raw_euh.ZSTVA_PE_DEX_DTL_STN import ZSTVA_PE_DEX_DTL_STN, ZSTVA_PE_DEX_DTL_STN_key, ZSTVA_PE_DEX_DTL_STN_sequence_by, ZSTVA_PE_DEX_DTL_STN_scd_type

from datta_pipeline_library.transformation.finance.raw_euh.ZSTVA_PE_MC_DTL_STN import ZSTVA_PE_MC_DTL_STN, ZSTVA_PE_MC_DTL_STN_key, ZSTVA_PE_MC_DTL_STN_sequence_by, ZSTVA_PE_MC_DTL_STN_scd_type

load_type = spark.conf.get("pipeline.load_type")
print("raw_to_euh load_type:", load_type)
full_load_source = spark.conf.get("pipeline.full_load_source")
delta_load_source = spark.conf.get("pipeline.delta_load_source")

def execute_raw_euh_fi(spn, base_config, collibra_config):
    """
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param collibra_config: CollibraConfig object
    """
    if(load_type=="INIT"):
        if(full_load_source=="HANA"):
            print("Inside raw_to_euh execute.py for : load_type = INIT and full_load_source = HANA")
            raw_euh_merge(spn, base_config, GLPCA, "GLPCA", GLPCA_key, GLPCA_sequence_by, GLPCA_scd_type, None )
            raw_euh_merge(spn, base_config, DS1HM_MT_BB, "DS1HM_MT_BB",DS1HM_MT_BB_key, DS1HM_MT_BB_sequence_by, DS1HM_MT_BB_scd_type, None)    
            raw_euh_merge(spn, base_config, BKPF, "BKPF", BKPF_key, BKPF_sequence_by, BKPF_scd_type, None)
            raw_euh_merge(spn, base_config, DS1HM_MT_BB01, "DS1HM_MT_BB01",DS1HM_MT_BB01_key, DS1HM_MT_BB01_sequence_by,DS1HM_MT_BB01_scd_type, None)
        elif(full_load_source=="AECORSOFT"):
            print("Inside raw_to_euh execute.py : load_type = INIT and full_load_source = AECORSOFT")
            raw_euh_aecorsoft_delta(spn, base_config, GLPCA, "GLPCA", GLPCA_key, GLPCA_sequence_by, GLPCA_scd_type, None)
            raw_euh_aecorsoft_delta(spn, base_config, DS1HM_MT_BB, "DS1HM_MT_BB", DS1HM_MT_BB_key, DS1HM_MT_BB_sequence_by, DS1HM_MT_BB_scd_type, None)
            raw_euh_aecorsoft_delta(spn, base_config, BKPF, "BKPF", BKPF_key, BKPF_sequence_by, BKPF_scd_type, None)
            raw_euh_aecorsoft_delta(spn, base_config, DS1HM_MT_BB01, "DS1HM_MT_BB01",DS1HM_MT_BB01_key, DS1HM_MT_BB01_sequence_by,DS1HM_MT_BB01_scd_type, None)
    elif(load_type=="DELTA"):
        if(delta_load_source=="HANA"):
            print("Inside raw_to_euh execute.py for : load_type = DELTA and full_load_source = HANA")
            raw_euh_merge(spn, base_config, GLPCA, "GLPCA", GLPCA_key, GLPCA_sequence_by, GLPCA_scd_type, None )
            raw_euh_merge(spn, base_config, DS1HM_MT_BB, "DS1HM_MT_BB",DS1HM_MT_BB_key, DS1HM_MT_BB_sequence_by, DS1HM_MT_BB_scd_type, None)    
            raw_euh_merge(spn, base_config, BKPF, "BKPF", BKPF_key, BKPF_sequence_by, BKPF_scd_type, None)
            raw_euh_merge(spn, base_config, DS1HM_MT_BB01, "DS1HM_MT_BB01",DS1HM_MT_BB01_key, DS1HM_MT_BB01_sequence_by,DS1HM_MT_BB01_scd_type, None)
        elif(delta_load_source=="AECORSOFT"):
            print("Inside raw_to_euh execute.py : load_type = DELTA and full_load_source = AECORSOFT")
            raw_euh_aecorsoft_delta(spn, base_config, GLPCA, "GLPCA", GLPCA_key, GLPCA_sequence_by, GLPCA_scd_type, None)
            raw_euh_aecorsoft_delta(spn, base_config, DS1HM_MT_BB, "DS1HM_MT_BB", DS1HM_MT_BB_key, DS1HM_MT_BB_sequence_by, DS1HM_MT_BB_scd_type, None)
            raw_euh_aecorsoft_delta(spn, base_config, BKPF, "BKPF", BKPF_key, BKPF_sequence_by, BKPF_scd_type, None)
            raw_euh_aecorsoft_delta(spn, base_config, DS1HM_MT_BB01, "DS1HM_MT_BB01", DS1HM_MT_BB01_key, DS1HM_MT_BB01_sequence_by,DS1HM_MT_BB01_scd_type, None)
    else:
        print("Please provide proper load type parameters [INIT/DELTA]...")  
        raise Exception("Please provide proper load type parameters for FI EUH raw-euh[INIT/DELTA]")

def execute_raw_euh_fi_d(spn, base_config, collibra_config):
    """
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    :param collibra_config: CollibraConfig object
    """
    if(load_type=="INIT"):
        if(full_load_source=="HANA"):
            print("Inside raw_to_euh execute.py for : load_type = INIT and full_load_source = HANA")
            raw_euh_merge(spn, base_config, BSEG_STN, "BSEG_STN", BSEG_STN_key, BSEG_STN_sequence_by, BSEG_STN_scd_type, None )
            raw_euh_merge(spn, base_config, BSEG_TSAP, "BSEG_TSAP",BSEG_TSAP_key, BSEG_TSAP_sequence_by, BSEG_TSAP_scd_type, None)    
            raw_euh_merge(spn, base_config, BKPF_STN, "BKPF_STN", BKPF_STN_key, BKPF_STN_sequence_by, BKPF_STN_scd_type, None)
            raw_euh_merge(spn, base_config, BKPF_TSAP, "BKPF_TSAP",BKPF_TSAP_key, BKPF_TSAP_sequence_by,BKPF_TSAP_scd_type, None)
            raw_euh_merge(spn, base_config, GLIDXA_STN, "GLIDXA_STN", GLIDXA_STN_key, GLIDXA_STN_sequence_by, GLIDXA_STN_scd_type, None )
            raw_euh_merge(spn, base_config, GLIDXA_TSAP, "GLIDXA_TSAP",GLIDXA_TSAP_key, GLIDXA_TSAP_sequence_by, GLIDXA_TSAP_scd_type, None)    
            raw_euh_merge(spn, base_config, ZSTVA_PE_DEX_DTL_STN, "ZSTVA_PE_DEX_DTL_STN", ZSTVA_PE_DEX_DTL_STN_key, ZSTVA_PE_DEX_DTL_STN_sequence_by, ZSTVA_PE_DEX_DTL_STN_scd_type, None)
            raw_euh_merge(spn, base_config, ZSTVA_PE_MC_DTL_STN, "ZSTVA_PE_MC_DTL_STN",ZSTVA_PE_MC_DTL_STN_key, ZSTVA_PE_MC_DTL_STN_sequence_by,ZSTVA_PE_MC_DTL_STN_scd_type, None)
        elif(full_load_source=="AECORSOFT"):
            print("Inside raw_to_euh execute.py : load_type = INIT and full_load_source = AECORSOFT")
            
            
    elif(load_type=="DELTA"):
        if(delta_load_source=="HANA"):
            print("Inside raw_to_euh execute.py for : load_type = DELTA and full_load_source = HANA")
            raw_euh_merge(spn, base_config, BSEG_STN, "BSEG_STN", BSEG_STN_key, BSEG_STN_sequence_by, BSEG_STN_scd_type, None )
            raw_euh_merge(spn, base_config, BSEG_TSAP, "BSEG_TSAP",BSEG_TSAP_key, BSEG_TSAP_sequence_by, BSEG_TSAP_scd_type, None)    
            raw_euh_merge(spn, base_config, BKPF_STN, "BKPF_STN", BKPF_STN_key, BKPF_STN_sequence_by, BKPF_STN_scd_type, None)
            raw_euh_merge(spn, base_config, BKPF_TSAP, "BKPF_TSAP",BKPF_TSAP_key, BKPF_TSAP_sequence_by,BKPF_TSAP_scd_type, None)
            raw_euh_merge(spn, base_config, GLIDXA_STN, "GLIDXA_STN", GLIDXA_STN_key, GLIDXA_STN_sequence_by, GLIDXA_STN_scd_type, None )
            raw_euh_merge(spn, base_config, GLIDXA_TSAP, "GLIDXA_TSAP",GLIDXA_TSAP_key, GLIDXA_TSAP_sequence_by, GLIDXA_TSAP_scd_type, None)    
            raw_euh_merge(spn, base_config, ZSTVA_PE_DEX_DTL_STN, "ZSTVA_PE_DEX_DTL_STN", ZSTVA_PE_DEX_DTL_STN_key, ZSTVA_PE_DEX_DTL_STN_sequence_by, ZSTVA_PE_DEX_DTL_STN_scd_type, None)
            raw_euh_merge(spn, base_config, ZSTVA_PE_MC_DTL_STN, "ZSTVA_PE_MC_DTL_STN",ZSTVA_PE_MC_DTL_STN_key, ZSTVA_PE_MC_DTL_STN_sequence_by,ZSTVA_PE_MC_DTL_STN_scd_type, None)
        elif(delta_load_source=="AECORSOFT"):
            print("Inside raw_to_euh execute.py : load_type = DELTA and full_load_source = AECORSOFT")
            
            
    else:
        print("Please provide proper load type parameters [INIT/DELTA]...")  
        raise Exception("Please provide proper load type parameters for FI EUH raw-euh[INIT/DELTA]")