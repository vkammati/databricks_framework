"""
Load all tables Landing to RAW
"""
from datta_pipeline_library.core.spark_init import spark

from datta_pipeline_library.transformation.finance.landing_raw.GLPCA import load_GLPCA
from datta_pipeline_library.transformation.finance.landing_raw.DS1HM_MT_BB import load_DS1HM_MT_BB
from datta_pipeline_library.transformation.finance.landing_raw.BKPF import load_BKPF
from datta_pipeline_library.transformation.finance.landing_raw.DS1HM_MT_BB01 import load_DS1HM_MT_BB01

#Minerva
from datta_pipeline_library.transformation.finance.landing_raw.BSEG_STN import load_BSEG_STN
from datta_pipeline_library.transformation.finance.landing_raw.BSEG_TSAP import load_BSEG_TSAP
from datta_pipeline_library.transformation.finance.landing_raw.BKPF_STN import load_BKPF_STN
from datta_pipeline_library.transformation.finance.landing_raw.BKPF_TSAP import load_BKPF_TSAP
from datta_pipeline_library.transformation.finance.landing_raw.GLIDXA_STN import load_GLIDXA_STN
from datta_pipeline_library.transformation.finance.landing_raw.GLIDXA_TSAP import load_GLIDXA_TSAP
from datta_pipeline_library.transformation.finance.landing_raw.ZSTVA_PE_DEX_DTL_STN import load_ZSTVA_PE_DEX_DTL_STN
from datta_pipeline_library.transformation.finance.landing_raw.ZSTVA_PE_MC_DTL_STN import load_ZSTVA_PE_MC_DTL_STN


from datta_pipeline_library.transformation.finance.landing_raw.Aecorsoft_GLPCA import load_Aecorsoft_GLPCA
from datta_pipeline_library.transformation.finance.landing_raw.Aecorsoft_DS1HM_MT_BB import load_Aecorsoft_DS1HM_MT_BB
from datta_pipeline_library.transformation.finance.landing_raw.Aecorsoft_BKPF import load_Aecorsoft_BKPF
from datta_pipeline_library.transformation.finance.landing_raw.Aecorsoft_DS1HM_MT_BB01 import load_Aecorsoft_DS1HM_MT_BB01

load_type = spark.conf.get("pipeline.load_type")
print("raw_to_euh load_type:", load_type)
full_load_source = spark.conf.get("pipeline.full_load_source")
delta_load_source = spark.conf.get("pipeline.delta_load_source")

def execute_landing_raw_fi(spn, base_config):
    """
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    """
    
    if(load_type=="INIT"):
        if(full_load_source=="HANA"):
            print("Inside landing_to_raw execute.py for FULL HANA Load for FI EUH")
            load_GLPCA(spn, base_config)
            load_DS1HM_MT_BB(spn, base_config) 
            load_BKPF(spn, base_config)
            load_DS1HM_MT_BB01(spn,base_config)
        elif(full_load_source=="AECORSOFT"):
            print("Inside landing_to_raw execute.py for FULL AECORSOFT Load for FI EUH") 
            load_Aecorsoft_GLPCA(spn, base_config)
            load_Aecorsoft_DS1HM_MT_BB(spn, base_config)
            load_Aecorsoft_BKPF(spn, base_config)
            load_Aecorsoft_DS1HM_MT_BB01(spn, base_config)
        else:
            print("Please provide proper full_load_source parameters for FI EUH landing-raw...")
    elif(load_type=="DELTA"):
        if(delta_load_source=="HANA"):
            print("Inside landing_to_raw execute.py for DELTA HANA Load for FI EUH")
            load_GLPCA(spn, base_config)
            load_DS1HM_MT_BB(spn, base_config)  
            load_BKPF(spn, base_config)
            load_DS1HM_MT_BB01(spn,base_config)      
        elif(delta_load_source=="AECORSOFT"):
            print("Inside landing_to_raw execute.py for DELTA AECORSOFT Load for FI EUH")
            load_Aecorsoft_GLPCA(spn, base_config)
            load_Aecorsoft_DS1HM_MT_BB(spn, base_config)
            load_Aecorsoft_BKPF(spn, base_config)
            load_Aecorsoft_DS1HM_MT_BB01(spn, base_config)
        else:
            print("Please provide proper full_load_source parameters for FI EUH landing-raw...")
    else:
        print("Please provide proper load type parameters [INIT/DELTA]...")
        raise Exception("Please provide proper load type parameters for FI EUH landing-raw[INIT/DELTA]")

def execute_landing_raw_fi_d(spn, base_config):
    """
    :param spn: Azure SPN used to call the Databricks REST API
    :param base_config: BaseConfig object
    """
    
    if(load_type=="INIT"):
        if(full_load_source=="HANA"):
            print("Inside landing_to_raw execute.py for FULL HANA Load for FI EUH")
            load_BSEG_STN(spn, base_config)
            load_BSEG_TSAP(spn, base_config) 
            load_BKPF_STN(spn, base_config)
            load_BKPF_TSAP(spn,base_config)
            load_GLIDXA_STN(spn, base_config)
            load_GLIDXA_TSAP(spn, base_config) 
            load_ZSTVA_PE_DEX_DTL_STN(spn, base_config)
            load_ZSTVA_PE_MC_DTL_STN(spn,base_config)
        elif(full_load_source=="AECORSOFT"):
            print("Inside landing_to_raw execute.py for FULL AECORSOFT Load for FI EUH")
            
        else:
            print("Please provide proper full_load_source parameters for FI EUH landing-raw...")
    elif(load_type=="DELTA"):
        if(delta_load_source=="HANA"):
            print("Inside landing_to_raw execute.py for DELTA HANA Load for FI EUH")
            load_BSEG_STN(spn, base_config)
            load_BSEG_TSAP(spn, base_config) 
            load_BKPF_STN(spn, base_config)
            load_BKPF_TSAP(spn,base_config)
            load_GLIDXA_STN(spn, base_config)
            load_GLIDXA_TSAP(spn, base_config) 
            load_ZSTVA_PE_DEX_DTL_STN(spn, base_config)
            load_ZSTVA_PE_MC_DTL_STN(spn,base_config)      
        elif(delta_load_source=="AECORSOFT"):
            print("Inside landing_to_raw execute.py for DELTA AECORSOFT Load for FI EUH")
            
        else:
            print("Please provide proper full_load_source parameters for FI EUH landing-raw...")
    else:
        print("Please provide proper load type parameters [INIT/DELTA]...")
        raise Exception("Please provide proper load type parameters for FI EUH landing-raw[INIT/DELTA]")