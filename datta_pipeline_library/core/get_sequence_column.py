from datta_pipeline_library.core.spark_init import spark
 
def get_sequence_column(delta_load_source):
    
    full_load_source = spark.conf.get("pipeline.full_load_source")
    load_type = spark.conf.get("pipeline.load_type")
    
    if load_type == 'INIT':
        if full_load_source=='HANA':
            return "LAST_DTM"
        elif full_load_source=='AECORSOFT':
            return "AEDATTM"
    elif load_type=='DELTA':
        if delta_load_source=='HANA':
            return "LAST_DTM"
        elif delta_load_source=='AECORSOFT':
            return "AEDATTM"
    else:
        raise Exception("Provide proper delta load source name to get the sequence column")

def get_delete_columnname(load_type, full_load_source,delta_load_source):
    if load_type == 'INIT':
        if full_load_source=='HANA':
            return 'LAST_ACTION_CD'
        elif full_load_source=='AECORSOFT':
            return 'OPFLAG'
    
    elif load_type =='DELTA':
        if delta_load_source=='HANA':
            return 'LAST_ACTION_CD'
        elif delta_load_source=='AECORSOFT':
            return 'OPFLAG'
    else:
        raise Exception("Provide proper delta load source name to get the sequence column")