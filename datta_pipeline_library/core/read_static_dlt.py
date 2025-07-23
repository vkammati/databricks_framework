"""
read static dlt table
"""
import dlt
from pyspark.sql import DataFrame

def read_static_dlt(tbl: str) -> DataFrame:
    """
    Takes DLT table name as input and returns a DLT time
    :param tbl: Name of the Table
    :return: DLT Table
    """
    df = dlt.read(tbl)
    return df
