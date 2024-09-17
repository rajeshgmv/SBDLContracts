from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib import ConfigLoader


def AccountDataImport(spark):
    accounts_schema = """load_date date,active_ind int,account_id string,source_sys string,account_start_date date,legal_title_1 string,legal_title_2 string,tax_id_type string,tax_id string,branch_code string,country string
    """
    accounts_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(accounts_schema) \
        .load("TestData/accounts/*") \
        .where("active_ind =1")
    return accounts_df

def PartyDataImport(spark):
    party_schema = """
    load_date date,account_id string,party_id string,relation_type string,relation_start_date date
    """
    parties_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(party_schema) \
        .load("TestData/parties/*")
    return parties_df

def PartyAddrDataImport(spark):
    party_addr_schema = """
    load_date date,party_id string,address_line_1 string,address_line_2 string,city string,postal_code string,country_of_address string,address_start_date date
    """
    party_addr_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(party_addr_schema) \
        .load("TestData/party_address/*")
    return party_addr_df