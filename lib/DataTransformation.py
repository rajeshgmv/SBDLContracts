from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

def get_insert_str(column,alias):
    return struct(lit("INSERT").alias("operation"),column.alias("newValue")).alias(alias)

def get_contract_taxid_str(taxtype,taxid):
    return struct(taxtype.alias("taxIdType"),taxid.alias("taxId"))

def partyTransform(party_df):
    Transformedparty_df = party_df.select("account_id","party_id",
                                          get_insert_str(col("party_id"),"partyIdentifier"),
                                          get_insert_str(col("relation_type"), "partyRelationshipType"),
                                          get_insert_str(col("relation_start_date"), "partyRelationStartDateTime")
                                          )
    return Transformedparty_df

def partyAddrTransform(party_df):
    TransformedpartyAddr_df = party_df.select("party_id",
                                          get_insert_str(struct(col("address_line_1").alias("addressLine1"),
                                                                col("address_line_2").alias("addressLine2"),
                                                                col("city").alias("addressCity"),
                                                                col("postal_code").alias("addressPostalCode"),
                                                                col("country_of_address").alias("addressCountry"),
                                                                col("address_start_date").alias("addressStartDate")
                                                                ),"partyAddress")                                          )
    return TransformedpartyAddr_df


def AccountTransform(account_df):


    contract_title = array(when(~isnull("legal_title_1"),
                                struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                       col("legal_title_1").alias("contractTitleLine")).alias("contractTitle")),
                           when(~isnull("legal_title_2"),
                                struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                       col("legal_title_2").alias("contractTitleLine")).alias("contractTitle"))
                           )

    contract_title_nl = filter(contract_title, lambda x: ~isnull(x))

    Transformedparty_df = account_df.select("account_id",
                                          get_insert_str(col("account_id"),"contractIdentifier"),
                                          get_insert_str(col("source_sys"), "sourceSystemIdentifier"),
                                          get_insert_str(col("account_start_date"), "contactStartDateTime"),
                                          get_insert_str(contract_title_nl, "contractTitle"),
                                          get_insert_str(get_contract_taxid_str(col("tax_id_type"),col("tax_id")), "taxIdentifier"),
                                          get_insert_str(col("branch_code"), "contractBranchCode"),
                                          get_insert_str(col("country"), "contractCountry")
                                          )
    return Transformedparty_df

def apply_header(spark, df):
    header_info = [("SBDL-Contract", 1, 0), ]
    header_df = spark.createDataFrame(header_info) \
        .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")

    event_df = header_df.hint("broadcast").crossJoin(df) \
        .select(struct(expr("uuid()").alias("eventIdentifier"),
                       col("eventType"), col("majorSchemaVersion"), col("minorSchemaVersion"),
                       lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
                       ).alias("eventHeader"),
                array(struct(lit("contractIdentifier").alias("keyField"),
                             col("account_id").alias("keyValue")
                             )).alias("keys"),
                struct(col("contractIdentifier"),
                       col("sourceSystemIdentifier"),
                       col("contactStartDateTime"),
                       col("contractTitle"),
                       col("taxIdentifier"),
                       col("contractBranchCode"),
                       col("contractCountry"),
                       col("partyRelations")
                       ).alias("payload")
                )

    return event_df