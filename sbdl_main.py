import sys
import uuid

import lib.Utils

from lib import Utils
from lib.Dataimport import *
from lib.DataTransformation import *
from lib.Dataimport import PartyDataImport, PartyAddrDataImport

if __name__=="__main__":
    if len(sys.argv)<3:
        print("provide all the arguements")
        sys.exit(-1)
    print("creating spark session")
    thisenv = sys.argv[1].upper()
    load_date = sys.argv[2]
    spark = Utils.get_spark_session(thisenv)
    print("spark session created")
    print("importing data into memory")
    Account_df = AccountDataImport(spark)
    Party_df = PartyDataImport(spark)
    Party_addr_df = PartyAddrDataImport(spark)
    print("data ready for transformation")

    print("trying transform party")

    Trans_party_df = partyTransform(Party_df)

    print("party transformed")

    """Trans_party_df \
        .write \
        .mode("overwrite") \
        .format("json") \
        .option("path","TestData/Output/") \
        .save()
    """
    print("transforming party address")


    Trans_party_addr_df = partyAddrTransform(Party_addr_df)

    print("party address transformed")

    print("transforming Accounts")

    Trans_account_df = AccountTransform(Account_df)
    #Trans_account_df.show()

    print("Accounts transformed")
    print("joining parties")
    join_expr = Trans_party_df.party_id==Trans_party_addr_df.party_id
    joined_party = Trans_party_df \
                    .join(Trans_party_addr_df,join_expr,"left") \
                    .groupBy("account_id") \
                    .agg(collect_list(struct("partyIdentifier",
                                 "partyRelationshipType",
                                 "partyRelationStartDateTime",
                                 "partyAddress").alias()
                          ).alias("partyRelations")) \
                    .drop(Trans_party_addr_df.party_id) \
                    .drop(Trans_party_df.party_id)
    print("parties joined")

    print("joining Account and parties")
    join_expr = Trans_account_df.account_id==joined_party.account_id

    joined_DF = Trans_account_df \
        .join(joined_party, join_expr, "left") \
        .drop(joined_party.account_id)

    print("Joined accounts and parties")

    print("adding header")

    final_df = apply_header(spark, joined_DF)

    print("header added")

    final_df.write \
        .mode("overwrite") \
        .format("json") \
        .option("path", "TestData/Output/") \
        .save()