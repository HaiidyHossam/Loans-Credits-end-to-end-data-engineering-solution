from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("KafkaSparkApp").getOrCreate()



schema = StructType([ 
    StructField("BORROWER_KEY_PK_SK", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("loan_amnt", DoubleType(), True),
    StructField("funded_amnt", DoubleType(), True),
    StructField("funded_amnt_inv", DoubleType(), True),
    StructField("term", StringType(), True),
    StructField("int_rate", StringType(), True),
    StructField("installment", DoubleType(), True),
    StructField("grade", StringType(), True),
    StructField("sub_grade", StringType(), True),
    StructField("emp_title", StringType(), False),
    StructField("emp_length", StringType(), True),
    StructField("home_ownership", StringType(), True),
    StructField("annual_inc", DoubleType(), True),
    StructField("verification_status", StringType(), True),
    StructField("issue_d", DateType(), True),
    StructField("loan_status", StringType(), True),
    StructField("pymnt_plan", StringType(), True),
    StructField("url", StringType(), True),
    StructField("purpose", StringType(), True),
    StructField("title", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("addr_state", StringType(), True),
    StructField("dti", DoubleType(), True),
    StructField("delinq_2yrs", DoubleType(), True),
    StructField("earliest_cr_line", StringType(), True),
    StructField("fico_range_low", DoubleType(), True),
    StructField("fico_range_high", DoubleType(), True),
    StructField("inq_last_6mths", DoubleType(), True),
    StructField("mths_since_last_delinq", DoubleType(), True),
    StructField("mths_since_last_record", DoubleType(), True),
    StructField("open_acc", DoubleType(), True),
    StructField("pub_rec", DoubleType(), True),
    StructField("revol_bal", DoubleType(), True),
    StructField("revol_util", StringType(), True),
    StructField("total_acc", DoubleType(), True),
    StructField("initial_list_status", StringType(), True),
    StructField("out_prncp", DoubleType(), True),
    StructField("out_prncp_inv", DoubleType(), True),
    StructField("total_pymnt", DoubleType(), True),
    StructField("total_pymnt_inv", DoubleType(), True),
    StructField("total_rec_prncp", DoubleType(), True),
    StructField("total_rec_int", DoubleType(), True),
    StructField("total_rec_late_fee", DoubleType(), True),
    StructField("recoveries", DoubleType(), True),
    StructField("collection_recovery_fee", DoubleType(), True),
    StructField("last_pymnt_d", StringType(), True),
    StructField("last_pymnt_amnt", DoubleType(), True),
    StructField("next_pymnt_d", StringType(), True),
    StructField("last_credit_pull_d", StringType(), True),
    StructField("last_fico_range_high", DoubleType(), True),
    StructField("last_fico_range_low", DoubleType(), True),
    StructField("collections_12_mths_ex_med", DoubleType(), True),
    StructField("mths_since_last_major_derog", DoubleType(), True),
    StructField("policy_code", DoubleType(), True),
    StructField("application_type", StringType(), True),
    StructField("annual_inc_joint", DoubleType(), True),
    StructField("dti_joint", DoubleType(), True),
    StructField("verification_status_joint", StringType(), True),
    StructField("acc_now_delinq", DoubleType(), True),
    StructField("tot_coll_amt", DoubleType(), True),
    StructField("tot_cur_bal", DoubleType(), True),
    StructField("open_acc_6m", DoubleType(), True),
    StructField("open_act_il", DoubleType(), True),
    StructField("open_il_12m", DoubleType(), True),
    StructField("open_il_24m", DoubleType(), True),
    StructField("mths_since_rcnt_il", DoubleType(), True),
    StructField("total_bal_il", DoubleType(), True),
    StructField("il_util", DoubleType(), True),
    StructField("open_rv_12m", DoubleType(), True),
    StructField("open_rv_24m", DoubleType(), True),
    StructField("max_bal_bc", DoubleType(), True),
    StructField("all_util", DoubleType(), True),
    StructField("total_rev_hi_lim", DoubleType(), True),
    StructField("inq_fi", DoubleType(), True),
    StructField("total_cu_tl", DoubleType(), True),
    StructField("inq_last_12m", DoubleType(), True),
    StructField("acc_open_past_24mths", DoubleType(), True),
    StructField("avg_cur_bal", DoubleType(), True),
    StructField("bc_open_to_buy", DoubleType(), True),
    StructField("bc_util", DoubleType(), True),
    StructField("chargeoff_within_12_mths", DoubleType(), True),
    StructField("delinq_amnt", DoubleType(), True),
    StructField("mo_sin_old_il_acct", DoubleType(), True),
    StructField("mo_sin_old_rev_tl_op", DoubleType(), True),
    StructField("mo_sin_rcnt_rev_tl_op", DoubleType(), True),
    StructField("mo_sin_rcnt_tl", DoubleType(), True),
    StructField("mort_acc", DoubleType(), True),
    StructField("mths_since_recent_bc", DoubleType(), True),
    StructField("mths_since_recent_bc_dlq", DoubleType(), True),
    StructField("mths_since_recent_inq", DoubleType(), True),
    StructField("mths_since_recent_revol_delinq", DoubleType(), True),
    StructField("num_accts_ever_120_pd", DoubleType(), True),
    StructField("num_actv_bc_tl", DoubleType(), True),
    StructField("num_actv_rev_tl", DoubleType(), True),
    StructField("num_bc_sats", DoubleType(), True),
    StructField("num_bc_tl", DoubleType(), True),
    StructField("num_il_tl", DoubleType(), True),
    StructField("num_op_rev_tl", DoubleType(), True),
    StructField("num_rev_accts", DoubleType(), True),
    StructField("num_rev_tl_bal_gt_0", DoubleType(), True),
    StructField("num_sats", DoubleType(), True),
    StructField("num_tl_120dpd_2m", DoubleType(), True),
    StructField("num_tl_30dpd", DoubleType(), True),
    StructField("num_tl_90g_dpd_24m", DoubleType(), True),
    StructField("num_tl_op_past_12m", DoubleType(), True),
    StructField("pct_tl_nvr_dlq", DoubleType(), True),
    StructField("percent_bc_gt_75", DoubleType(), True),
    StructField("pub_rec_bankruptcies", DoubleType(), True),
    StructField("tax_liens", DoubleType(), True),
    StructField("tot_hi_cred_lim", DoubleType(), True),
    StructField("total_bal_ex_mort", DoubleType(), True),
    StructField("total_bc_limit", DoubleType(), True),
    StructField("total_il_high_credit_limit", DoubleType(), True),
    StructField("revol_bal_joint", DoubleType(), True),
    StructField("sec_app_fico_range_low", DoubleType(), True),
    StructField("sec_app_fico_range_high", DoubleType(), True),
    StructField("sec_app_earliest_cr_line", StringType(), True),
    StructField("sec_app_inq_last_6mths", DoubleType(), True),
    StructField("sec_app_mort_acc", DoubleType(), True),
    StructField("sec_app_open_acc", DoubleType(), True),
    StructField("sec_app_revol_util", DoubleType(), True),
    StructField("sec_app_open_act_il", DoubleType(), True),
    StructField("sec_app_num_rev_accts", DoubleType(), True),
    StructField("sec_app_chargeoff_within_12_mths", DoubleType(), True),
    StructField("sec_app_collections_12_mths_ex_med", DoubleType(), True),
    StructField("hardship_flag", StringType(), True),
    StructField("hardship_type", StringType(), True),
    StructField("hardship_reason", StringType(), True),
    StructField("hardship_status", StringType(), True),
    StructField("deferral_term", DoubleType(), True),
    StructField("hardship_amount", DoubleType(), True),
    StructField("hardship_start_date", StringType(), True),
    StructField("hardship_end_date", StringType(), True),
    StructField("payment_plan_start_date", StringType(), True),
    StructField("hardship_length", DoubleType(), True),
    StructField("hardship_dpd", DoubleType(), True),
    StructField("hardship_loan_status", StringType(), True),
    StructField("orig_projected_additional_accrued_interest", DoubleType(), True),
    StructField("hardship_payoff_balance_amount", DoubleType(), True),
    StructField("hardship_last_payment_amount", DoubleType(), True),
    StructField("debt_settlement_flag", StringType(), True),
])


column_mapping = {
    "grade": "loan_grade",
    "sub_grade": "loan_subgrade",
    "term": "loan_term",
    "purpose": "loan_purpose",
    "initial_list_status": "initial_list_status",
    "out_prncp": "out_principal",
    "application_type": "application_type",
    "debt_settlement_flag": "debt_settlement_flag",
    "id":"LOANPRODUCT_BK"
}

# Read from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "loan_data") \
    .option("kafka.group.id", 'LOANPRODUCT_consumer') \
    .option("startingOffsets", "earliest").load()

# Extract value and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*")
#####
# transformations
df_transformed = df_parsed \
    .fillna({"emp_title": "self_employed"}) \
    .withColumn("int_rate_clean", regexp_replace("int_rate", "%", "").cast("double")) \
    .withColumn("emp_length_clean",
        when(col("emp_length") == "10+ years", "10")
        .when(col("emp_length") == "< 1 year", "0")
        .when(col("emp_length").isNull(), "0")
        .otherwise(regexp_replace("emp_length", "\\D", ""))
    ) \
    .withColumn("high_income",
        when(col("annual_inc") > 100000, "yes").otherwise("no")
    ) \
    .select(
         "BORROWER_KEY_PK_SK","grade","sub_grade","term","purpose","initial_list_status","out_prncp",
        "application_type","debt_settlement_flag","id"
    )



# Clean term if 
if "term" in df_transformed.columns:
    df_transformed = df_transformed.withColumn("term", regexp_replace("term", " months", ""))

loanProduct_columns = ["grade","sub_grade","term","purpose","initial_list_status","out_prncp",
                       "application_type","debt_settlement_flag","id"]

df_transformed =df_transformed.select(loanProduct_columns)


# Rename columns in the DataFrame according to the column mapping
for source_col, dest_col in column_mapping.items():
    if source_col in df_transformed.columns:
        df_transformed = df_transformed.withColumnRenamed(source_col, dest_col)
        

# Generate a unique ID using hash of all columns
df_transformed = df_transformed.withColumn(
    "LOAN_PRODUCT_KEY_PK",
    F.abs(F.hash(F.concat_ws("|", *df_transformed.columns)))  
)

loanProduct_columns = ["LOAN_PRODUCT_KEY_PK","loan_grade","loan_subgrade","loan_term","loan_purpose","initial_list_status","out_principal",
                       "application_type","debt_settlement_flag","LOANPRODUCT_BK"]


df_transformed = df_transformed.select(loanProduct_columns)

# Output to console (or you can write to HDFS, database, etc.)

def write_to_snowflake(batch_df, batch_id):
    sfOptions = {
        "sfURL": "https://WOA97553.east-us-2.azure.snowflakecomputing.com",
        "sfUser": "hussien1",
        "sfPassword": "@Hussien123456",
        "sfDatabase": "Loan_DB",
        "sfSchema": "loan_SCHEMA_STREAMING",
        "sfWarehouse": "loan_Warehouse"  
    }
    batch_df.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "DIM_LOANPRODUCT_STREAMING ") \
        .mode("append") \
        .save()


# Apply the foreachBatch operation to write to Snowflake
query = df_transformed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_snowflake) \
    .start()

query.awaitTermination()
"""




query = df_transformed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False).start()

query.awaitTermination()

"""


