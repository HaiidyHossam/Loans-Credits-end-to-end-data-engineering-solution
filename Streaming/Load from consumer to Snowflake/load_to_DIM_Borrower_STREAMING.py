
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql.functions import *
from pyspark.sql.window import Window


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, regexp_replace, isnan, lit, hash, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToSnowflakeBorrowerStreaming") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .getOrCreate()

# Define complete schema
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
    StructField("full_state_name", StringType(), True)
])

# Column mapping
column_mapping = {
    "annual_inc": "ANNUAL_INCOME",
    "emp_title": "EMPLOYMENT_TITLE",
    "emp_length": "EMPLOYMENT_LENGTH",
    "home_ownership": "HOMEOWNERSHIP",
    "earliest_cr_line": "EARLIEST_CREDIT_LINE_DATE",
    "verification_status": "VERIFICATION_STATUS",
    "zip_code": "ZIP_CODE",
    "addr_state": "ADDRESS_STATE",
    "full_state_name": "STATE",
    "delinq_2yrs": "DELINQ_2YEARS",
    "avg_fico_range": "AVG_FICO_RANGE",
    "open_acc": "OPEN_ACCOUNT",
    "pub_rec": "PUBLIC_RECORD",
    "total_acc": "TOTAL_ACCOUNT",
    "last_pymnt_d": "LAST_PAYMENT_DAY",
    "last_pymnt_amnt": "LAST_PAYMENT_AMOUNT",
    "next_pymnt_d": "NEXT_PAYMENT_DAY",
    "mths_since_last_major_derog": "MTHS_SINCE_LAST_MAJOR_DEROG",
    "open_il_24m": "ACTIVE_LOANS_LAST_24MONTH",
    "mort_acc": "MORTGAGE_ACCOUNT",
    "hardship_flag": "HARDSHIP_FLAG",
    "id": "LOANPRODUCT_BK"
}

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "loan_data") \
    .option("kafka.group.id", 'BORROWER_consumer') \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON data
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*")

# Data transformations
df_transformed = df_parsed.fillna({
    "emp_title": "self_employed",
    "emp_length": "0 years",
    "annual_inc": 0.0,
    "delinq_2yrs": 0.0,
    "pub_rec": 0.0
 }) \
    .withColumn("int_rate_clean", regexp_replace("int_rate", "%", "").cast("double")) \
    .withColumn("high_income", when(col("annual_inc") > 100000, "yes").otherwise("no")) \
    .withColumn("avg_fico_range", (col("fico_range_low") + col("fico_range_high")) / 2) \
    .withColumn("sec_app_fico_range_avg", 
               (col("sec_app_fico_range_low") + col("sec_app_fico_range_high")) / 2) \
    .withColumn("sec_app_fico_range_avg",
        when(col("sec_app_fico_range_avg").isNull() | isnan(col("sec_app_fico_range_avg")), 0.0)
        .otherwise(col("sec_app_fico_range_avg"))
    )
# Clean emp_length values
df_transformed = df_transformed.withColumn(
    "emp_length",
    when(col("emp_length").isNull(), "0")
    .when(col("emp_length") == "10+ years", "+10")
    .when(col("emp_length") == "< 1 year", "<1")
    .otherwise(regexp_replace(col("emp_length"), "[^0-9]", ""))
)



# State name mapping
state_mapping = {
    "AZ": "Arizona", "SC": "South Carolina", "LA": "Louisiana", "MN": "Minnesota",
    "NJ": "New Jersey", "DC": "District of Columbia", "OR": "Oregon", "VA": "Virginia",
    "RI": "Rhode Island", "WY": "Wyoming", "KY": "Kentucky", "NH": "New Hampshire",
    "MI": "Michigan", "NV": "Nevada", "WI": "Wisconsin", "ID": "Idaho", "CA": "California",
    "CT": "Connecticut", "NE": "Nebraska", "MT": "Montana", "NC": "North Carolina",
    "VT": "Vermont", "MD": "Maryland", "DE": "Delaware", "MO": "Missouri", "IL": "Illinois",
    "ME": "Maine", "WA": "Washington", "ND": "North Dakota", "MS": "Mississippi",
    "AL": "Alabama", "IN": "Indiana", "OH": "Ohio", "TN": "Tennessee", "NM": "New Mexico",
    "PA": "Pennsylvania", "SD": "South Dakota", "NY": "New York", "TX": "Texas",
    "GA": "Georgia", "MA": "Massachusetts", "KS": "Kansas", "FL": "Florida",
    "CO": "Colorado", "AK": "Alaska", "AR": "Arkansas", "OK": "Oklahoma", "UT": "Utah",
    "HI": "Hawaii"
}

# Create a map expression for state name mapping
state_expr = F.create_map(
    *[F.lit(x) for pair in state_mapping.items() for x in pair]
)

# Apply the state name mapping in your transformations
df_transformed = df_transformed.withColumn("state_name", state_expr.getItem(col("addr_state")))

# Select and rename columns
df_transformed = df_transformed.select(
    "annual_inc", "emp_title", "emp_length", "home_ownership", "earliest_cr_line", 
    "verification_status", "zip_code", "addr_state", "full_state_name", "delinq_2yrs",
    "avg_fico_range", "open_acc", "pub_rec", "total_acc", "last_pymnt_d", 
    "last_pymnt_amnt", "next_pymnt_d", "mths_since_last_major_derog", 
    "open_il_24m", "mort_acc", "hardship_flag", "id"
)

# Apply column renaming
for source_col, dest_col in column_mapping.items():
    if source_col in df_transformed.columns:
        df_transformed = df_transformed.withColumnRenamed(source_col, dest_col)

# Generate a unique ID using hash of all columns
df_transformed = df_transformed.withColumn(
    "BORROWER_KEY_PK_SK",
    F.abs(F.hash(F.concat_ws("|", *df_transformed.columns)))
)

# Final column selection
columns_of_borrowers = [
    "BORROWER_KEY_PK_SK", "ANNUAL_INCOME", "EMPLOYMENT_TITLE", "EMPLOYMENT_LENGTH", 
    "HOMEOWNERSHIP", "EARLIEST_CREDIT_LINE_DATE", "VERIFICATION_STATUS", 
    "ZIP_CODE", "ADDRESS_STATE", "STATE", "DELINQ_2YEARS", "AVG_FICO_RANGE", 
    "OPEN_ACCOUNT", "PUBLIC_RECORD", "TOTAL_ACCOUNT", "LAST_PAYMENT_DAY", 
    "LAST_PAYMENT_AMOUNT", "NEXT_PAYMENT_DAY", "MTHS_SINCE_LAST_MAJOR_DEROG", 
    "ACTIVE_LOANS_LAST_24MONTH", "MORTGAGE_ACCOUNT", "HARDSHIP_FLAG", "LOANPRODUCT_BK"
]

df_transformed = df_transformed.select(columns_of_borrowers)

# Snowflake write function
def write_to_snowflake(batch_df, batch_id):
    try:
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
            .option("dbtable", "DIM_BORROWER_STREAMING") \
            .mode("append") \
            .save()
            
    except Exception as e:
        print(f"Error writing batch {batch_id} to Snowflake: {str(e)}")

# Start streaming query
query = df_transformed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_snowflake) \
    .option("checkpointLocation", "/tmp/checkpoints/borrower_snowflake") \
    .start()

query.awaitTermination()

"""
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
    StructField("full_state_name", StringType(), True)
])


column_mapping = {
    "annual_inc": "ANNUAL_INCOME",
    "emp_title": "EMPLOYMENT_TITLE",
    "emp_length": "EMPLOYMENT_LENGTH",
    "home_ownership": "HOMEOWNERSHIP",
    "earliest_cr_line": "EARLIEST_CREDIT_LINE_DATE",
    "verification_status": "VERIFICATION_STATUS",
    "zip_code": "ZIP_CODE",
    "addr_state": "ADDRESS_STATE",
    "full_state_name": "STATE",
    "delinq_2yrs": "DELINQ_2YEARS",
    "avg_fico_range": "AVG_FICO_RANGE",
    "open_acc": "OPEN_ACCOUNT",   # If open_acc is duplicated, keep it only once
    "pub_rec": "PUBLIC_RECORD",   # If pub_rec is duplicated, keep it only once
    "total_acc": "TOTAL_ACCOUNT",
    "last_pymnt_d": "LAST_PAYMENT_DAY",
    "last_pymnt_amnt": "LAST_PAYMENT_AMOUNT",
    "next_pymnt_d": "NEXT_PAYMENT_DAY",
    "mths_since_last_major_derog": "MTHS_SINCE_LAST_MAJOR_DEROG",
    "open_il_24m": "ACTIVE_LOANS_LAST_24MONTH",
    "mort_acc": "MORTGAGE_ACCOUNT",
    "hardship_flag": "HARDSHIP_FLAG",
    "id":"LOANPRODUCT_BK"
}

# Read from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "loan_data") \
    .load()

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
    ) 

df_transformed = df_transformed.withColumn("avg_fico_range", (col("fico_range_low") + col("fico_range_high")) / 2)

# Compute sec_app_fico_range_avg and drop original
df_transformed = df_transformed.withColumn("sec_app_fico_range_avg", 
                   (col("sec_app_fico_range_low") + col("sec_app_fico_range_high")) / 2) \
       .drop("sec_app_fico_range_low", "sec_app_fico_range_high")

# Replace null or NaN in sec_app_fico_range_avg with 0.0
df_transformed = df_transformed.withColumn(
    "sec_app_fico_range_avg",
    when(col("sec_app_fico_range_avg").isNull() | isnan(col("sec_app_fico_range_avg")), 0.0)
    .otherwise(col("sec_app_fico_range_avg"))
)

#############################
from pyspark.sql import functions as F
 
state_abbreviations = {
    "AZ": "Arizona",
    "SC": "South Carolina",
    "LA": "Louisiana",
    "MN": "Minnesota",
    "NJ": "New Jersey",
    "DC": "District of Columbia",
    "OR": "Oregon",
    "VA": "Virginia",
    "RI": "Rhode Island",
    "WY": "Wyoming",
    "KY": "Kentucky",
    "NH": "New Hampshire",
    "MI": "Michigan",
    "NV": "Nevada",
    "WI": "Wisconsin",
    "ID": "Idaho",
    "CA": "California",
    "CT": "Connecticut",
    "NE": "Nebraska",
    "MT": "Montana",
    "NC": "North Carolina",
    "VT": "Vermont",
    "MD": "Maryland",
    "DE": "Delaware",
    "MO": "Missouri",
    "IL": "Illinois",
    "ME": "Maine",
    "WA": "Washington",
    "ND": "North Dakota",
    "MS": "Mississippi",
    "AL": "Alabama",
    "IN": "Indiana",
    "OH": "Ohio",
    "TN": "Tennessee",
    "NM": "New Mexico",
    "PA": "Pennsylvania",
    "SD": "South Dakota",
    "NY": "New York",
    "TX": "Texas",
    "GA": "Georgia",
    "MA": "Massachusetts",
    "KS": "Kansas",
    "FL": "Florida",
    "CO": "Colorado",
    "AK": "Alaska",
    "AR": "Arkansas",
    "OK": "Oklahoma",
    "UT": "Utah",
    "HI": "Hawaii"
}
 
df_transformed = df_transformed.withColumn(
    'full_state_name',
    F.when(df_transformed['addr_state'] == "AZ", "Arizona")
     .when(df_transformed['addr_state'] == "SC", "South Carolina")
     .when(df_transformed['addr_state'] == "LA", "Louisiana")
     .when(df_transformed['addr_state'] == "MN", "Minnesota")
     .when(df_transformed['addr_state'] == "NJ", "New Jersey")
     .when(df_transformed['addr_state'] == "DC", "District of Columbia")
     .when(df_transformed['addr_state'] == "OR", "Oregon")
     .when(df_transformed['addr_state'] == "VA", "Virginia")
     .when(df_transformed['addr_state'] == "RI", "Rhode Island")
     .when(df_transformed['addr_state'] == "WY", "Wyoming")
     .when(df_transformed['addr_state'] == "KY", "Kentucky")
     .when(df_transformed['addr_state'] == "NH", "New Hampshire")
     .when(df_transformed['addr_state'] == "MI", "Michigan")
     .when(df_transformed['addr_state'] == "NV", "Nevada")
     .when(df_transformed['addr_state'] == "WI", "Wisconsin")
     .when(df_transformed['addr_state'] == "ID", "Idaho")
     .when(df_transformed['addr_state'] == "CA", "California")
     .when(df_transformed['addr_state'] == "CT", "Connecticut")
     .when(df_transformed['addr_state'] == "NE", "Nebraska")
     .when(df_transformed['addr_state'] == "MT", "Montana")
     .when(df_transformed['addr_state'] == "NC", "North Carolina")
     .when(df_transformed['addr_state'] == "VT", "Vermont")
     .when(df_transformed['addr_state'] == "MD", "Maryland")
     .when(df_transformed['addr_state'] == "DE", "Delaware")
     .when(df_transformed['addr_state'] == "MO", "Missouri")
     .when(df_transformed['addr_state'] == "IL", "Illinois")
     .when(df_transformed['addr_state'] == "ME", "Maine")
     .when(df_transformed['addr_state'] == "WA", "Washington")
     .when(df_transformed['addr_state'] == "ND", "North Dakota")
     .when(df_transformed['addr_state'] == "MS", "Mississippi")
     .when(df_transformed['addr_state'] == "AL", "Alabama")
     .when(df_transformed['addr_state'] == "IN", "Indiana")
     .when(df_transformed['addr_state'] == "OH", "Ohio")
     .when(df_transformed['addr_state'] == "TN", "Tennessee")
     .when(df_transformed['addr_state'] == "NM", "New Mexico")
     .when(df_transformed['addr_state'] == "PA", "Pennsylvania")
     .when(df_transformed['addr_state'] == "SD", "South Dakota")
     .when(df_transformed['addr_state'] == "NY", "New York")
     .when(df_transformed['addr_state'] == "TX", "Texas")
     .when(df_transformed['addr_state'] == "GA", "Georgia")
     .when(df_transformed['addr_state'] == "MA", "Massachusetts")
     .when(df_transformed['addr_state'] == "KS", "Kansas")
     .when(df_transformed['addr_state'] == "FL", "Florida")
     .when(df_transformed['addr_state'] == "CO", "Colorado")
     .when(df_transformed['addr_state'] == "AK", "Alaska")
     .when(df_transformed['addr_state'] == "AR", "Arkansas")
     .when(df_transformed['addr_state'] == "OK", "Oklahoma")
     .when(df_transformed['addr_state'] == "UT", "Utah")
     .when(df_transformed['addr_state'] == "HI", "Hawaii")
     .otherwise(df_transformed['addr_state'])
    )
 
#######################
df_transformed = df_transformed.select(
         "annual_inc", "emp_title", "emp_length", "home_ownership","earliest_cr_line", "verification_status", 
        "zip_code", "addr_state",
    "full_state_name","delinq_2yrs","avg_fico_range", "open_acc",
    "pub_rec", "total_acc",  
     "last_pymnt_d", "last_pymnt_amnt", "next_pymnt_d",
    "mths_since_last_major_derog", "open_il_24m", "mort_acc", "hardship_flag","id"
    )

# Rename columns in the DataFrame according to the column mapping
for source_col, dest_col in column_mapping.items():
    if source_col in df_transformed.columns:
        df_transformed = df_transformed.withColumnRenamed(source_col, dest_col)


# Define the window spec for row numbering
windowSpec = Window.orderBy(F.lit(1))  # Constant value to ensure numbering starts from 1

# Add the BORROWER_KEY_PK_SK column with row numbers starting from 1
df_transformed = df_transformed.withColumn("BORROWER_KEY_PK_SK", F.row_number().over(windowSpec))

# List of columns to select (ensure that the column names match after renaming)
columns_of_borrowers = [
    "BORROWER_KEY_PK_SK", "ANNUAL_INCOME", "EMPLOYMENT_TITLE", "EMPLOYMENT_LENGTH", "HOMEOWNERSHIP",
    "EARLIEST_CREDIT_LINE_DATE", "VERIFICATION_STATUS", 
    "ZIP_CODE", "ADDRESS_STATE", "STATE", "DELINQ_2YEARS", "AVG_FICO_RANGE", "OPEN_ACCOUNT", "PUBLIC_RECORD", "TOTAL_ACCOUNT", 
    "LAST_PAYMENT_DAY", "LAST_PAYMENT_AMOUNT", "NEXT_PAYMENT_DAY", "MTHS_SINCE_LAST_MAJOR_DEROG", "ACTIVE_LOANS_LAST_24MONTH", "MORTGAGE_ACCOUNT", 
    "HARDSHIP_FLAG","LOANPRODUCT_BK"
]

df_transformed = df_transformed.select(columns_of_borrowers)

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
        .option("dbtable", "DIM_BORROWER_STREAMING") \
        .mode("append") \
        .save()


# Apply the foreachBatch operation to write to Snowflake
query = df_transformed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_snowflake) \
    .start()

query.awaitTermination()

"""
