from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ======================
# 1. Spark Session Setup
# ======================
spark = SparkSession.builder \
    .appName("LoanDataStreaming") \
    .master("local[*]") \
    .config("spark.jars", "/home/haidy/kafka-spark-project/jars/spark-sql-kafka-0-10_2.12-3.3.2.jar,"
                          "/home/haidy/kafka-spark-project/jars/kafka-clients-3.3.2.jar,"
                          "/home/haidy/kafka-spark-project/jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar,"
                          "/home/haidy/kafka-spark-project/jars/commons-pool2-2.11.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ======================
# 2. Read from Kafka
# ======================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loan_topic_v2") \
    .option("startingOffsets", "earliest") \
    .load()

# ======================
# 3. Define Schema
# ======================
loan_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("loan_amnt", DoubleType(), True),
    StructField("funded_amnt_inv", DoubleType(), True),
    StructField("int_rate", StringType(), True),
    StructField("installment", DoubleType(), True),
    StructField("loan_status", StringType(), True),
    StructField("dti", DoubleType(), True),
    StructField("revol_bal", DoubleType(), True),
    StructField("revol_util", StringType(), True),
    StructField("tot_cur_bal", DoubleType(), True),
    StructField("total_bal_il", DoubleType(), True),
    StructField("max_bal_bc", DoubleType(), True),
    StructField("delinq_amnt", DoubleType(), True),
    StructField("annual_inc", DoubleType(), True),
    StructField("emp_title", StringType(), True),
    StructField("emp_length", StringType(), True),
    StructField("home_ownership", StringType(), True),
    StructField("open_acc", DoubleType(), True),
    StructField("pub_rec", DoubleType(), True),
    StructField("delinq_2yrs", DoubleType(), True),
    StructField("earliest_cr_line", StringType(), True),
    StructField("fico_range_low", DoubleType(), True),
    StructField("fico_range_high", DoubleType(), True),
    StructField("total_acc", DoubleType(), True),
    StructField("verification_status", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("addr_state", StringType(), True),
    StructField("last_pymnt_d", StringType(), True),
    StructField("last_pymnt_amnt", DoubleType(), True),
    StructField("next_pymnt_d", StringType(), True),
    StructField("mths_since_last_major_derog", DoubleType(), True),
    StructField("open_il_24m", DoubleType(), True),
    StructField("mort_acc", DoubleType(), True),
    StructField("hardship_flag", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("sub_grade", StringType(), True),
    StructField("term", StringType(), True),
    StructField("purpose", StringType(), True),
    StructField("initial_list_status", StringType(), True),
    StructField("out_prncp", DoubleType(), True),
    StructField("application_type", StringType(), True),
    StructField("debt_settlement_flag", StringType(), True),
    StructField("hardship_type", StringType(), True),
    StructField("hardship_reason", StringType(), True),
    StructField("hardship_status", StringType(), True),
    StructField("hardship_amount", DoubleType(), True),
    StructField("hardship_start_date", StringType(), True),
    StructField("hardship_end_date", StringType(), True),
    StructField("hardship_length", DoubleType(), True),
    StructField("hardship_dpd", DoubleType(), True),
    StructField("hardship_loan_status", StringType(), True),
    StructField("hardship_payoff_balance_amount", DoubleType(), True),
    StructField("annual_inc_joint", DoubleType(), True),
    StructField("dti_joint", DoubleType(), True),
    StructField("sec_app_fico_range_low", DoubleType(), True),
    StructField("sec_app_fico_range_high", DoubleType(), True),
    StructField("sec_app_earliest_cr_line", StringType(), True),
    StructField("sec_app_inq_last_6mths", DoubleType(), True),
    StructField("sec_app_mort_acc", DoubleType(), True),
    StructField("sec_app_open_acc", DoubleType(), True),
    StructField("sec_app_revol_util", DoubleType(), True),
    StructField("issue_d", DateType(), True),
    StructField("full_state_name", StringType(), True)
])

  

# ======================
# 4. Parse JSON from Kafka
# ======================
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
df = json_df.select(from_json(col("json_value"), loan_schema).alias("data")).select("data.*")

# ======================
# 5. Clean raw string data
# ======================
# Replace "NaN" and empty string with null
df = df.replace("NaN", None).replace("", None)

# ======================
# 6. Cast columns to proper types
# ======================
df = df \
    .withColumn("loan_amnt", col("loan_amnt").cast("double")) \
    .withColumn("funded_amnt_inv", col("funded_amnt_inv").cast("double")) \
    .withColumn("installment", col("installment").cast("double")) \
    .withColumn("annual_inc", col("annual_inc").cast("double")) \
    .withColumn("dti", col("dti").cast("double")) \
    .withColumn("fico_range_low", col("fico_range_low").cast("double")) \
    .withColumn("fico_range_high", col("fico_range_high").cast("double")) \
    .withColumn("revol_util", col("revol_util").cast("double")) \
    .withColumn("last_pymnt_amnt", col("last_pymnt_amnt").cast("double")) \
    .withColumn("sec_app_fico_range_low", col("sec_app_fico_range_low").cast("double")) \
    .withColumn("sec_app_fico_range_high", col("sec_app_fico_range_high").cast("double")) \
    .withColumn("issue_d", to_date("issue_d", "yyyy-MM-dd"))

# ======================
# 7. Transformations and Null Handling
# ======================

# Fill missing emp_title with "self_employed"
df = df.fillna({"emp_title": "self_employed"})


# Clean emp_length values
df = df.withColumn(
    "emp_length",
    when(col("emp_length").isNull(), "0")
    .when(col("emp_length") == "10+ years", "+10")
    .when(col("emp_length") == "< 1 year", "<1")
    .otherwise(regexp_replace(col("emp_length"), "[^0-9]", ""))
)

# Handle missing dti using formula
df = df.withColumn(
    "dti",
    when(col("dti").isNull(), col("installment") / (col("annual_inc") / 12)).otherwise(col("dti"))
)

# Calculate avg_fico_range
df = df.withColumn("avg_fico_range", (col("fico_range_low") + col("fico_range_high")) / 2)



# Fill missing revol_util with estimated median
median_revol_util = 52.0
df = df.withColumn(
    "revol_util",
    when(col("revol_util").isNull(), median_revol_util).otherwise(col("revol_util"))
)

# Make last_pymnt_amnt absolute
df = df.withColumn("last_pymnt_amnt", abs(col("last_pymnt_amnt")))

# Fill missing dates
df = df.withColumn("next_pymnt_d", when(col("next_pymnt_d").isNull(), "finished").otherwise(col("next_pymnt_d")))
df = df.withColumn("last_pymnt_d", when(col("last_pymnt_d").isNull(), "first month").otherwise(col("last_pymnt_d")))

# Compute sec_app_fico_range_avg and drop original
df = df.withColumn("sec_app_fico_range_avg", 
                   (col("sec_app_fico_range_low") + col("sec_app_fico_range_high")) / 2) \
       .drop("sec_app_fico_range_low", "sec_app_fico_range_high")

# Replace null or NaN in sec_app_fico_range_avg with 0.0
df = df.withColumn(
    "sec_app_fico_range_avg",
    when(col("sec_app_fico_range_avg").isNull() | isnan(col("sec_app_fico_range_avg")), 0.0)
    .otherwise(col("sec_app_fico_range_avg"))
)



# Clean term if 
if "term" in df.columns:
    df = df.withColumn("term", regexp_replace("term", " months", ""))

# ======================
# 8. Output to Console
# ======================
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()


