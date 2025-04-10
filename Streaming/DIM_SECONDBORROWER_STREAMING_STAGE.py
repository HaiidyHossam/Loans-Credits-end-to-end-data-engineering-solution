from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("LoanDataStreaming") \
    .master("local[*]") \
    .config("spark.jars", 
        "/home/haidy/kafka-spark-project/jars/spark-sql-kafka-0-10_2.12-3.3.2.jar,"
        "/home/haidy/kafka-spark-project/jars/kafka-clients-3.3.2.jar,"
        "/home/haidy/kafka-spark-project/jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar,"
        "/home/haidy/kafka-spark-project/jars/commons-pool2-2.11.1.jar,"
        "/home/haidy/kafka-spark-project/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar,"
        "/home/haidy/kafka-spark-project/jars/snowflake-jdbc-3.13.29.jar,"
        "/home/haidy/kafka-spark-project/jars/jackson-databind-2.12.7.jar,"
        "/home/haidy/kafka-spark-project/jars/jackson-core-2.12.7.jar,"
        "/home/haidy/kafka-spark-project/jars/jackson-annotations-2.12.7.jar"
    ) \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

# 2. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loan_topic_v2") \
    .option("startingOffsets", "earliest") \
    .load()

loan_schema = StructType([
    StructField("BORROWER_KEY_PK_SK", IntegerType(), True),
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
    StructField("full_state_name", StringType(), True),
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

])
# 4. Parse JSON from Kafka
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
df = json_df.select(from_json(col("json_value"), loan_schema).alias("data")).select("data.*")

# 5. Clean & transform
df = df.replace("NaN", None).replace("", None)

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



df = df.withColumnRenamed("id", "loanproduct_bk")


final_df = df.select("BORROWER_KEY_PK_SK", "application_type", "annual_inc_joint", "dti_joint", 
                     "sec_app_fico_range_avg", "sec_app_earliest_cr_line", "sec_app_inq_last_6mths", 
                     "sec_app_mort_acc", "sec_app_open_acc", "sec_app_revol_util", "loanproduct_bk"
)

final_df = final_df.withColumnRenamed("BORROWER_KEY_PK_SK", "SEC_BORROWER_KEY_PK_SK")

final_df = final_df.filter(col("application_type") == "Joint App")



# 7. Write to Snowflake
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
            .option("dbtable", "DIM_SECONDBORROWER_STREAMING_STAGE") \
            .mode("append") \
            .save()
            
        print(f"Writing batch {batch_id}, count = {batch_df.count()}")

    except Exception as e:
        print(f" Error writing to Snowflake: {e}")

query = final_df.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .start()

query.awaitTermination()
