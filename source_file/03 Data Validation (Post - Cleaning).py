# Databricks notebook source
# MAGIC %md
# MAGIC ### 03 — Data Validation (Post-Cleaning)
# MAGIC #### ShieldLife Insurance — Policy Lapse & Retention Analysis
# MAGIC **Purpose:** Validate all cleaned tables by verifying null 
# MAGIC removal, duplicate elimination, schema correctness, row counts, 
# MAGIC and logical consistency before feature engineering.  
# MAGIC **Input:** Cleaned Delta tables from Notebook 02  
# MAGIC **Output:** Validation report confirming data is analysis-ready  
# MAGIC **Platform:** Databricks (PySpark)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Customers Data

# COMMAND ----------

customers_df = spark.read.format('delta').load("/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_customers_data.delta/", inferSchema=True, header=True)
print(f"Rows: {customers_df.count()}, Columns: {len(customers_df.columns)}")

# COMMAND ----------

print(customers_df.columns)

# COMMAND ----------

display(customers_df)

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

display(customers_df.describe())

# COMMAND ----------

display(
    customers_df.select([
        count(
            when(
                col(c).isNull(),
                c
            )
        ).alias(c)

        for c in customers_df.columns
    ])
)

# COMMAND ----------

bef_row = customers_df.count()
cust_df = customers_df.dropDuplicates(["customer_id"])
print(f"Total Dups: {bef_row - cust_df.count()}")

# COMMAND ----------

display(customers_df.groupBy("customer_city").count())

# COMMAND ----------

display(customers_df.groupBy("customer_gender").count())

# COMMAND ----------

display(customers_df.groupBy("customer_income_bracket").count())

# COMMAND ----------

display(customers_df.groupBy("customer_occupation").count())

# COMMAND ----------

customers_df.filter(
    (col("customer_age")<18) | (col("customer_age")>100)
    ).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Agents Data

# COMMAND ----------

agents_df = spark.read.format('delta').load("/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_agents_data.delta/", inferSchema=True, header=True)

# COMMAND ----------

print(f"Rows: {agents_df.count()}, Columns: {len(agents_df.columns)}")

# COMMAND ----------

print(agents_df.columns)

# COMMAND ----------

display(agents_df)

# COMMAND ----------

agents_df.printSchema()

# COMMAND ----------

display(agents_df.describe())

# COMMAND ----------

display(
    agents_df.select([
        count(
            when(
                col(c).isNull(),
                c
            )
        ).alias(c)

        for c in agents_df.columns
    ])
)

# COMMAND ----------

bef_row = agents_df.count()
cust_df = agents_df.dropDuplicates(["agent_id"])
print(f"Total Dups: {bef_row - cust_df.count()}")

# COMMAND ----------

display(agents_df.groupBy("agent_city").count())

# COMMAND ----------

display(agents_df.groupBy("agent_region").count())

# COMMAND ----------

agents_df.groupBy("channel_id").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Channels Data

# COMMAND ----------

channels_df = spark.read.format('delta').load("/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_channels_data.delta/", inferSchema=True, header=True)

# COMMAND ----------

print(f"Rows: {channels_df.count()}, Columns: {len(channels_df.columns)}")

# COMMAND ----------

print(channels_df.columns)

# COMMAND ----------

display(channels_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Date Data

# COMMAND ----------

date_df = spark.read.format('delta').load("/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_date_data_(2018-26).delta/", inferSchema=True, header=True)

# COMMAND ----------

print(f"Rows: {date_df.count()}, Columns: {len(date_df.columns)}")

# COMMAND ----------


print(date_df.columns)

# COMMAND ----------

display(date_df)

# COMMAND ----------

date_df.printSchema()

# COMMAND ----------

display(date_df.describe())

# COMMAND ----------

from pyspark.sql.functions import *
display(
    date_df.select([
        count(
            when(
                col(c).isNull(),
                c
            )
        ).alias(c)

        for c in date_df.columns
    ])
)

# COMMAND ----------

bef_row = date_df.count()
backup_df = date_df.dropDuplicates(["date"])
print(f"Total Dups: {bef_row - backup_df.count()}")

# COMMAND ----------

display(date_df.groupBy("year").count().sort("year"))

# COMMAND ----------

display(date_df.filter(col("date")>'2026-06-30')) # No Future Dates!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Products Data 

# COMMAND ----------

products_df = spark.read.format('delta').load("/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_products_data.delta/", inferSchema=True, header=True)

# COMMAND ----------

print(f"Rows: {products_df.count()}, Columns: {len(products_df.columns)}")

# COMMAND ----------

print(products_df.columns)

# COMMAND ----------

display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Policies Data 

# COMMAND ----------

policies_df = spark.read.format('delta').load("/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policies_data.delta/", inferSchema=True, header=True)

# COMMAND ----------

print(f"Rows: {policies_df.count()}, Columns: {len(policies_df.columns)}")

# COMMAND ----------

print(policies_df.columns)

# COMMAND ----------

display(policies_df)

# COMMAND ----------

policies_df.printSchema()

# COMMAND ----------

display(policies_df.describe())

# COMMAND ----------

from pyspark.sql.functions import *
# Imputing 24 Nulls of Latest Status
display(
    policies_df.select([
        count(
            when(
                col(c).isNull(),
                c
            )
        ).alias(c)

        for c in policies_df.columns
    ])
)

# COMMAND ----------

bef_row = policies_df.count()
# Keep the variable name consistent
backup_df = policies_df.dropDuplicates(["policy_id"])
print(f"Total Dups: {bef_row - backup_df.count()}")

# COMMAND ----------

display(policies_df.groupBy("channel_id").count())

# COMMAND ----------

display(policies_df.groupBy("issue_year").count())

# COMMAND ----------

display(policies_df.groupBy("latest_status").count())

# COMMAND ----------

display(policies_df.groupBy("policy_tenure").count())

# COMMAND ----------

display(policies_df.filter((col("channel_id")==1) & (col("agent_id").isNull()))) # There are 250 policies which are sold by agent network, but agent_id is null

# COMMAND ----------

display(policies_df.filter((col("channel_id")!=1) & (col("agent_id").isNull()))) # There are no policies which arn't sold by agent network, and has a agent_id

# COMMAND ----------

policies_df.filter((col("is_loss_making_policy")==False) & (col("policy_value")<col("cost_spent"))).count() # There 4,826 policies whoes cost is more than its value

# COMMAND ----------

policies_df.filter((col("is_unreliable_premium")==False) & (col("policy_premium")<250)).count() # There are 592 policies which have premium less than 500 inlcuding 0s 

# COMMAND ----------

policies_df.filter((col("is_future_date")==False) & (col("issue_date")>'2026-06-30')).count() # There are 647 policies with future issue dates

# COMMAND ----------

policies_df.filter((col("is_invalid_date_range")==False) & (col("issue_date")>col("expiry_date"))).count() # There are 740 policies which issue dates is more than its expire

# COMMAND ----------

display(policies_df.filter(col("channel_id")==1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating fact_policy_events Data

# COMMAND ----------

fact_policy_events = spark.read.format('delta').load("/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policy_event_data.delta/", inferSchema=True, header=True)

# COMMAND ----------

print(f"Rows: {fact_policy_events.count()}, Columns: {len(fact_policy_events.columns)}")

# COMMAND ----------

print(fact_policy_events.columns)

# COMMAND ----------

display(fact_policy_events)

# COMMAND ----------

fact_policy_events.printSchema()

# COMMAND ----------

display(fact_policy_events.describe())

# COMMAND ----------

from pyspark.sql.functions import *
display(
    fact_policy_events.select([
        count(
            when(
                col(c).isNull(),
                c
            )
        ).alias(c)

        for c in fact_policy_events.columns
    ])
)

# COMMAND ----------

bef_row = fact_policy_events.count()
# Keep the variable name consistent
backup_df = fact_policy_events.dropDuplicates(["event_id"])
print(f"Total Dups: {bef_row - backup_df.count()}")

# COMMAND ----------

display(fact_policy_events.groupBy("channel_id").count())

# COMMAND ----------

display(fact_policy_events.groupBy("current_status").count())

# COMMAND ----------

display(fact_policy_events.groupBy("is_renewed_event").count())

# COMMAND ----------

display(fact_policy_events.groupBy("previous_status").count())

# COMMAND ----------

display(fact_policy_events.groupBy("product_id").count())

# COMMAND ----------

display(fact_policy_events.filter(col("is_renewed_event")==True).select("previous_status","current_status"))

# COMMAND ----------

# 1. Get the orphan event records
ghost_events_df = fact_policy_events.join(
    policies_df,
    on="policy_id", 
    how="anti"
)

# 2. Count how many ghost records exist
ghost_count = ghost_events_df.count()
print(f"Total ghost foreign key records: {ghost_count}")

# 3. View a sample of the ghost records
if ghost_count > 0:
    display(ghost_events_df.select("policy_id").distinct())

# COMMAND ----------

fact_policy_events.filter((col("is_first_event")==False)&(col("previous_status").isNull())).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring fact_premium_payments Data

# COMMAND ----------

fact_premium_payments = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_premiums_payment_data/', inferSchema=True, header=True)

# COMMAND ----------

print(f"Rows: {fact_premium_payments.count()}, Columns: {len(fact_premium_payments.columns)}")

# COMMAND ----------

print(fact_premium_payments.columns)

# COMMAND ----------

display(fact_premium_payments)

# COMMAND ----------

fact_premium_payments.printSchema()

# COMMAND ----------

display(fact_premium_payments.describe())

# COMMAND ----------

from pyspark.sql.functions import *
display(
    fact_premium_payments.select([
        count(
            when(
                col(c).isNull(),
                c
            )
        ).alias(c)

        for c in fact_premium_payments.columns
    ])
)

# COMMAND ----------

bef_row = fact_premium_payments.count()
# Keep the variable name consistent
backup_df = fact_premium_payments.dropDuplicates(["payment_id","policy_id","date_id"])
print(f"Total Dups: {bef_row - backup_df.count()}")

# COMMAND ----------

display(fact_premium_payments.groupBy("is_in_grace_period").count())

# COMMAND ----------

display(fact_premium_payments.groupBy("is_missed_premium").count())

# COMMAND ----------

display(fact_premium_payments.groupBy("payment_status").count())

# COMMAND ----------

# 1. Get the orphan event records
ghost_events_df = fact_premium_payments.join(
    policies_df,
    on="policy_id", 
    how="anti"
)

# 2. Count how many ghost records exist
ghost_count = ghost_events_df.count()
print(f"Total ghost foreign key records: {ghost_count}")

# 3. View a sample of the ghost records
if ghost_count > 0:
    display(ghost_events_df.select("policy_id").distinct())

# COMMAND ----------

fact_premium_payments.filter((col('is_invalid_amount')==False) & (fact_premium_payments.premium_amount <0)).count()