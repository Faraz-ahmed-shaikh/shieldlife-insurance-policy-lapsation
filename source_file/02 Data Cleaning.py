# Databricks notebook source
# MAGIC %md
# MAGIC ### 02 — Data Cleaning
# MAGIC #### ShieldLife Insurance — Policy Lapse & Retention Analysis
# MAGIC **Purpose:** Clean all 8 tables by handling nulls, duplicates, 
# MAGIC standardization issues, ghost foreign keys, invalid values, 
# MAGIC and logical anomalies identified during exploration.  
# MAGIC **Input:** Raw CSV files — all 8 tables  
# MAGIC **Output:** Cleaned Delta tables saved to Databricks Volume  
# MAGIC **Cleaning Order:** Dimensions first → fct_policy_events → 
# MAGIC dim_policies latest_status update → fct_premium_payments  
# MAGIC **Platform:** Databricks (PySpark)

# COMMAND ----------

# Importing essential function
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Customers Data

# COMMAND ----------

# Importing customers data
customers_df = spark.read.csv("/Volumes/workspace/default/shieldlife_insurance_db/original_data/customers_data.csv", inferSchema=True, header=True)
print(f"Customers Data Loaded with {customers_df.count()}")

# COMMAND ----------

# Standardization of 'customer_name' and 'customer_city'
customers_df = customers_df.withColumns({
    'customer_name': initcap(trim('customer_name')),
    'customer_city': initcap(trim('customer_city'))
})
customers_df.select("customer_city","customer_name").show(50)

# COMMAND ----------

# Filling Nulls of customer_age by median age of there occupation fall back to median age of whole table if any null left.
occupation_window = Window.partitionBy("customer_occupation")
median_age = percentile_approx("customer_age", 0.5, 10000).over(occupation_window)
customers_df = customers_df.withColumn("customer_age", coalesce(col("customer_age"),median_age))

# Filling Nulls of customer_occupation with 'Unknown Occupation'
customers_df = customers_df.fillna({'customer_occupation': 'Unknown Occupation'})

# Filling Nulls of customer_income_bracket with most repeated bracket for their occupation fallback to bracket of "3L-30L"
mode_bracket = mode('customer_income_bracket').over(occupation_window)
customers_df = customers_df.withColumn('customer_income_bracket', coalesce(col('customer_income_bracket'),mode_bracket,lit('3L-30L')))

# Validating Nulls
display(customers_df.select([count(when(col(c).isNull(), c) ).alias(c) for c in customers_df.columns ]))

# COMMAND ----------

# flagging il-logical age with invalid_age column i.e age <18, > 90. 
customers_df = customers_df.withColumn('is_invalid_age', when(
     (col("customer_age")<18) | (col("customer_age")>90), True)
        .otherwise(False)
    )

# COMMAND ----------

# Saving cleaned customers data as delta table for further analysis
customers_df.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_customers_data.delta')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Agents Data

# COMMAND ----------

# Importing Agents data
agents_df = spark.read.csv('/Volumes/workspace/default/shieldlife_insurance_db/original_data/agents_data.csv', inferSchema=True, header=True)
print(f"agents_df loaded with {agents_df.count()} rows")

# COMMAND ----------

display(agents_df)

# COMMAND ----------

# Standardization of agents column
agents_df = agents_df.withColumns({
    'agent_name': initcap(trim('agent_name')),
    'agent_region': initcap(trim('agent_region')),
    'agent_city': initcap(trim('agent_city'))
})

# Filling Nulls for channel_id
agents_df = agents_df.fillna({"channel_id":1})

display(agents_df)

# COMMAND ----------

# Saving Agents data as delta table for further analysis
agents_df.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_agents_data.delta')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Channels Data

# COMMAND ----------

# Importing Channels Data
channels_df = spark.read.csv('/Volumes/workspace/default/shieldlife_insurance_db/original_data/channels_data.csv', inferSchema=True, header=True)
print(f"channels_df loaded with {channels_df.count()} rows")

# COMMAND ----------

# As the Data is already small & cleaned there is no need for cleaning
display(channels_df)

# COMMAND ----------

# Saving data as delta table for further analysis
channels_df.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_channels_data.delta')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Date Data 

# COMMAND ----------

# Importing Date Data
date_df = spark.read.csv('/Volumes/workspace/default/shieldlife_insurance_db/original_data/date_data(2018-2026).csv', inferSchema=True, header=True)
print(f"Date data loaded with {date_df.count()} rows")

# COMMAND ----------

display(date_df)

# COMMAND ----------

# Casting data type from int to boolean
date_df = date_df.withColumn('is_weekday', col("is_weekday").cast('boolean'))
date_df.printSchema()

# COMMAND ----------

# Saving Date data as delta table for further analysis
date_df.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_date_data_(2018-26).delta')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Products Data

# COMMAND ----------

# Importing Products Data
products_df = spark.read.csv('/Volumes/workspace/default/shieldlife_insurance_db/original_data/products_data.csv', inferSchema=True, header=True)
print(f"products_df loaded with {products_df.count()} rows")

# COMMAND ----------

# As the Data is already small & cleaned there is no need for cleaning
display(products_df)

# COMMAND ----------

# Saving Products data as delta table for further analysis
products_df.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_products_data.delta')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Policies Data

# COMMAND ----------

# Importing Policies Data
policies_df = spark.read.csv('/Volumes/workspace/default/shieldlife_insurance_db/original_data/policies_data.csv', inferSchema=True, header=True)
print(f"policies_df loaded with {policies_df.count()} rows")

# COMMAND ----------

display(policies_df)

# COMMAND ----------

# Filling Nulls of agent_id by "Sold by Direct Sales" for channel_id 2, "Sold by Digital Sales" for channel_id 3, rest "Unknow Agent"
policies_df = policies_df.withColumn('agent_id', 
    coalesce('agent_id', 
        when(col('channel_id')==2,'Sold by Direct Sales')
        .when(col('channel_id')==3,'Sold by Digital Sales')
        .otherwise('Unknown Agent')
))

# Filling Nulls of cost_spent by median cost_spent for there channel_id
channel_window = Window.partitionBy('channel_id')
median_cost = percentile_approx('cost_spent', 0.5, 10000).over(channel_window)
policies_df = policies_df.withColumn('cost_spent', coalesce('cost_spent', median_cost))

display(policies_df.select([count(when(col(c).isNull(), c) ).alias(c) for c in policies_df.columns ]))

# COMMAND ----------

# Flag 5048 policies as 'is_loss_making_policy' whoes cost is more than its value
policies_df = policies_df.withColumn('is_loss_making_policy',when(col("cost_spent") > col("policy_value"), True).otherwise(False))
policies_df.filter(col('is_loss_making_policy')==True).count()

# COMMAND ----------

# Flag 592 policies to 'is_unrelaible_premium' which have premium less than 250 inlcuding 0s 
policies_df = policies_df.withColumn('is_unreliable_premium',when(col("policy_premium") < 250, True).otherwise(False))
policies_df.filter(col('is_unreliable_premium')==True).count()

# COMMAND ----------

# Flag 647 policies as 'is_future_date' with future issue dates
policies_df = policies_df.withColumn('is_future_date',when(col("issue_date") > '2026-06-30', True).otherwise(False))
policies_df.filter(col('is_future_date')==True).count()

# COMMAND ----------

# Flag 740 policies as 'is_invalid_date_range' which issue dates is more than its expire date
policies_df = policies_df.withColumn('is_invalid_date_range',when(col("issue_date") > col("expiry_date"), True).otherwise(False))
policies_df.filter(col('is_invalid_date_range')==True).count()

# COMMAND ----------

# rename is_current to is_current_policy
policies_df = policies_df.withColumnRenamed("is_current","is_current_policy")

# convert is_current_policy into boolean
policies_df = policies_df.withColumn("is_current_policy",col("is_current_policy").cast('boolean'))

policies_df.printSchema()

# COMMAND ----------

# replacing latest_status with current_status of fact_event. (Note this will be done after cleaning Events Data)
# 1. Define a window to find the most recent event for each policy
# Replace "event_timestamp" with your actual date/time or sequence column name
latest_event_window = Window.partitionBy("policy_id").orderBy(col("date_id").desc())

# 2. Get only the last event record per policy
latest_events_df = policy_event_df \
    .withColumn("rank", row_number().over(latest_event_window)) \
    .filter(col("rank") == 1) \
    .select(col("policy_id").alias("evt_policy_id"), col("current_status").alias("new_status"))

# 3. Join back to policies_df and update the status column
policies_df = policies_df.join(
    latest_events_df, 
    col("policy_id") == col("evt_policy_id"), 
    how="left"
)

# 4. Overwrite latest_status with the new_status if a recent event exists
# If no new event is found, it falls back onto its original status value
policies_df = policies_df.withColumn(
    "latest_status", 
    col("new_status").cast(policies_df.schema["latest_status"].dataType)
).drop("evt_policy_id", "new_status")

# Preview updated dimension table
display(policies_df)


# COMMAND ----------

# Null imputations on latest_status
policies_df = policies_df.fillna({'latest_status':'unknown status'})
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

# Saving Policies [dim] data as delta table for further analysis
policies_df.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policies_data.delta/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Policy Events Data

# COMMAND ----------

# Importing Policy Event Data
policy_event_df = spark.read.csv('/Volumes/workspace/default/shieldlife_insurance_db/original_data/policy_events_data.csv', inferSchema=True, header=True)
row_before = policy_event_df.count()
print(f"policy_event_df loaded with {row_before} rows")

# COMMAND ----------

display(policy_event_df)

# COMMAND ----------

policy_event_df.groupBy("current_status").count().show()

# COMMAND ----------

# Flagging rows which has prev status null as 'is_first_event' as it was the first status of Policies 
policy_event_df = policy_event_df.withColumn('is_first_event', when(col("previous_status").isNull(),True).otherwise(False))
policy_event_df.show(20)

# COMMAND ----------

# Removing ghost records
row_before = policy_event_df.count()
policy_event_df = policy_event_df.join(policies_df, on="policy_id", how="semi")
print(f"{row_before - policy_event_df.count()} Ghost records remove")

# COMMAND ----------

# Dropping Duplicates 
policy_event_df = policy_event_df.dropDuplicates(["event_id"])
print(f"{row_before - policy_event_df.count()} Duplicates Removed")

# COMMAND ----------

# renaming 'is_renewed' to 'is_renewed_event'
policy_event_df = policy_event_df.withColumnRenamed("is_renewed","is_renewed_event")
print(policy_event_df.columns)

# COMMAND ----------

# Saving Policy Event [fact] data as delta table for further analysis
policy_event_df.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policy_event_data.delta/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Premium Payments Data

# COMMAND ----------

# Importing Premium Payments Data
premium_payment_data = spark.read.csv(
    '/Volumes/workspace/default/shieldlife_insurance_db/original_data/premium_payments_data.csv', 
    header=True, 
    inferSchema=True
)
print(f"{premium_payment_data.count()} Rows Loaded")

# COMMAND ----------

# Coverting is_missed & is_in_grace_period into boolean.
premium_payment_data = premium_payment_data.withColumns({
    'is_missed' : col("is_missed").cast('boolean'),
    'is_in_grace_period' : col("is_missed").cast('boolean')
})
# Renaming Columns
premium_payment_data = premium_payment_data.withColumnRenamed("is_missed","is_missed_premium")

premium_payment_data.printSchema()

# COMMAND ----------

# Removing ghost records
row_before = premium_payment_data.count()
premium_payment_data = premium_payment_data.join(policies_df, on="policy_id", how="semi")
print(f"{row_before - premium_payment_data.count()} Ghost records remove")

# COMMAND ----------

# Dropping Dups based on "payment_id","policy_id","date_id"
rows_before = premium_payment_data.count()
premium_payment_data = premium_payment_data.dropDuplicates(["payment_id","policy_id","date_id"])
print(f"{rows_before - premium_payment_data.count()} Dups Rows Removed")

# COMMAND ----------

# Flagging Negative Payments with 'is_invalid_amount'
premium_payment_data = premium_payment_data.withColumn("is_invalid_amount", when(col("premium_amount")<0,True).otherwise(False))
premium_payment_data.filter(col('is_invalid_amount')==True).count()

# COMMAND ----------

# Saving premium payments data as delta table for further analysis
premium_payment_data.write.mode('overwrite').format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_premiums_payment_data')