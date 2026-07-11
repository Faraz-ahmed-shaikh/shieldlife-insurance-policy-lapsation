# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Engineering on Policies Data

# COMMAND ----------

# Importing Dim-Policies Data for Feature Engineering
policies_df = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policies_data.delta/', header=True, inferSchema = True)

# COMMAND ----------

# is_renewal_eligible - It will be True for those policies whiich are not lapsed & cancelled and which will expire in or before June 2026 are eligible
policies_df = policies_df.withColumn('is_renewal_eligible', when((col("latest_status").isin('active', 'claimed', 'matured')) & (col("expiry_date") <= '2026-06-30'), True).otherwise(False))
policies_df.filter(col('is_renewal_eligible')==True).count()

# COMMAND ----------

# total_premium_received - By using fact_premium_payments we have to sum all premium received for each policies where is_invalid_amount = False, then each sum will be shown in Policies Table
fact_premium_payments = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_premiums_payment_data/', header=True, inferSchema = True)

filtered_payments = fact_premium_payments.filter(col('is_invalid_amount')==False)
premium_calculation = filtered_payments.groupBy('policy_id').agg(sum('premium_amount').alias('total_premium_received'))

policies_df = policies_df.join(premium_calculation, on='policy_id', how="left")
policies_df = policies_df.withColumn("total_premium_received",coalesce(col("total_premium_received"), lit(0)))
policies_df.filter(col('total_premium_received').isNull()).count()

# COMMAND ----------

# expected_premium_value i.e policy_premium * tenure
policies_df = policies_df.withColumn('expected_premium_value', col("policy_premium")*col("policy_tenure"))

# unrealised_premium_value - (policy_value - total_premium_received) total_premium_received for calculation of Total Lifetime Value Loss 
policies_df = policies_df.withColumn('unrealised_premium_value', when(col("latest_status").isin('lapsed','cancelled'),col("expected_premium_value")-col("total_premium_received")).otherwise(0)) 

# This will be true for policies which has unrealised value i.e Lapsed and Cancelled 
policies_df = policies_df.withColumn(
    "has_unrealised_value",
    col("latest_status").isin("lapsed", "cancelled")
)

# is_negative_unrealised 
policies_df = policies_df.withColumn('is_negative_unrealised', when(col("unrealised_premium_value")<0,True).otherwise(False))

display(policies_df.describe('unrealised_premium_value'))
print(f"No. of Pol that has unrealised premium : {policies_df.filter(col('has_unrealised_value')==True).count()}")
print(f"No. of Pol that has negative unrealised bcz unspecified frequency : {policies_df.filter(col('is_negative_unrealised')==True).count()}")

# COMMAND ----------

# tenure_bucket - A bucket for Policy Tenure Short Term [1,2,3,4,5], Mid Term [10,15] and Long Term [20,25,30].
policies_df = policies_df.withColumn('tenure_bucket', when(col("policy_tenure")>=20,'Long Term (20-30 Yrs)').when(col("policy_tenure")>=10,'Mid Term (10-15 Yrs)').otherwise('Short Term  (1-5 Yrs)'))
policies_df.groupBy('tenure_bucket').count().show()

# COMMAND ----------

# feature engineering of is_early_default feature, to find are policies defaulting in the early stage of lifecycle
fact_events_data = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policy_event_data.delta/')
date_data = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_date_data_(2018-26).delta/')

lapsed_events = fact_events_data.filter(col('current_status') == 'lapsed')
lapsed_events = lapsed_events.join(date_data.select("date_id", col("date").alias('lapsed_date')), on='date_id', how="left")

policy_window = Window.partitionBy("policy_id")
lapsed_events = lapsed_events.withColumn("first_lapsed_date", min(col("lapsed_date")).over(policy_window))
cleaned_lapsed_events = lapsed_events.select("policy_id", "first_lapsed_date").distinct()

policies_df = policies_df.join(cleaned_lapsed_events, on='policy_id', how='left')

policies_df = policies_df.withColumn('month_between_lapsed', months_between(col('first_lapsed_date'),col("issue_date")))
policies_df = policies_df.withColumns({
    'is_early_defaulted': when((col("latest_status") == "lapsed") & (col("month_between_lapsed") <= 12),
        True).otherwise(False),
    'has_lapsed_date':when(col('first_lapsed_date').isNull(),False).otherwise(True)
})

display(policies_df)

# COMMAND ----------

# is_graced - True for policies which total grace time > 0
policies_df = policies_df.withColumn('is_graced',when(col("total_time_in_grace")>0,True).otherwise(False))
policies_df.filter(col('is_graced')==True).count()
policies_df.filter((col('is_graced')==True)&(col("total_time_in_grace")==0)).count()
policies_df.filter((col('is_graced')==False)&(col("total_time_in_grace")>0)).count()

# COMMAND ----------

# premium_bucket - Low: [< ₹5k], Medium: [₹5k–₹20k], High: [₹20k - ₹50k], Very High: [> ₹50k]
policies_df = policies_df.withColumn('premium_bucket', 
    when(col("policy_premium") > 50000,'Very High (> ₹50k)')
    .when(col("policy_premium") >= 20000,'High (₹20k - ₹50k)')
    .when(col("policy_premium") >= 5000,'Medium (₹5k - ₹19k)')
    .otherwise('Short Term  (< ₹5k)'))
policies_df.groupBy('premium_bucket').count().show()

# COMMAND ----------

# is_lapsed_after_grace — True for policies that went in_grace → lapsed
lapsed_after_grace_status = fact_events_data.filter((col('previous_status')=='in_grace')&(col('current_status')=='lapsed')).select("policy_id","previous_status","current_status").distinct()
policies_df = policies_df.join(lapsed_after_grace_status,on="policy_id",how="left")
policies_df = policies_df.withColumn('is_lapsed_after_grace',when((col('previous_status')=='in_grace')&(col('current_status')=='lapsed'),True).otherwise(False))
policies_df = policies_df.drop("previous_status","current_status")
policies_df.filter(col('is_lapsed_after_grace')==True).count()

# COMMAND ----------

# is_acquisition_at_risk — True for lapsed policies where cost_spent > total_premium_received
policies_df = policies_df.withColumn('is_acquisition_at_risk', when((col("latest_status")=='lapsed') & (col('cost_spent')>col("total_premium_received")),True).otherwise(False))
policies_df.filter(col('is_acquisition_at_risk')==True).count()

# COMMAND ----------

# payment_frequency - derived from fct_premium_payments, avg gap between payments per policy
payments_with_date = fact_premium_payments.join(date_data.select("date_id", col("date").alias("payment_date")),on="date_id", how="inner")

policy_payment_window = Window.partitionBy("policy_id").orderBy("payment_date")
payments_with_gap = payments_with_date.withColumn("prev_payment_date", lag("payment_date", 1).over(policy_payment_window)).withColumn("payment_gap_days", datediff("payment_date", "prev_payment_date"))

avg_gap_df = payments_with_gap.groupBy("policy_id").agg(percentile_approx('payment_gap_days',0.5).alias("avg_payment_gap"))

policies_df = policies_df.join(avg_gap_df,policies_df["policy_id"] == avg_gap_df["policy_id"],how="left")

policies_df = policies_df.withColumn(
    "payment_frequency",
    when((col("avg_payment_gap") >= 28) & (col("avg_payment_gap") <= 32), "Monthly")
    .when((col("avg_payment_gap") >= 85) & (col("avg_payment_gap") <= 95), "Quarterly")
    .when((col("avg_payment_gap") >= 175) & (col("avg_payment_gap") <= 190), "Semi Annual")
    .when((col("avg_payment_gap") >= 350) & (col("avg_payment_gap") <= 380), "Annual")
    .otherwise("Irregular")
).drop(avg_gap_df["policy_id"])

# Preview final classification breakdown
policies_df.groupBy("payment_frequency").count().show()

# COMMAND ----------

fact_events_data.filter((col('previous_status')=='in_grace')&(col('current_status')=='cancelled')).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Engineering on Customers Data

# COMMAND ----------

customers_df = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_customers_data.delta/', inferSchema=True, header=True)
fact_events_data = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policy_event_data.delta/', inferSchema=True, header=True)

# COMMAND ----------

# 1. customer_latest_status- It will have a latest status based on latest event for each policies
customer_window = Window.partitionBy(col('customer_id')).orderBy(col('date_id').desc())
customer_latest_status = fact_events_data.withColumn('row_num_by_occurrence', row_number().over(customer_window))
customer_latest_status = customer_latest_status.filter(col('row_num_by_occurrence')==1).drop('row_num_by_occurrence')
customer_latest_status = customer_latest_status.select(col("customer_id").alias('lookup_cust_id'), col("current_status").alias('customer_latest_status'))

customers_df = customers_df.join(customer_latest_status,customers_df['customer_id']==customer_latest_status['lookup_cust_id'], how='left').drop('lookup_cust_id')

customers_df = customers_df.withColumn('customer_latest_status', initcap(trim(col("customer_latest_status"))))
customers_df = customers_df.fillna({'customer_latest_status':'No Policy'})
customers_df.groupBy('customer_latest_status').count().show()

# COMMAND ----------

# 2. no_of_policies (Group policies by customer and count)
policy_count_df = policies_df.groupBy("customer_id").agg(count("policy_id").alias("no_of_policies"))
customers_df = customers_df.join(policy_count_df,customers_df["customer_id"] == policy_count_df["customer_id"],how="left").drop(policy_count_df["customer_id"])
customers_df = customers_df.fillna({"no_of_policies": 0})

# 3. Calculate has_renewed_event (Filter, isolate customer keys, and flag)
renewed_customers_df = fact_events_data.filter(col("is_renewed_event") == True).select(col("customer_id").alias("renewed_cust_id")).distinct()
customers_df = customers_df.join(renewed_customers_df,customers_df["customer_id"] == renewed_customers_df["renewed_cust_id"],how="left")
customers_df = customers_df.withColumn("is_renewed_customer", col("renewed_cust_id").isNotNull()).drop("renewed_cust_id")

# Preview the finalized dataframe
display(customers_df)


# COMMAND ----------

# 4. customers total_lifetime_value
ltv_df = policies_df.groupBy("customer_id").agg(sum("expected_premium_value").alias("expected_lifetime_value"))
customers_df = customers_df.join(ltv_df,customers_df["customer_id"] == ltv_df["customer_id"],how="left").drop(ltv_df["customer_id"])
customers_df = customers_df.fillna({"expected_lifetime_value": 0.0})

# 5. Categorise into age_bucket
customers_df = customers_df.withColumn(
    "age_bucket",
    when((col("customer_age") < 18) | (col("customer_age") > 90), "Unrealistic (<18 or >90)")
    .when((col("customer_age") >= 18) & (col("customer_age") <= 30), "Young (18–30)")
    .when((col("customer_age") >= 31) & (col("customer_age") <= 40), "Early Career (31–40)")
    .when((col("customer_age") >= 41) & (col("customer_age") <= 50), "Mid Career (41–50)")
    .when((col("customer_age") >= 51) & (col("customer_age") <= 65), "Senior (51–65)")
    .when(col("customer_age") > 65, "Elder (65+)")
    .otherwise("Unknown")
)

display(customers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Features, Reordering Columns and Saving Data

# COMMAND ----------

from pyspark.sql.functions import *

print("="*80)
print("POLICY FEATURES VALIDATION")
print("="*80)

# 1 Total Premium Received
print("\n1. total_premium_received")
policies_df.describe("total_premium_received").show()

# 2 Expected Premium Value
print("\n2. expected_premium_value")
policies_df.describe("expected_premium_value").show()

# 3 Unrealised Premium Value
print("\n3. unrealised_premium_value")
policies_df.describe("unrealised_premium_value").show()

# 4 Negative Unrealised
print("\n4. is_negative_unrealised")
policies_df.groupBy("is_negative_unrealised").count().show()

# 5 Has Unrealised Value
print("\n5. has_unrealised_value")
policies_df.groupBy("has_unrealised_value").count().show()

# 6 Early Defaulted
print("\n6. is_early_defaulted")
policies_df.groupBy("is_early_defaulted").count().show()

# 7 Has Lapsed Date
print("\n7. has_lapsed_date")
policies_df.groupBy("has_lapsed_date").count().show()

# 8 Lapsed After Grace
print("\n8. is_lapsed_after_grace")
policies_df.groupBy("is_lapsed_after_grace").count().show()

# 9 Acquisition At Risk
print("\n9. is_acquisition_at_risk")
policies_df.groupBy("is_acquisition_at_risk").count().show()

# 10 Payment Frequency
print("\n10. payment_frequency")
policies_df.groupBy("payment_frequency").count().show()

print("="*80)
print("CUSTOMER FEATURES VALIDATION")
print("="*80)

# 11 Customer Latest Status
print("\n11. customer_latest_status")
customers_df.groupBy("customer_latest_status").count().show()

# 12 Number of Policies
print("\n12. no_of_policies")
customers_df.describe("no_of_policies").show()

# 13 Renewed Customer
print("\n13. is_renewed_customer")
customers_df.groupBy("is_renewed_customer").count().show()

# 14 Expected Lifetime Value
print("\n14. expected_lifetime_value")
customers_df.describe("expected_lifetime_value").show()

# 15 Age Bucket
print("\n15. age_bucket")
customers_df.groupBy("age_bucket").count().show()

print("="*80)
print("NULL VALIDATION")
print("="*80)

policy_features = [
    "total_premium_received",
    "expected_premium_value",
    "unrealised_premium_value",
    "is_negative_unrealised",
    "has_unrealised_value",
    "is_early_defaulted",
    "has_lapsed_date",
    "is_lapsed_after_grace",
    "is_acquisition_at_risk",
    "payment_frequency"
]

customer_features = [
    "customer_latest_status",
    "no_of_policies",
    "is_renewed_customer",
    "expected_lifetime_value",
    "age_bucket"
]

print("\nPolicy Features Null Check")
policies_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in policy_features
]).show(truncate=False)

print("\nCustomer Features Null Check")
customers_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in customer_features
]).show(truncate=False)

print("="*80)
print("FEATURE ENGINEERING VALIDATION COMPLETED")
print("="*80)

# COMMAND ----------

print("\nBusiness Validation")

print("Negative Unrealised Policies:",
      policies_df.filter(col("is_negative_unrealised")).count())

print("Early Defaulted Policies:",
      policies_df.filter(col("is_early_defaulted")).count())

print("Lapsed After Grace:",
      policies_df.filter(col("is_lapsed_after_grace")).count())

print("Acquisition At Risk:",
      policies_df.filter(col("is_acquisition_at_risk")).count())

print("Customers With Renewals:",
      customers_df.filter(col("is_renewed_customer")).count())

# COMMAND ----------

# Drop helper columns
policies_df = policies_df.drop(
    "first_lapsed_date",
    "month_between_lapsed",
    "avg_payment_gap"
)

# Reorder columns
policies_df = policies_df.select(
    "policy_id",
    "policy_number",
    "customer_id",
    "product_id",
    "channel_id",
    "agent_id",
    "latest_status",
    "is_current_policy",
    "issue_date",
    "expiry_date",
    "issue_year",
    "policy_tenure",
    "tenure_bucket",
    "policy_value",
    "policy_premium",
    "premium_bucket",
    "cost_spent",
    "expected_premium_value",
    "total_premium_received",
    "unrealised_premium_value",
    "total_time_in_grace",
    "is_graced",
    "payment_frequency",
    "is_unreliable_premium",
    "is_future_date",
    "is_invalid_date_range",
    "has_unrealised_value",
    "is_negative_unrealised",
    "is_loss_making_policy",
    "is_renewal_eligible",
    "has_lapsed_date",
    "is_early_defaulted",
    "is_lapsed_after_grace",
    "is_acquisition_at_risk"
)

display(policies_df)

# COMMAND ----------

policies_df.write.mode('overwrite').option("mergeSchema", "true").format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policies_data.delta/')

# COMMAND ----------

# Reorder Customer Dimension
customers_df = customers_df.select(
    "customer_id",
    "customer_name",
    "customer_age",
    "age_bucket",
    "customer_gender",
    "customer_city",
    "customer_occupation",
    "customer_income_bracket",
    "customer_latest_status",
    "no_of_policies",
    "expected_lifetime_value",
    "is_renewed_customer",
    "is_invalid_age"
)

display(customers_df)

# COMMAND ----------

customers_df.write.mode('overwrite').option("mergeSchema", "true").format('delta').save('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_customers_data.delta')