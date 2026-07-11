-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 05 — Spark SQL Analysis
-- MAGIC #### ShieldLife Insurance — Policy Lapse & Retention Analysis
-- MAGIC **Purpose:** Perform structured KPI analysis across 6 buckets — 
-- MAGIC Portfolio KPIs, Customer, Policy, Channel, Financial, and 
-- MAGIC Behavioural — using Spark SQL to answer ShieldLife's core 
-- MAGIC business questions on policy lapsation and retention.  
-- MAGIC **Input:** Feature-enriched Delta tables from Notebook 04  
-- MAGIC **Output:** KPI metrics, segmentation results, and analytical 
-- MAGIC insights feeding Tableau dashboards and executive summary  
-- MAGIC **Analysis Structure:** KPIs → Customer Bucket → Policy Bucket 
-- MAGIC → Channel Bucket → Financial Bucket → Behavioural Bucket 
-- MAGIC → Product Bucket  
-- MAGIC **Platform:** Databricks (Spark SQL)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating Temp View for Analysis

-- COMMAND ----------

-- MAGIC %python
-- MAGIC policies = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policies_data.delta/', header=True, inferSchema = True)
-- MAGIC customers = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_customers_data.delta/', inferSchema=True, header=True)
-- MAGIC fact_events = spark.read.format('delta').load('/Volumes/workspace/default/shieldlife_insurance_db/cleaned_data_for_analysis/cleaned_policy_event_data.delta/', inferSchema=True, header=True)
-- MAGIC
-- MAGIC policies.createOrReplaceTempView('policies')
-- MAGIC customers.createOrReplaceTempView('customers')
-- MAGIC fact_events.createOrReplaceTempView('events')

-- COMMAND ----------

select * from policies limit 5;

-- COMMAND ----------

select * from customers limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KPI Calculation

-- COMMAND ----------

-- i. Primary: Persistency Ratio = Policies active at end of period / Policies issued at start of period × 100
select 
    round((sum(case when latest_status =='active' then 1 else 0 end ) * 100.0) / count(policy_id), 2) as persistency_ratio
from policies;

-- COMMAND ----------

-- ii. Customer Renewal Rate 
select round((sum(case when is_renewed_customer then 1 else 0 end) * 100.0) / count(customer_id), 2) as customer_renewal_rate
from customers;

-- COMMAND ----------

-- iii. Policy Renewal Rate
select round(
    try_divide(count(distinct e.policy_id) * 100.0, 
    count(distinct p.policy_id)), 2
) as policy_renewal_rate
from policies p
left join events e 
    on p.policy_id = e.policy_id 
    and e.is_renewed_event = True
where p.expiry_date <= '2026-06-30'
and p.latest_status in ('active', 'matured', 'claimed')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer Bucket

-- COMMAND ----------

-- i. Total Customers
select count(customer_id) from customers;

-- COMMAND ----------

-- ii. Total Active Customers
select 
    sum(case when customer_latest_status = 'Active' then 1 else 0 end) as total_active_customer,
    round((sum(case when customer_latest_status = 'Active' then 1 else 0 end) * 100.0) / count(customer_id), 2) as active_customer_rate
from customers;

-- COMMAND ----------

--  iii. Total Lapsed Customer
select 
    sum(case when customer_latest_status = 'Lapsed' then 1 else 0 end) as total_lapsed_customer,
    round((sum(case when customer_latest_status = 'Lapsed' then 1 else 0 end) * 100.0) / count(customer_id), 2) as lapse_rate
from customers;

-- COMMAND ----------

-- iv. Total Renewed/Retained Customers (Customers who have renewed there policy)
select 
    sum(case when is_renewed_customer = True then 1 else 0 end) as total_renewed_customers,
    round((sum(case when is_renewed_customer = True then 1 else 0 end) * 100.0) / count(customer_id), 2) as customer_renewal_rate
from customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Policy Bucket 

-- COMMAND ----------

-- i. Total Policies Issued
select count(policy_id) from policies;

-- COMMAND ----------

-- ii. Total Policies Lapsed (Policies which has lapsed as latest status)
select 
    sum(case when latest_status = 'lapsed' then 1 else 0 end) as total_lapsed_policies,
    round((sum(case when latest_status = 'lapsed' then 1 else 0 end) * 100.0) / count(policy_id), 2) as lapse_rate
from policies;

-- COMMAND ----------

-- iii. Total Polices Cancelled
select 
    sum(case when latest_status = 'cancelled' then 1 else 0 end) as total_cancelled_policies,
    round((sum(case when latest_status = 'cancelled' then 1 else 0 end) * 100.0) / count(policy_id), 2) as cancellation_rate
from policies;

-- COMMAND ----------

-- iv.	Total Polices Renewed
select count(distinct policy_id) as total_renewed_policies
from events
where is_renewed_event = True

-- COMMAND ----------

-- v.	Total Polices Claimed
select 
    sum(case when latest_status = 'claimed' then 1 else 0 end) as total_claimed_policies,
    round((sum(case when latest_status = 'claimed' then 1 else 0 end) * 100.0) / count(policy_id), 2) as claim_rate
from policies;

-- COMMAND ----------

-- vi.	Avg Premium Value
select round(avg(policy_premium),2) as avg_premium_value
from policies
where is_unreliable_premium = false;

-- COMMAND ----------

-- vii.	Avg Policy Tenure
select round(avg(policy_tenure),2) as avg_policy_tenure
from policies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Channel Bucket

-- COMMAND ----------

-- i.	Top Policy Selling Channel
select channel_id, count(policy_id) as total_policies_sold from policies group by channel_id; 

-- COMMAND ----------

-- ii.	Highest and Lowset Persistency Ratio Channel
select channel_id,
     round((sum(case when latest_status = 'active' then 1 else 0 end)*100.0)/count(policy_id),2) as persistency_ratio
from policies group by channel_id;

-- COMMAND ----------

-- Channel distribution for leakage amount and contribution by channel 
select 
    channel_id,
    sum(case when latest_status = 'lapsed' then cost_spent else 0 end) as leakage_amount,
    round(sum(case when latest_status = 'lapsed' then cost_spent else 0 end) * 100.0 / 246437863.08, 2) as per_contribution_to_total_leak
from policies
group by 1
order by leakage_amount desc;


-- COMMAND ----------

select 
channel_id, 
sum(case when latest_status = 'lapsed' then cost_spent else 0 end) as total_acquisition_at_risk, 
round((sum(case when latest_status = 'lapsed' then cost_spent else 0 end)*100.0)/(select sum(cost_spent))),2) 
from policies
group by channel_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Financial Bucket 

-- COMMAND ----------

-- i.	Total Premium Received
select sum(total_premium_received) as total_premium_received 
from policies;

-- COMMAND ----------

-- ii.	Total Acquisition Cost Spent & At Risk
select 
    sum(cost_spent) as total_acquisition_cost_spent,
    sum(case when latest_status = 'lapsed' then cost_spent else 0 end) as total_acquisition_at_risk,
    round((sum(case when latest_status = 'lapsed' then cost_spent else 0 end)*100.0)/sum(cost_spent),2) total_acquisition_per_at_risk
from policies; 

-- COMMAND ----------

-- iii.	Total Lapsed Amount (Money we received before customer lapsing)
select sum(total_premium_received) as money_received_before_lapse
 from policies where latest_status = 'lapsed'

-- COMMAND ----------

-- iv.	Total Lifetime Value Loss
select sum(unrealised_premium_value) as total_ltv_loss
from policies

-- COMMAND ----------

-- v.	Avg Customer Excpected Lifetime Value
select avg(expected_lifetime_value) as expected_customer_lifetime_value from customers  

-- COMMAND ----------

-- vi.	Lapse Rate by Premium Payment Frequency
select payment_frequency, 
    sum(case when latest_status = 'lapsed' then 1 else 0 end) total_policies_lapsed,
    round((sum(case when latest_status = 'lapsed' then 1 else 0 end) * 100.0) / count(policy_id), 2) as lapsed_rate
from policies 
group by payment_frequency

-- COMMAND ----------

-- vii. Total Excpected Premium 
select sum(expected_premium_value) as total_expected_premium_value from policies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Behaviour Bucket

-- COMMAND ----------

-- i. first premium default / early lapse rate (within 12 months)
select 
    sum(case when latest_status = 'lapsed' and is_early_defaulted = true then 1 else 0 end) as total_early_lapsed,
    round(sum(case when latest_status = 'lapsed' and is_early_defaulted = true then 1 else 0 end) * 100.0 / count(policy_id), 2) as early_lapse_rate
from policies;

-- COMMAND ----------

-- ii. lapse rate by age
select 
    c.age_bucket,
    count(case when p.latest_status = 'lapsed' then 1 end) as total_lapsed,
    round(count(case when p.latest_status = 'lapsed' then 1 end) * 100.0 / count(p.policy_id), 2) as lapse_rate_by_age
from policies p
join customers c on p.customer_id = c.customer_id
group by c.age_bucket;

-- COMMAND ----------

-- iii. lapse rate by profession
select 
    c.customer_occupation,
    count(case when p.latest_status = 'lapsed' then 1 end) as total_lapsed,
    round(count(case when p.latest_status = 'lapsed' then 1 end) * 100.0 / count(p.policy_id), 2) as lapse_rate_by_profession
from policies p
join customers c on p.customer_id = c.customer_id
group by c.customer_occupation;

-- COMMAND ----------

-- iv. lapse rate after grace period
select 
    count(case when latest_status = 'lapsed' and is_lapsed_after_grace = true then 1 end) as total_lapsed_after_grace,
    round(count(case when latest_status = 'lapsed' and is_lapsed_after_grace = true then 1 end) * 100.0 / count(policy_id), 2) as lapse_rate_after_grace
from policies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Product Bucket 

-- COMMAND ----------

-- i. product with high lapse rate
select 
    product_id,
    count(case when latest_status = 'lapsed' then 1 end) as total_lapsed,
    round(count(case when latest_status = 'lapsed' then 1 end) * 100.0 / count(policy_id), 2) as product_lapse_rate
from policies
group by product_id
order by product_lapse_rate desc;

-- COMMAND ----------

-- ii. lapse rate by tenure bucket
select 
    tenure_bucket,
    count(case when latest_status = 'lapsed' then 1 end) as total_lapsed,
    round(count(case when latest_status = 'lapsed' then 1 end) * 100.0 / count(policy_id), 2) as lapse_rate_by_tenure
from policies
group by tenure_bucket;

-- COMMAND ----------

-- iii. lapse rate by premium price
select 
    premium_bucket,
    count(case when latest_status = 'lapsed' then 1 end) as total_lapsed,
    round(count(case when latest_status = 'lapsed' then 1 end) * 100.0 / count(policy_id), 2) as lapse_rate_by_premium_price
from policies
group by premium_bucket;

-- COMMAND ----------

-- iv. lapse rate by product category
select 
    product_id as product_category,
    count(case when latest_status = 'lapsed' then 1 end) as total_lapsed,
    round(count(case when latest_status = 'lapsed' then 1 end) * 100.0 / count(policy_id), 2) as lapse_rate_by_category
from policies
group by product_id;

-- COMMAND ----------

select latest_status, round(count(*) * 100.0 / (select count(*) from policies),2)
from policies
group by latest_status;