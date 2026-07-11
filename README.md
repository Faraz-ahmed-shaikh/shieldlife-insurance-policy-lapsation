Bro is this github readme good ?: 
# 🛡️ ShieldLife Insurance — Policy Lapse & Retention Analysis

An end-to-end Insurance Analytics case study built using PySpark, Spark SQL, Tableau, and Databricks to investigate policy lapsation, customer retention, and financial leakage for a fictional Indian insurance company — ShieldLife Insurance.

---

## 📌 Problem Statement

ShieldLife Insurance was losing customers and revenue due to policy lapsation and cancellation. The company had no visibility into **which customers were at risk of lapsing** and **which channels and products were driving the most losses** — making timely intervention impossible.

---

## 🎯 Project Goal

Analyze insurance data at the policy, customer, and channel level to:
- Measure portfolio health using business KPIs
- Identify operational drivers behind policy lapsation and poor retention
- Detect high-risk customers, channels, and products
- Deliver actionable recommendations for retention, operations, and finance teams

---

## 🔢 Key Results

| KPI | Value |
|-----|-------|
| Persistency Ratio | **48.10%** |
| Policy Renewal Rate | **72.27%** |
| Customer Renewal Rate | **54.12%** |
| Total Premium Received | **₹2,997 Crores** |
| Unrealised Premium Lost | **₹2,721.62 Crores** |
| Acquisition Cost at Risk | **₹24.64 Crores** |

---

## 💡 Top Findings

- **Agent Network** sells 56% of all policies but drives **84.3% of total acquisition cost loss**
- **Lapse rates are flat across all demographics** — age, income, occupation show no variation — showing that lapsation is primarily driven by operational factors rather than customer demographics.
- Only **7.9% of policies lapse in the first year** — customers disengage mid-lifecycle, not immediately
- **26.88% of lapses happen after the grace period** — a clear intervention window that is currently being missed
- **Digital channel** has the highest persistency ratio with the lowest acquisition cost

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Python (Faker, NumPy, Pandas) | Synthetic dataset generation |
| Databricks | Cloud analytics platform |
| PySpark | Data cleaning & feature engineering |
| Spark SQL | KPI analysis & segmentation |
| Tableau | Interactive dashboards |

---

## 📊 Dashboards

Three interactive Tableau dashboards were built to communicate findings across different audiences:

**1. Executive Overview**
Answers: *How healthy is ShieldLife's overall insurance portfolio?*
Covers portfolio-level KPIs (Persistency Ratio, Renewal Rates, Total Premium Received), Premium Composition (Collected vs Unrealised vs Remaining), Policy Status Distribution, Customer Status Distribution, and Acquisition Cost Loss by Channel.

**2. Retention & Behaviour Analysis**
Answers: *Who is leaving and what patterns exist?*
Covers Lapse Rate by Customer Segment (switchable between Age Group, Occupation, Income Group), Lapse Rate by Policy Segment (switchable between Product, Channel, Premium Bucket, Tenure Bucket), Early Default Distribution, and Product Performance (Persistency Ratio vs Lapse Rate by product).

**3. Financial & Channel Performance**
Answers: *Where is the company losing money?*
Covers Acquisition Cost Spent by Channel, Unrealised Premium by Product, Channel Comparison (Policies Sold vs Persistency Ratio vs Acquisition Cost Loss side by side), and Expected Premium by Product.

---

## 📁 Project Structure

```
├── dashboards/         # Tableau dashboard screenshots & workbook
├── data/               # Data note (raw data not included)
├── notebooks/          # 5 Databricks notebooks
│   ├── 01_data_exploration
│   ├── 02_data_cleaning
│   ├── 03_data_validation
│   ├── 04_feature_engineering
│   └── 05_analysis
├── reports/            # Executive summary PDF
└── source_file/        # A backup if Notebooks failed to open 
```

---

## 📋 Dataset Overview

Synthetic dataset generated to replicate real-world Indian insurance portfolio patterns.
> Total Records: ~3.9 Million

| Table | Type | Rows |
|-------|------|------|
| fct_premium_payments | Fact | ~3.2M |
| fct_policy_events | Fact | ~473K |
| dim_customers | Dimension | 100K |
| dim_policies | Dimension | 148K |
| dim_products | Dimension | 4 |
| dim_channels | Dimension | 3 |
| dim_agents | Dimension | 800 |
| dim_date | Dimension | 3,103 |

---

## 🔍 Methodology

```
Stakeholder Simulation → Problem Statement → KPI Framework →
Data Requirements → Data Generation → PySpark Cleaning →
Feature Engineering → Spark SQL Analysis → Insights →
Tableau Dashboards → Root Cause Analysis → Recommendations →
Documentation
```

---

## 📝 Business Recommendations

1. **Build a customer engagement system** — automated reminders + dedicated retention team for grace period customers
2. **Restructure agent commissions** — 50% at sale, 25% at 6 months, 25% at 12 months persistency
3. **Expand Digital Sales channel** — highest persistency, lowest acquisition cost
4. **Review ULIP & Endowment products** — highest lapse rates, need pricing and communication improvements

---

## 📄 Report

Full executive summary available in [`reports/`](reports/shieldlife_executive_summary.pdf)

---

*Synthetic data project built for portfolio and learning purposes. All figures are derived from the ShieldLife Insurance analytical dataset (2018–2026).*
