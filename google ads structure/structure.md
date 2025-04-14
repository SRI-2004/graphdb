**📦 Google Ads Data Ingestion Plan (Neo4j Graph Schema)**

We are selecting a subset of 27 available Google Ads streams to build a structured Neo4j-based graph database. Below is a brief overview of which streams we will include, and the rationale behind them.

---

## ✅ Selected Tables Overview (Grouped by Hierarchy)

### 📌 Account-Level Tables

| Table            | Purpose             | Selected Fields                                       | Sync Mode             | Notes                  |
| ---------------- | ------------------- | ----------------------------------------------------- | --------------------- | ---------------------- |
| `customer`       | Ad account metadata | `customer.resource_name`, `customer.descriptive_name` | Incremental + Deduped | Forms `AdAccount` node |
| `customer_label` | Labeling accounts   | TBD                                                   | TBD                   | Optional               |
| `label`          | Label metadata      | TBD                                                   | TBD                   | Optional               |

### 📌 Campaign-Level Tables

| Table                       | Purpose                      | Selected Fields                                                                          | Sync Mode             | Notes                                  |
| --------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------- | --------------------- | -------------------------------------- |
| `campaign`                  | Core campaign info           | `campaign.id`, `campaign.name`, `campaign.status`, `campaign.advertising_channel_type`   | Incremental + Deduped | Forms `Campaign` node                  |
| `campaign_budget`           | Shared budget info           | `campaign_budget.id`, `campaign_budget.amount_micros`, `campaign_budget.delivery_method` | Incremental + Deduped | Link via `:USES_BUDGET`                |
| `campaign_bidding_strategy` | Strategy details             | TBD                                                                                      | TBD                   | Optional for bidding optimization      |
| `campaign_criterion`        | Campaign-level targeting     | All (10/10)                                                                              | Incremental           | Used for geo/device targeting analysis |
| `campaign_label`            | Label grouping for campaigns | TBD                                                                                      | TBD                   | Optional                               |

### 📌 Ad Group-Level Tables

| Table                        | Purpose                    | Selected Fields        | Sync Mode    | Notes                                       |
| ---------------------------- | -------------------------- | ---------------------- | ------------ | ------------------------------------------- |
| `ad_group_label`             | Ad group tagging           | TBD                    | Full Refresh | Optional                                    |
| `ad_group_criterion_label`   | Labeling ad group criteria | TBD                    | Full Refresh | Optional                                    |
| `ad_listing_group_criterion` | Shopping structure         | TBD                    | TBD          | Optional, eCommerce only                    |
| `keyword_view`               | Keyword-level performance  | 3/26 + `segments.date` | Incremental  | Feeds into `KeywordGroup` or `Keyword` node |
| `display_keyword_view`       | Display keyword data       | TBD                    | TBD          | Optional                                    |

### 📌 Ad-Level Tables

| Table        | Purpose              | Selected Fields        | Sync Mode    | Notes                                      |
| ------------ | -------------------- | ---------------------- | ------------ | ------------------------------------------ |
| `click_view` | Click event logs     | 3/16 + `segments.date` | Incremental  | Useful for detailed click-level tracking   |
| `audience`   | Target audience node | 2/8                    | Full Refresh | Create `Audience` node, link via targeting |
| `asset`      | Ad creative elements | TBD                    | TBD          | Optional (if asset tracking enabled)       |

### 📌 Metric & Aggregate Views

| Table                       | Purpose                                     | Selected Fields                                                                                                                                               | Sync Mode | Notes                                                                        |   |   |   |   |   |
| --------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---------------------------------------------------------------------------- | - | - | - | - | - |
| `ad` (metrics)              | Time-series KPIs at Ad level                | `ad_metric_ctr`, `ad_metric_clicks`, `ad_metric_conversions`, `ad_metric_average_cpc`, `ad_metric_impressions`, `date`                                        | Derived   | Attached to `Ad` nodes as separate `:Metric` nodes for time-series analysis  |   |   |   |   |   |
| `ad_group` (metrics)        | Weekly aggregated metrics at AdGroup level  | `ad_group_metric_ctr`, `ad_group_metric_clicks`, `ad_group_metric_conversions`, `ad_group_metric_average_cpc`, `ad_group_metric_impressions`, `segments.date` | Derived   | Attached to `AdGroup` nodes as weekly summary `:Metric` nodes or properties  |   |   |   |   |   |
| `campaign` (metrics)        | Weekly aggregated metrics at Campaign level | `campaign_metric_ctr`, `campaign_metric_clicks`, `campaign_metric_conversions`, `campaign_metric_average_cpc`, `campaign_metric_impressions`, `segments.date` | Derived   | Attached directly to `Campaign` node as weekly `:Metric` nodes or properties |   |   |   |   |   |
| `customer` (metrics)        | Weekly account-level aggregates             | `account_metric_ctr`, `account_metric_conversions`, `account_metric_cost_micros`, `segments.date`                                                             | Derived   | Used for top-level weekly spend and performance tracking on `AdAccount` node |   |   |   |   |   |
| `geographic_view`           | Regional breakdown                          | TBD                                                                                                                                                           | TBD       | Optional for geo-based optimization                                          |   |   |   |   |   |
| `shopping_performance_view` | Shopping campaign metrics                   | TBD                                                                                                                                                           | TBD       | Optional for eCommerce                                                       |   |   |   |   |   |
| `topic_view`                | Content targeting view                      | TBD                                                                                                                                                           | TBD       | Optional                                                                     |   |   |   |   |   |
| `user_interest`             | Affinity/in-market categories               | TBD                                                                                                                                                           | TBD       | Optional (advanced audience modeling)                                        |   |   |   |   |   |
| `user_location_view`        | Regional targeting detail                   | TBD                                                                                                                                                           | TBD       | Optional                                                                     |   |   |   |   |   |

\-

---

## ⏭️ Next Steps

- Define which **columns** to extract and how to **map them to graph nodes/edges**
- Establish **Cypher-based schema creation**: constraints + indexes
- Define **ETL logic** for transforming tabular streams → Neo4j entity-relationship model
- Set up **parallel load routines** for metric-based data

