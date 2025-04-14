# Google Ads Graph Database

This repository contains a data pipeline that transforms Google Ads data into a Neo4j graph database. The graph structure represents the hierarchical relationships between different entities in Google Ads, with metrics consolidated at the Ad level.

## Node Types and Properties

### AdAccount
- **Properties**:
  - `account_id`: Unique identifier for the ad account
  - `name`: Name of the ad account
  - `descriptive_name`: Descriptive name of the ad account
  - `currency`: Currency code used by the account
  - `timezone`: Timezone of the account
- **Constraints**:
  - `adaccount_id_unique`: Ensures `customer_id` is unique
  - `adaccount_name_not_null`: Ensures `descriptive_name` is not null

### Campaign
- **Properties**:
  - `campaign_id`: Unique identifier for the campaign
  - `name`: Name of the campaign
  - `status`: Status of the campaign (e.g., ENABLED, PAUSED)
  - `campaign_budget_id`: ID of the campaign budget
  - `campaign_type`: Type of campaign (e.g., SEARCH, DISPLAY)
- **Constraints**:
  - `campaign_id_unique`: Ensures `campaign_id` is unique
  - `campaign_name_not_null`: Ensures `name` is not null

### AdGroup
- **Properties**:
  - `ad_group_id`: Unique identifier for the ad group
  - `name`: Name of the ad group
  - `status`: Status of the ad group
  - `campaign_id`: ID of the parent campaign
- **Constraints**:
  - `adgroup_id_unique`: Ensures `ad_group_id` is unique
  - `adgroup_name_not_null`: Ensures `name` is not null

### Ad
- **Properties**:
  - `ad_id`: Unique identifier for the ad
  - `name`: Name of the ad
  - `status`: Status of the ad
  - `type`: Type of ad
  - `final_urls`: List of final URLs
  - `headlines`: List of headlines
  - `descriptions`: List of descriptions
  - `path1`: Path 1 for the ad
  - `path2`: Path 2 for the ad
  - `device_preference`: Device preference for the ad
  - `system_managed_resource_source`: System managed resource source
- **Constraints**:
  - `ad_id_unique`: Ensures `ad_id` is unique
  - `ad_name_not_null`: Ensures `name` is not null

### KeywordGroup
- **Properties**:
  - `ad_group_id`: ID of the parent ad group
  - `keyword_count`: Number of keywords in the group
  - `keywords`: List of keyword texts
  - `match_types`: List of match types for the keywords
  - `statuses`: List of statuses for the keywords
  - `bid_modifiers`: List of bid modifiers for the keywords
  - `quality_scores`: List of quality scores for the keywords
  - `criterion_ids`: List of criterion IDs for the keywords
- **Constraints**:
  - `keywordgroup_id_unique`: Ensures `ad_group_id` is unique

### Audience
- **Properties**:
  - `audience_id`: Unique identifier for the audience
  - `name`: Name of the audience
  - `status`: Status of the audience
  - `dimensions`: Dimensions of the audience
- **Constraints**:
  - `audience_id_unique`: Ensures `audience_id` is unique
  - `audience_name_not_null`: Ensures `name` is not null

### Asset
- **Properties**:
  - `asset_id`: Unique identifier for the asset
  - `asset_type`: Type of asset (e.g., IMAGE, VIDEO)
  - `name`: Name of the asset
  - `file_hash`: Hash of the asset file
- **Constraints**:
  - `asset_id_unique`: Ensures `asset_id` is unique
  - `asset_name_not_null`: Ensures `name` is not null

### ConversionAction
- **Properties**:
  - `account_id`: ID of the parent AdAccount
  - `conversion_actions_json`: JSON string containing all conversion actions with their properties:
    - `conversion_action_id`: Unique identifier for the conversion action
    - `name`: Name of the conversion action
    - `category`: Category of the conversion action
    - `type`: Type of the conversion action
    - `value_per_conversion`: Value per conversion
    - `counting_type`: Counting type for the conversion action
  - `conversion_count`: Number of conversion actions for this account
- **Constraints**:
  - `conversion_action_account_unique`: Ensures one ConversionAction node per AdAccount

### Metric
- **Properties**:
  - `entity_id`: ID of the entity (Ad ID)
  - `entity_type`: Type of the entity (always 'Ad')
  - `date`: Date of the metrics
  - `period`: Period of the metrics (e.g., daily)
  - `name`: Name of the metrics (consolidated_metrics)
  - `category`: Category of the metrics (performance)
  - Various metric values prefixed with:
    - `ad_metric_`: Metrics specific to the Ad
    - `campaign_metric_`: Metrics from the parent Campaign
    - `adgroup_metric_`: Metrics from the parent AdGroup
    - `account_metric_`: Metrics from the parent AdAccount
- **Constraints**:
  - `metric_name_not_null`: Ensures `name` is not null
  - `ad_metric_unique`: Ensures `(entity_id, date, entity_type)` is unique

## Relationships

1. **AdAccount to Campaign**:
   - Type: `HAS_CAMPAIGN`
   - Direction: AdAccount → Campaign

2. **Campaign to AdGroup**:
   - Type: `HAS_ADGROUP`
   - Direction: Campaign → AdGroup

3. **AdGroup to Ad**:
   - Type: `CONTAINS`
   - Direction: AdGroup → Ad

4. **AdGroup to KeywordGroup**:
   - Type: `HAS_KEYWORDS`
   - Direction: AdGroup → KeywordGroup

5. **AdAccount to ConversionAction**:
   - Type: `HAS_CONVERSION_ACTIONS`
   - Direction: AdAccount → ConversionAction

6. **Ad to Metric**:
   - Type: `HAS_METRIC`
   - Direction: Ad → Metric

## Indexes

The following indexes are created for better query performance:

- `(m:Metric) ON (m.name, m.date)`
- `(m:Metric) ON (m.entity_id, m.date)`
- `(c:Campaign) ON (c.campaign_id)`
- `(a:Ad) ON (a.ad_id)`
- `(ag:AdGroup) ON (ag.ad_group_id)`
- `(k:Keyword) ON (k.criterion_id)`
- `(a:Audience) ON (a.criterion_id)`
- `(aa:AdAccount) ON (aa.account_id)`

## Data Flow

1. The pipeline extracts data from PostgreSQL tables
2. Data is transformed into Neo4j graph format
3. Constraints and indexes are created
4. Entity nodes are created (AdAccount, Campaign, AdGroup, Ad, etc.)
5. Relationships are established between nodes
6. Metrics are consolidated at the Ad level, with relationships only between Ad and Metric nodes
7. Conversion actions are consolidated into a single node per AdAccount

## Usage

To run the pipeline:

```bash
python pipeline.py
```

The script will:
1. Connect to the PostgreSQL database
2. Extract data from the relevant tables
3. Transform the data into a Neo4j graph
4. Create all necessary nodes, relationships, and metrics 

# Graph Database Schema Mapping

This document outlines the mapping between PostgreSQL tables and Neo4j nodes in the graph database.

## Node Types and Their Source Tables

### AdAccount Node
- Source Table: `customer`
- Key Properties:
  - `account_id`: customer_id
  - `name`: customer_descriptive_name
  - `descriptive_name`: customer_descriptive_name
  - `currency`: customer_currency_code
  - `timezone`: customer_time_zone

### Campaign Node
- Source Table: `campaign`
- Key Properties:
  - `campaign_id`: campaign_id
  - `name`: campaign_name
  - `status`: campaign_status
  - `campaign_budget_id`: campaign_campaign_budget
  - `campaign_type`: campaign_advertising_channel_type

### AdGroup Node
- Source Table: `ad_group`
- Key Properties:
  - `ad_group_id`: ad_group_id
  - `name`: ad_group_name
  - `status`: ad_group_status
  - `campaign_id`: campaign_id

### Ad Node
- Source Table: `ad_group_ad`
- Key Properties:
  - `ad_id`: ad_group_ad_ad_id
  - `name`: ad_group_ad_ad_name
  - `status`: ad_group_ad_status
  - `type`: ad_group_ad_ad_type
  - `final_urls`: ad_group_ad_final_urls
  - `headlines`: ad_group_ad_headlines
  - `descriptions`: ad_group_ad_descriptions
  - `path1`: ad_group_ad_path1
  - `path2`: ad_group_ad_path2
  - `device_preference`: ad_group_ad_device_preference
  - `system_managed_resource_source`: ad_group_ad_system_managed_resource_source

### KeywordGroup Node
- Source Table: `ad_group_criterion` (filtered for KEYWORD type)
- Key Properties:
  - `ad_group_id`: ad_group_id
  - `keyword_count`: count of keywords
  - `keywords`: list of ad_group_criterion_keyword_text
  - `match_types`: list of ad_group_criterion_keyword_match_type
  - `statuses`: list of ad_group_criterion_status
  - `bid_modifiers`: list of ad_group_criterion_bid_modifier
  - `quality_scores`: list of ad_group_criterion_quality_info_quality_score
  - `criterion_ids`: list of ad_group_criterion_criterion_id

### Asset Node
- Source Table: `ad_group_ad` (extracted image assets)
- Key Properties:
  - `asset_id`: generated from ad_id + '_image'
  - `asset_type`: 'IMAGE'
  - `name`: ad_group_ad_ad_image_ad_name
  - `file_hash`: ad_group_ad_ad_image_ad_image_url

### ConversionAction Node
- Source Table: `campaign_budget` (derived)
- Key Properties:
  - `account_id`: customer_id
  - `conversion_actions_json`: JSON string containing conversion actions
  - `conversion_count`: count of conversion actions

### Metric Node
- Source Tables: Multiple (consolidated metrics)
- Key Properties:
  - `entity_id`: ID of the entity (ad_id)
  - `entity_type`: Type of entity ('Ad')
  - `date`: segments_date
  - `period`: 'daily'
  - `name`: 'consolidated_metrics'
  - `category`: 'performance'
  - Additional metric properties from:
    - Ad metrics (prefixed with 'metric_ad_')
    - AdGroup metrics (prefixed with 'metric_adgroup_')
    - Campaign metrics (prefixed with 'metric_campaign_')
    - Account metrics (prefixed with 'metric_account_')

## Relationships

1. AdAccount -[HAS_CAMPAIGN]-> Campaign
2. Campaign -[HAS_ADGROUP]-> AdGroup
3. AdGroup -[CONTAINS]-> Ad
4. AdGroup -[HAS_KEYWORDS]-> KeywordGroup
5. Ad -[HAS_METRIC]-> Metric
6. AdAccount -[HAS_CONVERSION_ACTIONS]-> ConversionAction

## Metric Categories

Metrics are categorized into the following groups:
- Engagement: impressions, clicks, ctr, interactions
- Cost: cost_micros, average_cpc, average_cpm
- Conversion: conversions, conversion_value
- Competitive: search_impression_share 