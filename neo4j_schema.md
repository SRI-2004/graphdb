# Neo4j Graph Schema (Concise with Descriptions)

## Node Types & Properties

**Metric Nodes (Common Properties):**
*(These properties are frequently found across various metric nodes below)*
* all_conversions: Double
* all_conversions_value: Double
* average_cpc: Double
* clicks: Double
* conversions: Double
* conversions_value: Double
* cost_micros: Double
* cost_per_conversion: Double | String (*Note: Type varies*)
* ctr: Double
* impressions: Double
* interaction_rate: Double
* interactions: Double
* value_per_conversion: Double
* view_through_conversions: Double | Long (*Note: Type varies*)

**Specific Metric Node Properties & Identifiers:**

* **AccountMonthlyMetric:** Aggregates performance statistics for the entire AdAccount over a specific month.
    * *Uses Common Properties*
    * account_id: Long (Identifier)
    * content_budget_lost_impression_share: Double
    * content_impression_share: Double
    * content_rank_lost_impression_share: Double
    * days_aggregated: Long
    * month_start_date: String (Time Identifier)
    * search_budget_lost_impression_share: Double
    * search_impression_share: Double
    * search_rank_lost_impression_share: Double
* **AccountOverallMetric:** Aggregates lifetime performance statistics for the entire AdAccount since tracking began.
    * *Uses Common Properties*
    * account_id: Long (Identifier)
    * all_conversions_value_per_cost: Double
    * average_cpm: Double
    * content_budget_lost_impression_share: Double
    * content_impression_share: Double
    * content_rank_lost_impression_share: Double
    * days_with_metrics: Long
    * first_metric_date: String
    * last_metric_date: String
    * search_budget_lost_impression_share: Double
    * search_impression_share: Double
    * search_rank_lost_impression_share: Double
* **AdDailyMetric:** Records performance statistics for a specific Ad on a particular day.
    * ad_id: Long (Identifier)
    * all_conversions: Long
    * all_conversions_value: Double
    * average_cpc: Double
    * average_cpm: Double
    * clicks: Long
    * conversions: Long
    * conversions_value: Double
    * cost_micros: Double
    * cost_per_conversion: String
    * ctr: Double
    * date: Date (Time Identifier)
    * impressions: Long
    * interaction_rate: Double
    * interactions: Long
    * value_per_conversion: Double
    * view_through_conversions: Long
* **AdMonthlyMetric:** Aggregates performance statistics for a specific Ad over a specific month.
    * *Uses Common Properties*
    * ad_id: Long (Identifier)
    * all_conversions_value_per_cost: Double
    * average_cpm: Double
    * days_aggregated: Long
    * month_start_date: String (Time Identifier)
* **AdOverallMetric:** Aggregates lifetime performance statistics for a specific Ad since tracking began.
    * *Uses Common Properties*
    * ad_id: Long (Identifier)
    * all_conversions_value_per_cost: Double
    * average_cpm: Double
    * days_with_metrics: Long
    * first_metric_date: String
    * last_metric_date: String
* **CampaignMonthlyMetric:** Aggregates performance statistics for a specific Campaign over a specific month.
    * *Uses Common Properties*
    * campaign_id: Long (Identifier)
    * all_conversions_value_per_cost: Double
    * average_cpm: Double
    * content_budget_lost_impression_share: Double
    * content_impression_share: Double
    * content_rank_lost_impression_share: Double
    * days_aggregated: Long
    * month_start_date: String (Time Identifier)
    * search_budget_lost_impression_share: Double
    * search_impression_share: Double
    * search_rank_lost_impression_share: Double
* **CampaignOverallMetric:** Aggregates lifetime performance statistics for a specific Campaign since tracking began.
    * *Uses Common Properties*
    * campaign_id: Long (Identifier)
    * all_conversions_value_per_cost: Double
    * average_cpm: Double
    * content_budget_lost_impression_share: Double
    * content_impression_share: Double
    * content_rank_lost_impression_share: Double
    * days_with_metrics: Long
    * first_metric_date: String
    * last_metric_date: String
    * search_budget_lost_impression_share: Double
    * search_impression_share: Double
    * search_rank_lost_impression_share: Double
* **WeeklyMetric:** Aggregates performance statistics, typically associated with a Campaign, over a specific week.
    * *Uses Common Properties*
    * campaign_id: Long (Identifier - typically relates to Campaign)
    * content_budget_lost_impression_share: Double
    * content_impression_share: Double
    * content_rank_lost_impression_share: Double
    * days_aggregated: Long
    * entity_type: String (Indicates the entity type the metric applies to, e.g., 'Campaign')
    * search_budget_lost_impression_share: Double
    * search_impression_share: Double
    * search_rank_lost_impression_share: Double
    * week_start_date: String (Time Identifier)

**Other Node Types:**

* **AdAccount:** Represents a Google Ads account containing campaigns, settings, and overall performance data.
    * account_id: Long, auto_tagging_enabled: Boolean, call_conversion_action: String, call_conversion_reporting_enabled: Boolean, call_reporting_enabled: Boolean, conversion_tracking_id: Long, cross_account_conversion_tracking: Boolean, currency: String, descriptive_name: String, final_url_suffix: String, google_global_site_tag: String, has_partners_badge: Boolean, manager: Boolean, name: String, optimization_score: Double, optimization_score_weight: Double, pay_per_conversion_eligibility_failure_reasons: StringArray, resource_name: String, test_account: Boolean, timezone: String, tracking_url_template: String
* **AdGroupBiddingSettings:** Defines the bidding strategy and specific bid amounts (CPC, CPM, etc.) for an AdGroup.
    * adGroupResourceName: String, cpc_bid_micros: Long, cpm_bid_micros: Long, cpv_bid_micros: Long, percent_cpc_bid_micros: Long, target_cpa_micros: Long, target_roas: Double
* **AdGroup:** Represents a collection of related Ads within a Campaign, sharing targeting criteria and bids.
    * ad_group_id: Long, base_ad_group_resource_name: String, campaign_id: Long, final_url_suffix: String, name: String, resource_name: String, status: String, tracking_url_template: String, type: String, url_custom_parameters: StringArray
* **Ad:** Represents an individual advertisement creative, including its text, images, URLs, and status.
    * ad_group_resource_name: String, ad_id: Long, added_by_google_ads: Boolean, business_name: String, description1: String, description2: String, descriptions: String, device_preference: String, display_url: String, final_mobile_urls: String, final_url_suffix: String, final_urls: String, headline1: String, headline2: String, headline3: String, headlines: String, image_media_id: Long, image_url: String, logo_images: String, long_headline: String, marketing_images: String, name: String, path1: String, path2: String, resource_name: String, square_logo_images: String, square_marketing_images: String, status: String, tracking_url_template: String, type: String, url_custom_parameters: String
* **AgeRange:** Represents a specific age demographic (e.g., 18-24) used for audience targeting.
    * maxAge: Long, minAge: Long
* **Asset:** Represents a reusable piece of content (like an image, video, or text) potentially used across multiple Ads.
    * asset_id: String, asset_type: String, file_hash: String, name: String
* **Audience:** Represents a defined group of users (e.g., based on demographics, interests, remarketing) used for targeting or observation.
    * audience_id: Long, description: String, name: String, resourceName: String, status: String
* **CampaignBudget:** Defines the budget amount, delivery method, and status for a specific Campaign or shared budget.
    * amount: Double, budget_id: Long, delivery_method: String, explicitly_shared: Boolean, has_shared_set: Boolean, name: String, resource_name: String, status: String, type: String
* **Campaign:** Represents an advertising campaign with a specific objective, budget, targeting settings, and containing AdGroups.
    * advertising_channel_sub_type: String, advertising_channel_type: String, campaign_id: Long, end_date: String, final_url_suffix: String, name: String, resource_name: String, serving_status: String, start_date: String, status: String, tracking_url_template: String, url_custom_parameters: StringArray
* **ConversionAction:** Defines a specific action tracked as a conversion (e.g., purchase, lead submission) within the AdAccount.
    * account_id: Long, conversion_actions_json: String, conversion_count: Long
* **Gender:** Represents a specific gender demographic (e.g., Male, Female, Undetermined) used for audience targeting.
    * genderType: String
* **KeywordGroup:** Represents keywords targeted by an AdGroup, including their match types, statuses, quality scores, and criterion IDs.
    * ad_group_id: Long, bid_modifiers: DoubleArray, criterion_ids: LongArray, keyword_count: Long, keywords: StringArray, match_types: StringArray, quality_scores: LongArray, statuses: StringArray
* *(Nodes with 0 count in original schema omitted: `AccountDailyMetric`, `AdGroupWeeklyMetric`, `CustomAudience`, `GeoLocation`, `Label`, `Language`, `NegativeKeyword`, `Product`, `TargetedLocation`, `UserInterest`)*

## Relationship Types (No Properties)

* `(AdGroup)-[:CONTAINS]->(Ad)`
* `(AdAccount)-[:DEFINED_AUDIENCE]->(Audience)`
* `(Campaign)-[:HAS_ADGROUP]->(AdGroup)`
* `(Audience)-[:HAS_AGE_RANGE]->(AgeRange)`
* `(AdGroup)-[:HAS_BIDDING_SETTINGS]->(AdGroupBiddingSettings)`
* `(AdAccount)-[:HAS_CAMPAIGN]->(Campaign)`
* `(AdAccount)-[:HAS_CONVERSION_ACTIONS]->(ConversionAction)`
* `(Ad)-[:HAS_DAILY_METRICS]->(AdDailyMetric)`
* `(Audience)-[:HAS_GENDER]->(Gender)`
* `(AdGroup)-[:HAS_KEYWORDS]->(KeywordGroup)`
* `(AdAccount)-[:HAS_MONTHLY_METRICS]->(AccountMonthlyMetric)`
* `(Ad)-[:HAS_MONTHLY_METRICS]->(AdMonthlyMetric)`
* `(Campaign)-[:HAS_MONTHLY_METRICS]->(CampaignMonthlyMetric)`
* `(AdAccount)-[:HAS_OVERALL_METRICS]->(AccountOverallMetric)`
* `(Ad)-[:HAS_OVERALL_METRICS]->(AdOverallMetric)`
* `(Campaign)-[:HAS_OVERALL_METRICS]->(CampaignOverallMetric)`
* `(Campaign)-[:HAS_WEEKLY_METRICS]->(WeeklyMetric)`
* `(Campaign)-[:USES_BUDGET]->(CampaignBudget)`
* *(Relationships involving omitted 0-count nodes are also omitted)*