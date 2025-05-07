# 🧠 Neo4j Graph Schema

## 📊 Node Counts

- `FbAd`: 332
- `FbAdAccount`: 1
- `FbAdCreative`: 938
- `FbAdLabel`: 0
- `FbAdSet`: 34
- `FbCampaign`: 27
- `FbImage`: 119
- `FbMonthlyCampaignInsight`: 600
- `FbMonthlyInsight`: 0
- `FbWeeklyCampaignInsight`: 1325
- `FbWeeklyInsight`: 10275

## 🟢 Node Types & Properties

### `:`FbAdAccount``
- account_id : String
- age : Double
- amount_spent : String
- balance : String
- business_city : String
- business_country_code : String
- business_name : String
- capabilities : String
- created_time : String
- currency : String
- disable_reason : Double
- funding_source : Double
- graph_api_id : String
- has_migrated_permissions : Boolean
- is_personal : Double
- is_prepay_account : Boolean
- is_tax_id_required : Boolean
- name : String
- owner_id : Double
- partner : String
- spend_cap : String
- status : Long
- tax_id : String
- tax_id_type : String
- timezone_id : Double
- timezone_name : String
- user_tasks : String

### `:`FbAdCreative``
- account_id : String
- adlabels : String
- asset_feed_spec : String
- body : String
- call_to_action_type : String
- id : String
- image_hash : String
- image_url : String
- instagram_actor_id : String
- instagram_story_id : String
- link_url : String
- name : String
- object_id : String
- object_story_id : String
- object_story_spec : String
- object_type : String
- object_url : String
- status : String
- thumbnail_url : String
- title : String
- video_id : String

### `:`FbAdSet``
- id : String
- name : String

### `:`FbAd``
- account_id : String
- adlabels : String
- bid_amount : String
- bid_info : String
- bid_type : String
- campaign_id : String
- conversion_specs : String
- created_time : String
- creative : String
- creative_id : String
- effective_status : String
- id : String
- name : String
- recommendations : String
- source_ad_id : String
- status : String
- targeting : String
- tracking_specs : String
- updated_time : String

### `:`FbCampaign``
- adlabels : String
- bid_strategy : String
- budget_rebalance_flag : Boolean
- budget_remaining : Double
- buying_type : String
- created_time : String
- daily_budget : Double
- effective_status : String
- id : String
- issues_info : String
- lifetime_budget : Double
- name : String
- objective : String
- special_ad_category : String
- spend_cap : Double
- start_time : String
- status : String
- stop_time : String
- updated_time : String

### `:`FbImage``
- account_id : String
- created_time : String
- filename : String
- hash : String
- height : Long
- id : String
- name : String
- original_height : Long
- original_width : Long
- permalink_url : String
- status : String
- updated_time : String
- url : String
- url_128 : String
- width : Long

### `:`FbMonthlyCampaignInsight``
- age : String
- campaign_id : String
- clicks : Double
- cpc : Double
- cpm : Double
- ctr : Double
- gender : String
- granularity : String
- impressions : Double
- insight_id : String
- insight_type : String
- period_start : String
- reach : Double
- social_spend : Double
- spend : Double

### `:`FbWeeklyCampaignInsight``
- age : String
- campaign_id : String
- clicks : Double
- cpc : Double
- cpm : Double
- ctr : Double
- gender : String
- granularity : String
- impressions : Double
- insight_id : String
- insight_type : String
- period_start : String
- reach : Double
- social_spend : Double
- spend : Double

### `:`FbWeeklyInsight``
- ad_id : String
- age : String
- clicks : Double
- cpc : Double
- cpm : Double
- ctr : Double
- gender : String
- granularity : String
- impressions : Double
- insight_id : String
- insight_type : String
- period_start : String
- reach : Double
- social_spend : Double
- spend : Double

## 🔗 Relationship Types, Structures & Properties

### `(`FbAdSet`)-[:CONTAINS_AD]->(`FbAd`)`
*(No properties)*

### `(`FbCampaign`)-[:HAS_ADSET]->(`FbAdSet`)`
*(No properties)*

### `(`FbAdAccount`)-[:HAS_CAMPAIGN]->(`FbCampaign`)`
*(No properties)*

### `(`FbCampaign`)-[:HAS_MONTHLY_INSIGHT]->(`FbMonthlyCampaignInsight`)`
*(No properties)*

### `(`FbAd`)-[:HAS_WEEKLY_INSIGHT]->(`FbWeeklyCampaignInsight`)`
### `(`FbAd`)-[:HAS_WEEKLY_INSIGHT]->(`FbWeeklyInsight`)`
### `(`FbCampaign`)-[:HAS_WEEKLY_INSIGHT]->(`FbWeeklyCampaignInsight`)`
### `(`FbCampaign`)-[:HAS_WEEKLY_INSIGHT]->(`FbWeeklyInsight`)`
*(No properties)*

### `(`FbAdCreative`)-[:USES_IMAGE]->(`FbImage`)`
*(No properties)*

