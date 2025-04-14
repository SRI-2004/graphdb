import json
import pandas as pd
from neo4j import GraphDatabase
from typing import Dict, List, Any
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class GraphTransformer:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """Initialize the transformer with Neo4j connection details"""
        logger.info("Initializing GraphTransformer")
        try:
            self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
            logger.info("Successfully connected to Neo4j database")
            # Define batch sizes and metric categories
            self.BATCH_SIZE = 1000
            self.metric_categories = {
                'engagement': ['impressions', 'clicks', 'ctr', 'interactions'],
                'cost': ['cost_micros', 'average_cpc', 'average_cpm'],
                'conversion': ['conversions', 'conversion_value'],
                'competitive': ['search_impression_share']
            }
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j database: {str(e)}")
            raise
        
    def close(self):
        """Close the Neo4j connection"""
        self.driver.close()

    def create_constraints(self):
        """Create constraints for Neo4j"""
        constraints = [
            # AdAccount constraints
            "CREATE CONSTRAINT adaccount_id_unique IF NOT EXISTS FOR (aa:AdAccount) REQUIRE aa.account_id IS UNIQUE",

            # Campaign constraints
            "CREATE CONSTRAINT campaign_id_unique IF NOT EXISTS FOR (c:Campaign) REQUIRE c.campaign_id IS UNIQUE",
            "CREATE CONSTRAINT campaign_name_not_null IF NOT EXISTS FOR (c:Campaign) REQUIRE c.name IS NOT NULL",
            
            # CampaignBudget constraints
            "CREATE CONSTRAINT campaign_budget_id_unique IF NOT EXISTS FOR (cb:CampaignBudget) REQUIRE cb.budget_id IS UNIQUE",
            "CREATE CONSTRAINT campaign_budget_name_not_null IF NOT EXISTS FOR (cb:CampaignBudget) REQUIRE cb.name IS NOT NULL",
            
            # Ad constraints
            "CREATE CONSTRAINT ad_id_unique IF NOT EXISTS FOR (a:Ad) REQUIRE a.ad_id IS UNIQUE",
            "CREATE CONSTRAINT ad_name_not_null IF NOT EXISTS FOR (a:Ad) REQUIRE a.name IS NOT NULL",
            
            # AdGroup constraints
            "CREATE CONSTRAINT adgroup_id_unique IF NOT EXISTS FOR (ag:AdGroup) REQUIRE ag.ad_group_id IS UNIQUE",
            "CREATE CONSTRAINT adgroup_name_not_null IF NOT EXISTS FOR (ag:AdGroup) REQUIRE ag.name IS NOT NULL",
            
            # KeywordGroup constraints
            "CREATE CONSTRAINT keywordgroup_id_unique IF NOT EXISTS FOR (kg:KeywordGroup) REQUIRE kg.ad_group_id IS UNIQUE",
            
            # Audience constraints
            "CREATE CONSTRAINT audience_id_unique IF NOT EXISTS FOR (a:Audience) REQUIRE a.audience_id IS UNIQUE",
            "CREATE CONSTRAINT audience_name_not_null IF NOT EXISTS FOR (a:Audience) REQUIRE a.name IS NOT NULL",
            
            # Label constraints
            "CREATE CONSTRAINT label_id_unique IF NOT EXISTS FOR (l:Label) REQUIRE l.label_id IS UNIQUE",
            "CREATE CONSTRAINT label_name_not_null IF NOT EXISTS FOR (l:Label) REQUIRE l.name IS NOT NULL",
            
            # Asset constraints
            "CREATE CONSTRAINT asset_id_unique IF NOT EXISTS FOR (a:Asset) REQUIRE a.asset_id IS UNIQUE",
            "CREATE CONSTRAINT asset_name_not_null IF NOT EXISTS FOR (a:Asset) REQUIRE a.name IS NOT NULL",
            
            # ConversionAction constraints
            "CREATE CONSTRAINT conversion_action_account_unique IF NOT EXISTS FOR (ca:ConversionAction) REQUIRE ca.account_id IS UNIQUE",
            
            # Product constraints (New)
            "CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.itemId IS UNIQUE",

            # GeoLocation constraints (New)
            "CREATE CONSTRAINT geolocation_id_unique IF NOT EXISTS FOR (gl:GeoLocation) REQUIRE gl.criterionId IS UNIQUE",
            
            # AdGroupBiddingSettings constraints (New)
            "CREATE CONSTRAINT adgroup_bidding_settings_resource_unique IF NOT EXISTS FOR (agbs:AdGroupBiddingSettings) REQUIRE agbs.adGroupResourceName IS UNIQUE",

            # Audience Component Constraints (New)
            "CREATE CONSTRAINT age_range_unique IF NOT EXISTS FOR (ar:AgeRange) REQUIRE (ar.minAge, ar.maxAge) IS UNIQUE",
            "CREATE CONSTRAINT gender_unique IF NOT EXISTS FOR (g:Gender) REQUIRE g.genderType IS UNIQUE",
            "CREATE CONSTRAINT user_interest_unique IF NOT EXISTS FOR (ui:UserInterest) REQUIRE ui.criterionId IS UNIQUE",
            "CREATE CONSTRAINT custom_audience_unique IF NOT EXISTS FOR (ca:CustomAudience) REQUIRE ca.customAudienceId IS UNIQUE",

            # DailyMetric constraints (New)
            # "CREATE CONSTRAINT daily_metric_unique IF NOT EXISTS FOR (dm:DailyMetric) REQUIRE (dm.ad_id, dm.date) IS UNIQUE",
            # "CREATE CONSTRAINT daily_metric_ad_id_not_null IF NOT EXISTS FOR (dm:DailyMetric) REQUIRE dm.ad_id IS NOT NULL",
            # "CREATE CONSTRAINT daily_metric_date_not_null IF NOT EXISTS FOR (dm:DailyMetric) REQUIRE dm.date IS NOT NULL"

            # AdDailyMetric constraints
            "CREATE CONSTRAINT ad_daily_metric_unique IF NOT EXISTS FOR (adm:AdDailyMetric) REQUIRE (adm.ad_id, adm.date) IS UNIQUE",
            "CREATE CONSTRAINT ad_daily_metric_ad_id_not_null IF NOT EXISTS FOR (adm:AdDailyMetric) REQUIRE adm.ad_id IS NOT NULL",
            "CREATE CONSTRAINT ad_daily_metric_date_not_null IF NOT EXISTS FOR (adm:AdDailyMetric) REQUIRE adm.date IS NOT NULL",

            # AdOverallMetric constraints (New)
            "CREATE CONSTRAINT ad_overall_metric_unique IF NOT EXISTS FOR (aom:AdOverallMetric) REQUIRE aom.ad_id IS UNIQUE",
            "CREATE CONSTRAINT ad_overall_metric_ad_id_not_null IF NOT EXISTS FOR (aom:AdOverallMetric) REQUIRE aom.ad_id IS NOT NULL",

            # AdMonthlyMetric constraints (New)
            "CREATE CONSTRAINT ad_monthly_metric_unique IF NOT EXISTS FOR (amm:AdMonthlyMetric) REQUIRE (amm.ad_id, amm.month_start_date) IS UNIQUE",
            "CREATE CONSTRAINT ad_monthly_metric_ad_id_not_null IF NOT EXISTS FOR (amm:AdMonthlyMetric) REQUIRE amm.ad_id IS NOT NULL",
            "CREATE CONSTRAINT ad_monthly_metric_month_start_date_not_null IF NOT EXISTS FOR (amm:AdMonthlyMetric) REQUIRE amm.month_start_date IS NOT NULL",

            # AccountMonthlyMetric constraints
            "CREATE CONSTRAINT account_monthly_metric_unique IF NOT EXISTS FOR (amm:AccountMonthlyMetric) REQUIRE (amm.account_id, amm.month_start_date) IS UNIQUE",
            "CREATE CONSTRAINT account_monthly_metric_account_id_not_null IF NOT EXISTS FOR (amm:AccountMonthlyMetric) REQUIRE amm.account_id IS NOT NULL",
            "CREATE CONSTRAINT account_monthly_metric_month_start_date_not_null IF NOT EXISTS FOR (amm:AccountMonthlyMetric) REQUIRE amm.month_start_date IS NOT NULL",

            # AdGroupWeeklyMetric constraints
            "CREATE CONSTRAINT adgroup_weekly_metric_unique IF NOT EXISTS FOR (agwm:AdGroupWeeklyMetric) REQUIRE (agwm.ad_group_id, agwm.week_start_date) IS UNIQUE",
            "CREATE CONSTRAINT adgroup_weekly_metric_ad_group_id_not_null IF NOT EXISTS FOR (agwm:AdGroupWeeklyMetric) REQUIRE agwm.ad_group_id IS NOT NULL",
            "CREATE CONSTRAINT adgroup_weekly_metric_week_start_date_not_null IF NOT EXISTS FOR (agwm:AdGroupWeeklyMetric) REQUIRE agwm.week_start_date IS NOT NULL",

            # CampaignWeeklyMetric constraints (Using :WeeklyMetric label)
            "CREATE CONSTRAINT campaign_weekly_metric_unique IF NOT EXISTS FOR (cwm:WeeklyMetric {entity_type: 'Campaign'}) REQUIRE (cwm.campaign_id, cwm.week_start_date) IS UNIQUE",
            "CREATE CONSTRAINT campaign_weekly_metric_campaign_id_not_null IF NOT EXISTS FOR (cwm:WeeklyMetric {entity_type: 'Campaign'}) REQUIRE cwm.campaign_id IS NOT NULL",
            "CREATE CONSTRAINT campaign_weekly_metric_week_start_date_not_null IF NOT EXISTS FOR (cwm:WeeklyMetric {entity_type: 'Campaign'}) REQUIRE cwm.week_start_date IS NOT NULL",

            # CampaignOverallMetric constraints (New)
            "CREATE CONSTRAINT campaign_overall_metric_unique IF NOT EXISTS FOR (com:CampaignOverallMetric) REQUIRE com.campaign_id IS UNIQUE",
            "CREATE CONSTRAINT campaign_overall_metric_campaign_id_not_null IF NOT EXISTS FOR (com:CampaignOverallMetric) REQUIRE com.campaign_id IS NOT NULL",

            # CampaignMonthlyMetric constraints (New)
            "CREATE CONSTRAINT campaign_monthly_metric_unique IF NOT EXISTS FOR (cmm:CampaignMonthlyMetric) REQUIRE (cmm.campaign_id, cmm.month_start_date) IS UNIQUE",
            "CREATE CONSTRAINT campaign_monthly_metric_campaign_id_not_null IF NOT EXISTS FOR (cmm:CampaignMonthlyMetric) REQUIRE cmm.campaign_id IS NOT NULL",
            "CREATE CONSTRAINT campaign_monthly_metric_month_start_date_not_null IF NOT EXISTS FOR (cmm:CampaignMonthlyMetric) REQUIRE cmm.month_start_date IS NOT NULL",

            # Product indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.itemId)",

            # GeoLocation indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (gl:GeoLocation) ON (gl.criterionId)",

            # AdGroupBiddingSettings indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (agbs:AdGroupBiddingSettings) ON (agbs.adGroupResourceName)",
            
            # Audience Component Indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (ar:AgeRange) ON (ar.minAge, ar.maxAge)",
            "CREATE INDEX IF NOT EXISTS FOR (g:Gender) ON (g.genderType)",
            "CREATE INDEX IF NOT EXISTS FOR (ui:UserInterest) ON (ui.criterionId)",
            "CREATE INDEX IF NOT EXISTS FOR (ca:CustomAudience) ON (ca.customAudienceId)",

            # Account Metrics Aggregates (New)
            "CREATE INDEX IF NOT EXISTS FOR (acm:AccountOverallMetric) ON (acm.account_id)",

            # AccountOverallMetric constraints (New)
            "CREATE CONSTRAINT account_overall_metric_unique IF NOT EXISTS FOR (aoom:AccountOverallMetric) REQUIRE aoom.account_id IS UNIQUE",
            "CREATE CONSTRAINT account_overall_metric_account_id_not_null IF NOT EXISTS FOR (aoom:AccountOverallMetric) REQUIRE aoom.account_id IS NOT NULL",

            # AccountMonthlyMetric constraints
            "CREATE CONSTRAINT account_monthly_metric_unique IF NOT EXISTS FOR (amm:AccountMonthlyMetric) REQUIRE (amm.account_id, amm.month_start_date) IS UNIQUE",
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    logger.error(f"Error creating constraint {constraint}: {str(e)}")

    def create_metric_node(self, tx, metric_data: Dict[str, Any]):
        """Create a Metric node with its properties"""
        logger.debug(f"Creating metric node with data: {metric_data}")
        try:
            query = """
            MERGE (m:Metric {
                name: $name,
                category: $category,
                value: $value,
                date: $date,
                period: $period
            })
            RETURN m
            """
            tx.run(query, metric_data)
            logger.debug(f"Successfully created metric node: {metric_data['name']}")
        except Exception as e:
            logger.error(f"Failed to create metric node: {str(e)}")
            raise

    def create_entity_node(self, tx, entity_type: str, properties: Dict[str, Any]):
        """Create an entity node with its properties"""
        query = f"""
        MERGE (e:{entity_type} {{{', '.join(f'{k}: ${k}' for k in properties.keys())}}})
        RETURN e
        """
        tx.run(query, properties)

    def create_entity_nodes_batch(self, tx, entity_type: str, nodes: List[Dict[str, Any]]):
        """Create multiple entity nodes in a single transaction using UNWIND"""
        # Update id_property mapping to include MetricNode
        id_property = {
            'AdAccount': 'account_id',
            'Campaign': 'campaign_id',
            'AdGroup': 'ad_group_id',
            'Ad': 'ad_id',
            'Keyword': 'criterion_id',
            'KeywordGroup': 'ad_group_id',  # Add KeywordGroup with ad_group_id as the unique identifier
            'Audience': 'audience_id',  # Fix: Change from criterion_id to audience_id
            'Asset': 'asset_id',
            'ConversionAction': 'account_id',  # Updated to use account_id as the unique identifier
            'Metric': ['name', 'date', 'entity_id', 'entity_type'],  # Composite key for metrics
            'AdMetricsSnapshot': 'snapshot_id',
            'CampaignMetricsSnapshot': 'snapshot_id',
            'AdGroupMetricsSnapshot': 'snapshot_id',
            'Label': 'label_id',  # Add Label with label_id as the unique identifier
            'CampaignCriterion': 'resource_name',  # Add CampaignCriterion with resource_name as the unique identifier
            'CampaignBudget': 'budget_id',  # Add CampaignBudget with budget_id as the unique identifier
            'Product': 'itemId', # Add Product node identifier
            'GeoLocation': 'criterionId', # Add GeoLocation node identifier
            'DailyMetric': ['ad_id', 'date'], # New: Composite key for DailyMetric
            'AdDailyMetric': ['ad_id', 'date'],
            'AccountDailyMetric': ['account_id', 'date'],
            'AccountMonthlyMetric': ['account_id', 'month_start_date'],
            'AdGroupWeeklyMetric': ['ad_group_id', 'week_start_date'],
            'AdGroupBiddingSettings': 'adGroupResourceName', # New
            # Audience Components (New)
            'AgeRange': ['minAge', 'maxAge'],
            'Gender': 'genderType',
            'UserInterest': 'criterionId',
            'CustomAudience': 'customAudienceId',
            # Ad Metrics Aggregates (New)
            'AdOverallMetric': 'ad_id',
            'AdMonthlyMetric': ['ad_id', 'month_start_date'],
            # Campaign Metrics Aggregates (New)
            'CampaignOverallMetric': 'campaign_id',
            'CampaignMonthlyMetric': ['campaign_id', 'month_start_date'],
            # Account Metrics Aggregates (New)
            'AccountOverallMetric': 'account_id',
            # 'AdGroupWeeklyBiddingMetric': ['ad_group_id', 'week_start_date'], # Removed
            'WeeklyMetric': ['campaign_id', 'week_start_date'] # For Campaign context
        }.get(entity_type)
        
        if not id_property:
            raise ValueError(f"Unknown entity type: {entity_type}")
        
        # Create the MERGE query with property matching
        if isinstance(id_property, list):
            # Handle composite key case (for Metric nodes)
            conditions = [f"e.{prop} = node.{prop}" for prop in id_property]
            query = f"""
            UNWIND $nodes as node
            MERGE (e:{entity_type} {{{', '.join(f'{prop}: node.{prop}' for prop in id_property)}}})
            SET e += node
            """
        else:
            # Handle single property key case
            query = f"""
            UNWIND $nodes as node
            MERGE (e:{entity_type} {{{id_property}: node.{id_property}}})
            SET e += node
            """
        
        tx.run(query, {'nodes': nodes})

    def create_relationships_batch(self, tx, start_type: str, end_type: str, rel_type: str, 
                                 relationships: List[Dict[str, Any]]):
        """Create multiple relationships in a single transaction using UNWIND"""
        query = f"""
        UNWIND $relationships as rel
        MATCH (start:{start_type})
        WHERE start[rel.start_key] = rel.start_value
        MATCH (end:{end_type})
        WHERE end[rel.end_key] = rel.end_value
        MERGE (start)-[r:{rel_type}]->(end)
        """
        tx.run(query, {'relationships': relationships})

    def transform_adaccount(self, account_df: pd.DataFrame):
        """Transform ad account data using batch processing"""
        # Remove duplicates from DataFrame if any exist
        account_df = account_df.drop_duplicates(subset=['customer_id'])
        
        logger.info(f"Starting batch AdAccount transformation for {len(account_df)} unique accounts")
        with self.driver.session() as session:
            # Process in batches
            for start_idx in range(0, len(account_df), self.BATCH_SIZE):
                batch = account_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                # Create account nodes in batch with expanded properties
                nodes = [{
                    'account_id': row['customer_id'],
                    'name': row['customer_descriptive_name'],
                    'descriptive_name': row['customer_descriptive_name'],
                    'currency': row['customer_currency_code'],
                    'timezone': row['customer_time_zone'],
                    'manager': row.get('customer_manager', ''),
                    'test_account': row.get('customer_test_account', False),
                    'auto_tagging_enabled': row.get('customer_auto_tagging_enabled', False),
                    'optimization_score': row.get('customer_optimization_score', 0),
                    'optimization_score_weight': row.get('customer_optimization_score_weight', 0),
                    'has_partners_badge': row.get('customer_has_partners_badge', False),
                    'resource_name': row.get('customer_resource_name', ''),
                    'final_url_suffix': row.get('customer_final_url_suffix', ''),
                    'tracking_url_template': row.get('customer_tracking_url_template', ''),
                    'conversion_tracking_id': row.get('customer_conversion_tracking_setting_conversion_tracking_id', ''),
                    'cross_account_conversion_tracking': row.get('customer_conversion_tracking_setting_cross_account_conversion_tracking', False),
                    'call_reporting_enabled': row.get('customer_call_reporting_setting_call_reporting_enabled', False),
                    'call_conversion_action': row.get('customer_call_reporting_setting_call_conversion_action', ''),
                    'call_conversion_reporting_enabled': row.get('customer_call_reporting_setting_call_conversion_reporting_enabled', False),
                    'google_global_site_tag': row.get('customer_remarketing_setting_google_global_site_tag', ''),
                    'pay_per_conversion_eligibility_failure_reasons': row.get('customer_pay_per_conversion_eligibility_failure_reasons', [])
                } for _, row in batch.iterrows()]
                
                logger.debug(f"Processing batch of {len(nodes)} AdAccount nodes")
                session.execute_write(self.create_entity_nodes_batch, 'AdAccount', nodes)
                logger.info(f"Processed {len(nodes)} AdAccount nodes")
        
        logger.info("Completed AdAccount transformation")

    def transform_campaign(self, campaign_df: pd.DataFrame, customer_id: str):
        """Transform campaign data using batch processing"""
        with self.driver.session() as session:
            for start_idx in range(0, len(campaign_df), self.BATCH_SIZE):
                batch = campaign_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                # Create campaign nodes in batch
                nodes = [{
                    'campaign_id': row['campaign_id'],
                    'resource_name': row['campaign_resource_name'],
                    'name': row['campaign_name'],
                    'status': row['campaign_status'],
                    'advertising_channel_type': row['campaign_advertising_channel_type'],
                    'advertising_channel_sub_type': row['campaign_advertising_channel_sub_type'],
                    'serving_status': row['campaign_serving_status'],
                    'start_date': row['campaign_start_date'],
                    'end_date': row['campaign_end_date'],
                    'final_url_suffix': row['campaign_final_url_suffix'],
                    'tracking_url_template': row['campaign_tracking_url_template'],
                    'url_custom_parameters': row['campaign_url_custom_parameters']
                } for _, row in batch.iterrows()]
                
                session.execute_write(self.create_entity_nodes_batch, 'Campaign', nodes)
                
                # Create relationships to AdAccount
                relationships = [{
                    'start_key': 'account_id',
                    'start_value': customer_id,
                    'end_key': 'campaign_id',
                    'end_value': row['campaign_id']
                } for _, row in batch.iterrows()]
                
                session.execute_write(
                    self.create_relationships_batch,
                    'AdAccount',
                    'Campaign',
                    'HAS_CAMPAIGN',
                    relationships
                )

    def transform_adgroup(self, adgroup_df: pd.DataFrame):
        """Transform ad group data using batch processing, including creating AdGroupBiddingSettings nodes for overrides."""
        logger.info(f"--- Verifying adgroup_df ---")
        logger.info(f"Input adgroup_df shape: {adgroup_df.shape}")
        logger.info(f"First 5 rows of adgroup_df:\\n{adgroup_df.head().to_string()}")
        logger.info(f"--- End Verification ---")

        # Define columns representing potential Ad Group level bidding overrides
        bidding_override_cols = [
            'ad_group_cpc_bid_micros',
            'ad_group_cpm_bid_micros',
            'ad_group_cpv_bid_micros', # Add cpv if relevant
            'ad_group_target_cpa_micros',
            'ad_group_target_roas',
            'ad_group_percent_cpc_bid_micros' # Add percent cpc if relevant
        ]
        # Check which override columns actually exist in the dataframe
        existing_override_cols = [col for col in bidding_override_cols if col in adgroup_df.columns]

        with self.driver.session() as session:
            for start_idx in range(0, len(adgroup_df), self.BATCH_SIZE):
                batch = adgroup_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                # --- Create Ad Group nodes in batch (as before) ---
                adgroup_nodes = [{
                    'ad_group_id': row['ad_group_id'],
                    'resource_name': row.get('ad_group_resource_name', ''),
                    'name': row['ad_group_name'],
                    'status': row['ad_group_status'],
                    'type': row.get('ad_group_type', ''),
                    'campaign_id': row.get('campaign_id', ''),
                    'base_ad_group_resource_name': row.get('base_ad_group_resource_name', ''),
                    'tracking_url_template': row.get('ad_group_tracking_url_template', ''),
                    'final_url_suffix': row.get('ad_group_final_url_suffix', ''),
                    'url_custom_parameters': row.get('ad_group_url_custom_parameters', {}),
                    # Note: Bidding fields are now moved to the separate settings node IF they exist and are overrides
                    # We might still want effective bids/targets here if they are useful on the main node? Or keep AdGroup clean.
                    # 'effective_target_cpa_micros': row.get('ad_group_effective_target_cpa_micros', 0),
                    # 'effective_target_roas': row.get('ad_group_effective_target_roas', 0.0)
                } for _, row in batch.iterrows()]
                
                session.execute_write(self.create_entity_nodes_batch, 'AdGroup', adgroup_nodes)
                
                # --- Create Campaign -> AdGroup relationships (as before) ---
                campaign_relationships = [{
                    'start_key': 'campaign_id',
                    'start_value': row['campaign_id'],
                    'end_key': 'ad_group_id',
                    'end_value': row['ad_group_id']
                } for _, row in batch.iterrows() if 'campaign_id' in row and pd.notna(row['campaign_id'])] # Added check for campaign_id
                
                if campaign_relationships:
                    session.execute_write(
                    self.create_relationships_batch,
                    'Campaign',
                    'AdGroup',
                    'HAS_ADGROUP',
                        campaign_relationships
                    )
                
                # --- Create AdGroupBiddingSettings nodes and relationships for overrides ---
                bidding_settings_nodes = []
                bidding_relationships = []
                for _, row in batch.iterrows():
                    ad_group_resource = row.get('ad_group_resource_name')
                    if not ad_group_resource or pd.isna(ad_group_resource):
                        logger.warning(f"Skipping bidding settings for ad group {row.get('ad_group_id')} due to missing resource name.")
                        continue

                    settings_node = {'adGroupResourceName': ad_group_resource}
                    has_override = False
                    
                    for col in existing_override_cols:
                         # Check if the column exists and the value is not null/NA
                         if col in row and pd.notna(row[col]):
                             # Check if the value is potentially an override (e.g., not 0 or default)
                             # This check might need refinement based on actual default values
                             value = row[col]
                             is_override_value = True 
                             # Example refinement: only treat as override if > 0 for bids/targets
                             # if ('cpc_bid_micros' in col or 'cpm_bid_micros' in col or 'cpv_bid_micros' in col or 'target_cpa_micros' in col or 'percent_cpc_bid_micros' in col) and value <= 0:
                             #     is_override_value = False
                             # if 'target_roas' in col and value <= 0.0: # Assuming ROAS > 0 is an override
                             #      is_override_value = False
                             
                             if is_override_value:
                                 prop_name = col.replace('ad_group_', '') # Clean up property name
                                 # Convert micros if necessary
                                 if 'micros' in prop_name:
                                     settings_node[prop_name] = int(value) # Keep as micros integer
                                 elif 'roas' in prop_name:
                                      settings_node[prop_name] = float(value)
                                 else:
                                      settings_node[prop_name] = value # Assign directly
                                 has_override = True

                    if has_override:
                        bidding_settings_nodes.append(settings_node)
                        bidding_relationships.append({
                            'start_key': 'resource_name', # Link AdGroup using resource_name
                            'start_value': ad_group_resource,
                            'end_key': 'adGroupResourceName', # Link Settings using adGroupResourceName
                            'end_value': ad_group_resource
                        })

                # Batch write bidding settings nodes and relationships
                if bidding_settings_nodes:
                    logger.debug(f"Creating batch of {len(bidding_settings_nodes)} AdGroupBiddingSettings nodes.")
                    session.execute_write(self.create_entity_nodes_batch, 'AdGroupBiddingSettings', bidding_settings_nodes)
                
                if bidding_relationships:
                    logger.debug(f"Creating batch of {len(bidding_relationships)} HAS_BIDDING_SETTINGS relationships.")
                    # Custom query needed because start node key ('resource_name') is different from end node key ('adGroupResourceName')
                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:AdGroup {resource_name: rel.start_value})
                    MATCH (end:AdGroupBiddingSettings {adGroupResourceName: rel.end_value})
                    MERGE (start)-[r:HAS_BIDDING_SETTINGS]->(end)
                    """
                    session.run(rel_query, {'relationships': bidding_relationships})

        logger.info("Completed AdGroup transformation (including Bidding Settings).")

    def transform_ad(self, ad_df: pd.DataFrame, customer_id: str):
        """Transform ads into graph format using batch processing"""
        with self.driver.session() as session:
            # Remove duplicates based on ad_id
            ad_df = ad_df.drop_duplicates(subset=['ad_group_ad_ad_id'])
            
            for start_idx in range(0, len(ad_df), self.BATCH_SIZE):
                batch = ad_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                # Create ad nodes
                ads = []
                for _, row in batch.iterrows():
                    # Convert any dictionary values to JSON strings
                    url_custom_parameters = row.get('ad_group_ad_url_custom_parameters', {})
                    if isinstance(url_custom_parameters, dict):
                        url_custom_parameters = json.dumps(url_custom_parameters)
                    
                    final_urls = row.get('ad_group_ad_final_urls', [])
                    if isinstance(final_urls, list):
                        final_urls = json.dumps(final_urls)
                    
                    final_mobile_urls = row.get('ad_group_ad_final_mobile_urls', [])
                    if isinstance(final_mobile_urls, list):
                        final_mobile_urls = json.dumps(final_mobile_urls)
                    
                    headlines = row.get('ad_group_ad_headlines', [])
                    if isinstance(headlines, list):
                        headlines = json.dumps(headlines)
                    
                    descriptions = row.get('ad_group_ad_descriptions', [])
                    if isinstance(descriptions, list):
                        descriptions = json.dumps(descriptions)
                    
                    marketing_images = row.get('ad_group_ad_marketing_images', [])
                    if isinstance(marketing_images, list):
                        marketing_images = json.dumps(marketing_images)
                    
                    square_marketing_images = row.get('ad_group_ad_square_marketing_images', [])
                    if isinstance(square_marketing_images, list):
                        square_marketing_images = json.dumps(square_marketing_images)
                    
                    logo_images = row.get('ad_group_ad_logo_images', [])
                    if isinstance(logo_images, list):
                        logo_images = json.dumps(logo_images)
                    
                    square_logo_images = row.get('ad_group_ad_square_logo_images', [])
                    if isinstance(square_logo_images, list):
                        square_logo_images = json.dumps(square_logo_images)
                    
                    ad = {
                        'ad_id': row['ad_group_ad_ad_id'],
                        'resource_name': row.get('ad_group_ad_resource_name', ''),
                        'ad_group_resource_name': row.get('ad_group_resource_name', ''),
                        'status': row['ad_group_ad_status'],
                        'type': row['ad_group_ad_ad_type'],
                        'name': row.get('ad_group_ad_ad_name', ''),
                        'final_urls': final_urls,
                        'final_mobile_urls': final_mobile_urls,
                        'tracking_url_template': row.get('ad_group_ad_tracking_url_template', ''),
                        'final_url_suffix': row.get('ad_group_ad_final_url_suffix', ''),
                        'url_custom_parameters': url_custom_parameters,
                        'display_url': row.get('ad_group_ad_display_url', ''),
                        'added_by_google_ads': row.get('ad_group_ad_added_by_google_ads', False),
                        'device_preference': row.get('ad_group_ad_device_preference', 'UNSPECIFIED'),
                        
                        # Text Ad specific properties
                        'headline1': row.get('ad_group_ad_headline1', ''),
                        'headline2': row.get('ad_group_ad_headline2', ''),
                        'headline3': row.get('ad_group_ad_headline3', ''),
                        'description1': row.get('ad_group_ad_description1', ''),
                        'description2': row.get('ad_group_ad_description2', ''),
                        'path1': row.get('ad_group_ad_path1', ''),
                        'path2': row.get('ad_group_ad_path2', ''),
                        
                        # Responsive Search Ad properties
                        'headlines': headlines,
                        'descriptions': descriptions,
                        
                        # Image Ad properties
                        'image_url': row.get('ad_group_ad_image_url', ''),
                        'image_media_id': row.get('ad_group_ad_image_media_id', 0),
                        
                        # Responsive Display Ad properties
                        'long_headline': row.get('ad_group_ad_long_headline', ''),
                        'marketing_images': marketing_images,
                        'square_marketing_images': square_marketing_images,
                        'logo_images': logo_images,
                        'square_logo_images': square_logo_images,
                        'business_name': row.get('ad_group_ad_business_name', '')
                    }
                    ads.append(ad)
                
                # Create ad nodes in batch
                if ads:
                    session.execute_write(self.create_entity_nodes_batch, 'Ad', ads)
                    
                    # Create relationships to AdGroup
                    adgroup_relationships = []
                    for _, row in batch.iterrows():
                        adgroup_relationships.append({
                            'start_key': 'ad_group_id',
                            'start_value': row['ad_group_id'],
                            'end_key': 'ad_id',
                            'end_value': row['ad_group_ad_ad_id']
                        })
                    
                    # Create relationships in batch
                    session.execute_write(
                        self.create_relationships_batch,
                        'AdGroup',
                        'Ad',
                        'CONTAINS',
                        adgroup_relationships
                    )

    def transform_ad_daily_metrics(self, ad_legacy_df: pd.DataFrame):
        """Transform ad performance data from ad_group_ad_legacy into AdDailyMetric nodes linked to Ads."""
        logger.info(f"Starting AdDailyMetric transformation for {len(ad_legacy_df)} ad legacy records.")

        # Ensure required columns are present (using ad_group_ad_legacy columns)
        required_cols = ['ad_group_ad_ad_id', 'segments_date']
        if not all(col in ad_legacy_df.columns for col in required_cols):
            logger.warning(f"Skipping AdDailyMetric transformation: Missing required columns (need {required_cols}) from ad_group_ad_legacy")
            return
        
        # Drop rows where ad_id or date is missing
        ad_legacy_df = ad_legacy_df.dropna(subset=['ad_group_ad_ad_id', 'segments_date'])
        if ad_legacy_df.empty:
            logger.info("No valid ad legacy records with ad_id and date found for AdDailyMetric transformation.")
            return

        # Standardize metric column names (should be consistent)
        metric_cols = {
            'impressions': 'impressions',
            'clicks': 'clicks',
            'cost_micros': 'cost_micros',
            'ctr': 'ctr',
            'average_cpc': 'average_cpc',
            'conversions': 'conversions',
            'conversions_value': 'conversions_value',
            'cost_per_conversion': 'cost_per_conversion',
            'value_per_conversion': 'value_per_conversion',
            'all_conversions': 'all_conversions',
            'all_conversions_value': 'all_conversions_value',
            'view_through_conversions': 'view_through_conversions',
            'interactions': 'interactions',
            'interaction_rate': 'interaction_rate',
            'average_cpm': 'average_cpm'
            # Add other relevant metrics from your schema here
        }

        with self.driver.session() as session:
            for start_idx in range(0, len(ad_legacy_df), self.BATCH_SIZE):
                batch = ad_legacy_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                daily_metrics_nodes = []
                relationships = []

                for _, row in batch.iterrows():
                    # Use the correct ad_id column name from ad_group_ad_legacy
                    ad_id = row['ad_group_ad_ad_id'] 
                    metric_date = row['segments_date']
                    
                    metric_node = {
                        'ad_id': ad_id, # Link to the Ad node
                        'date': metric_date
                        # Add ad_resource_name if available and desired
                        # 'ad_resource_name': row.get('ad_group_ad_resource_name', '') 
                    }
                    
                    has_metrics = False
                    for source_col_suffix, target_prop in metric_cols.items():
                        source_col = f'metrics_{source_col_suffix}'
                        if source_col in row and pd.notna(row[source_col]):
                            value = row[source_col]
                            # Basic type conversion
                            # Handle currency/value/cpc/cpm -> float (and convert micros)
                            if 'micros' in source_col or 'value' in source_col or 'cpc' in source_col or 'cpm' in source_col:
                                try:
                                    metric_node[target_prop] = float(value) 
                                    if 'micros' in source_col:
                                        metric_node[target_prop] /= 1000000
                                    has_metrics = True
                                except ValueError:
                                    logger.warning(f"Could not convert {target_prop} to float for ad {ad_id}, date {metric_date}. Value: {value}")
                            # FIX: Handle specific rates (ctr, interaction_rate) -> float
                            elif target_prop in ['ctr', 'interaction_rate']:
                                try:
                                    metric_node[target_prop] = float(value)
                                    has_metrics = True
                                except ValueError:
                                    logger.warning(f"Could not convert {target_prop} to float for ad {ad_id}, date {metric_date}. Value: {value}")
                            # Handle counts/integers -> int
                            elif target_prop in ['impressions', 'clicks', 'conversions', 'all_conversions', 'view_through_conversions', 'interactions']:
                                try:
                                    metric_node[target_prop] = int(value)
                                    has_metrics = True
                                except ValueError:
                                     logger.warning(f"Could not convert {target_prop} to int for ad {ad_id}, date {metric_date}. Value: {value}")
                            # Fallback for any other types -> string
                            else:
                                metric_node[target_prop] = str(value)
                                has_metrics = True # Still record it even if it's a string

                    # Only add node and relationship if we successfully parsed at least one metric
                    if has_metrics:
                        daily_metrics_nodes.append(metric_node)
                        relationships.append({
                            'start_key': 'ad_id', 
                            'start_value': ad_id,
                            'end_key': 'ad_id', 
                            'end_value': ad_id,
                            'date_key': 'date',
                            'date_value': metric_date
                        })

                if daily_metrics_nodes:
                    # Create DailyMetric nodes
                    logger.debug(f"Creating batch of {len(daily_metrics_nodes)} DailyMetric nodes.")
                    session.execute_write(self.create_entity_nodes_batch, 'AdDailyMetric', daily_metrics_nodes)
                    
                    # Create relationships: Ad -> DailyMetric
                    # Adjust query for composite key matching on the target node
                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:Ad {ad_id: rel.start_value})
                    MATCH (end:AdDailyMetric {ad_id: rel.end_value, date: rel.date_value})
                    MERGE (start)-[r:HAS_DAILY_METRICS]->(end)
                    """
                    logger.debug(f"Creating batch of {len(relationships)} HAS_DAILY_METRICS relationships.")
                    session.run(rel_query, {'relationships': relationships})

            logger.info("Completed AdDailyMetric transformation.")

    def transform_ad_overall_metrics(self, ad_legacy_df: pd.DataFrame):
        """Aggregates daily Ad performance data to overall metrics and creates AdOverallMetric nodes."""
        logger.info(f"Starting AdOverallMetric aggregation and transformation for {len(ad_legacy_df)} ad legacy records.")

        required_cols = ['ad_group_ad_ad_id', 'segments_date'] # Need date to know timeframe
        if not all(col in ad_legacy_df.columns for col in required_cols):
            logger.warning(f"Skipping AdOverallMetric transformation: Missing required columns (need {required_cols}) from ad_group_ad_legacy")
            return

        ad_legacy_df = ad_legacy_df.dropna(subset=['ad_group_ad_ad_id'])
        if ad_legacy_df.empty:
            logger.info("No valid ad legacy records with ad_id found for overall aggregation.")
            return

        # Define metrics to SUM
        sum_metrics = [
            'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
            'metrics_conversions', 'metrics_conversions_value',
            'metrics_all_conversions', 'metrics_all_conversions_value',
            'metrics_view_through_conversions', 'metrics_interactions'
        ]
        # Add any other relevant metrics if needed

        # Ensure columns exist, fill missing with 0
        for metric_col in sum_metrics:
            if metric_col not in ad_legacy_df.columns:
                ad_legacy_df[metric_col] = 0
            else:
                # Convert to numeric, coercing errors and filling NaN with 0
                ad_legacy_df[metric_col] = pd.to_numeric(ad_legacy_df[metric_col], errors='coerce').fillna(0)

        # Group by Ad ID
        agg_funcs = {col: 'sum' for col in sum_metrics}
        # Add min/max date to track the period covered
        agg_funcs['segments_date'] = ['min', 'max', 'count'] 
        
        try:
            overall_agg = ad_legacy_df.groupby('ad_group_ad_ad_id', as_index=False).agg(agg_funcs)
            # Flatten MultiIndex columns
            overall_agg.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) and col[1] else col[0] for col in overall_agg.columns.values]
            overall_agg = overall_agg.rename(columns={
                'ad_group_ad_ad_id': 'ad_id',
                'segments_date_min': 'first_metric_date',
                'segments_date_max': 'last_metric_date',
                'segments_date_count': 'days_with_metrics'
            })
        except Exception as e:
            logger.error(f"Error during pandas aggregation for ad overall metrics: {e}")
            return

        # Calculate overall ratios from the SUMS
        # Corrected column names to access aggregated data (e.g., metrics_clicks_sum)
        overall_agg['ctr'] = (overall_agg['metrics_clicks_sum'] / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['average_cpc'] = (overall_agg['metrics_cost_micros_sum'] / overall_agg['metrics_clicks_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['average_cpm'] = (overall_agg['metrics_cost_micros_sum'] * 1000 / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['cost_per_conversion'] = (overall_agg['metrics_cost_micros_sum'] / overall_agg['metrics_conversions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['value_per_conversion'] = (overall_agg['metrics_conversions_value_sum'] / overall_agg['metrics_conversions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['interaction_rate'] = (overall_agg['metrics_interactions_sum'] / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['all_conversions_value_per_cost'] = (overall_agg['metrics_all_conversions_value_sum'] / overall_agg['metrics_cost_micros_sum']).fillna(0).replace([float('inf'), -float('inf')], 0) 

        # Prepare nodes and relationships
        with self.driver.session() as session:
            for start_idx in range(0, len(overall_agg), self.BATCH_SIZE):
                batch = overall_agg.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                overall_metric_nodes = []
                relationships = []
                
                for _, row in batch.iterrows():
                    node = {
                        'ad_id': row['ad_id'],
                        'first_metric_date': str(row['first_metric_date']), # Convert date to string
                        'last_metric_date': str(row['last_metric_date']),   # Convert date to string
                        'days_with_metrics': int(row['days_with_metrics'])
                    }
                    # Add summed metrics (convert micros)
                    for col in sum_metrics:
                        prop_name = col.replace('metrics_', '')
                        # Correctly access the aggregated column with _sum suffix
                        value = float(row[f'{col}_sum'])
                        if 'micros' in prop_name:
                            node[prop_name] = value / 1000000
                        else:
                            node[prop_name] = value
                            
                    # Add calculated ratios
                    node['ctr'] = float(row['ctr'])
                    node['average_cpc'] = float(row['average_cpc']) / 1000000 # Convert micros
                    node['average_cpm'] = float(row['average_cpm']) / 1000000 # Convert micros
                    node['cost_per_conversion'] = float(row['cost_per_conversion']) / 1000000 # Convert micros
                    node['value_per_conversion'] = float(row['value_per_conversion'])
                    node['interaction_rate'] = float(row['interaction_rate'])
                    node['all_conversions_value_per_cost'] = float(row['all_conversions_value_per_cost']) # Based on micros cost

                    # Clean up potential residual NaN/Inf
                    for key, value in node.items():
                        if isinstance(value, float) and (pd.isna(value) or value == float('inf') or value == -float('inf')):
                            node[key] = 0.0
                            
                    overall_metric_nodes.append(node)
                    relationships.append({
                        'start_key': 'ad_id',
                        'start_value': row['ad_id'],
                        'end_key': 'ad_id',
                        'end_value': row['ad_id']
                    })

                if overall_metric_nodes:
                    logger.debug(f"Creating batch of {len(overall_metric_nodes)} AdOverallMetric nodes.")
                    session.execute_write(self.create_entity_nodes_batch, 'AdOverallMetric', overall_metric_nodes)

                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:Ad {ad_id: rel.start_value})
                    MATCH (end:AdOverallMetric {ad_id: rel.end_value})
                    MERGE (start)-[r:HAS_OVERALL_METRICS]->(end)
                    """
                    logger.debug(f"Creating batch of {len(relationships)} HAS_OVERALL_METRICS relationships for Ad.")
                    session.run(rel_query, {'relationships': relationships})
                    
            logger.info("Completed AdOverallMetric transformation.")

    def transform_ad_monthly_metrics(self, ad_legacy_df: pd.DataFrame):
        """Aggregates daily Ad performance data to monthly metrics and creates AdMonthlyMetric nodes."""
        logger.info(f"Starting AdMonthlyMetric aggregation and transformation for {len(ad_legacy_df)} ad legacy records.")

        required_cols = ['ad_group_ad_ad_id', 'segments_date']
        if not all(col in ad_legacy_df.columns for col in required_cols):
            logger.warning(f"Skipping AdMonthlyMetric transformation: Missing required columns (need {required_cols}) from ad_group_ad_legacy")
            return

        ad_legacy_df = ad_legacy_df.dropna(subset=['ad_group_ad_ad_id', 'segments_date'])
        if ad_legacy_df.empty:
            logger.info("No valid ad legacy records with ad_id and date found for monthly aggregation.")
            return

        # Convert segments_date to datetime and calculate month start date
        try:
            ad_legacy_df['date'] = pd.to_datetime(ad_legacy_df['segments_date'])
            ad_legacy_df['month_start_date'] = ad_legacy_df['date'].dt.to_period('M').dt.start_time.dt.strftime('%Y-%m-%d')
        except Exception as e:
             logger.error(f"Error processing dates for ad monthly aggregation: {e}")
             return

        # Define metrics to SUM
        sum_metrics = [
            'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
            'metrics_conversions', 'metrics_conversions_value',
            'metrics_all_conversions', 'metrics_all_conversions_value',
            'metrics_view_through_conversions', 'metrics_interactions'
        ]

        # Ensure columns exist, fill missing with 0
        for metric_col in sum_metrics:
            if metric_col not in ad_legacy_df.columns:
                ad_legacy_df[metric_col] = 0
            else:
                ad_legacy_df[metric_col] = pd.to_numeric(ad_legacy_df[metric_col], errors='coerce').fillna(0)

        # Group by Ad ID and Month Start Date
        agg_funcs = {col: 'sum' for col in sum_metrics}
        agg_funcs['date'] = 'count' # Count days aggregated in month
        
        try:
             monthly_agg = ad_legacy_df.groupby(['ad_group_ad_ad_id', 'month_start_date'], as_index=False).agg(agg_funcs)
             monthly_agg = monthly_agg.rename(columns={'ad_group_ad_ad_id': 'ad_id', 'date': 'days_aggregated'})
        except Exception as e:
            logger.error(f"Error during pandas aggregation for ad monthly metrics: {e}")
            return
            
        # Calculate monthly ratios from SUMS
        monthly_agg['ctr'] = (monthly_agg['metrics_clicks'] / monthly_agg['metrics_impressions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['average_cpc'] = (monthly_agg['metrics_cost_micros'] / monthly_agg['metrics_clicks']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['average_cpm'] = (monthly_agg['metrics_cost_micros'] * 1000 / monthly_agg['metrics_impressions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['cost_per_conversion'] = (monthly_agg['metrics_cost_micros'] / monthly_agg['metrics_conversions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['value_per_conversion'] = (monthly_agg['metrics_conversions_value'] / monthly_agg['metrics_conversions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['interaction_rate'] = (monthly_agg['metrics_interactions'] / monthly_agg['metrics_impressions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['all_conversions_value_per_cost'] = (monthly_agg['metrics_all_conversions_value'] / monthly_agg['metrics_cost_micros']).fillna(0).replace([float('inf'), -float('inf')], 0)
        # Prepare nodes and relationships
        with self.driver.session() as session:
            for start_idx in range(0, len(monthly_agg), self.BATCH_SIZE):
                batch = monthly_agg.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                monthly_metric_nodes = []
                relationships = []
                
                for _, row in batch.iterrows():
                    node = {
                        'ad_id': row['ad_id'],
                        'month_start_date': row['month_start_date'],
                        'days_aggregated': int(row['days_aggregated'])
                    }
                    # Add summed metrics
                    for col in sum_metrics:
                        prop_name = col.replace('metrics_', '')
                        # Access the original column name (no suffix)
                        if col in row: # Check if the original column exists
                            value = float(row[col])
                            if 'micros' in prop_name:
                               node[prop_name] = value / 1000000
                            else:
                                node[prop_name] = value
                        else:
                            node[prop_name] = 0.0 # Default if column somehow missing
                            
                    # Add calculated ratios
                    node['ctr'] = float(row['ctr'])
                    node['average_cpc'] = float(row['average_cpc']) / 1000000 
                    node['average_cpm'] = float(row['average_cpm']) / 1000000
                    node['cost_per_conversion'] = float(row['cost_per_conversion']) / 1000000
                    node['value_per_conversion'] = float(row['value_per_conversion'])
                    node['interaction_rate'] = float(row['interaction_rate'])
                    node['all_conversions_value_per_cost'] = float(row['all_conversions_value_per_cost'])

                    # Clean up potential NaN/Inf
                    for key, value in node.items():
                        if isinstance(value, float) and (pd.isna(value) or value == float('inf') or value == -float('inf')):
                            node[key] = 0.0
                            
                    monthly_metric_nodes.append(node)
                    relationships.append({
                            'start_key': 'ad_id',
                        'start_value': row['ad_id'],
                        'end_key': 'ad_id',
                        'end_value': row['ad_id'],
                        'month_key': 'month_start_date',
                        'month_value': row['month_start_date']
                    })

                if monthly_metric_nodes:
                    logger.debug(f"Creating batch of {len(monthly_metric_nodes)} AdMonthlyMetric nodes.")
                    session.execute_write(self.create_entity_nodes_batch, 'AdMonthlyMetric', monthly_metric_nodes)

                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:Ad {ad_id: rel.start_value})
                    MATCH (end:AdMonthlyMetric {ad_id: rel.end_value, month_start_date: rel.month_value})
                    MERGE (start)-[r:HAS_MONTHLY_METRICS]->(end)
                    """
                    logger.debug(f"Creating batch of {len(relationships)} HAS_MONTHLY_METRICS relationships for Ad.")
                    session.run(rel_query, {'relationships': relationships})
                    
            logger.info("Completed AdMonthlyMetric transformation.")

    def transform_account_monthly_metrics(self, account_perf_df: pd.DataFrame):
        """Aggregates daily Account performance data to monthly metrics and creates AccountMonthlyMetric nodes."""
        logger.info(f"Starting AccountMonthlyMetric aggregation and transformation for {len(account_perf_df)} account performance records.")

        required_cols = ['customer_id', 'segments_date']
        if not all(col in account_perf_df.columns for col in required_cols):
            logger.warning(f"Skipping AccountMonthlyMetric transformation: Missing required columns (need {required_cols}) from account_performance_report")
            return

        account_perf_df = account_perf_df.dropna(subset=['customer_id', 'segments_date'])
        if account_perf_df.empty:
            logger.info("No valid account performance records with customer_id and date found for monthly aggregation.")
            return

        # Convert segments_date to datetime and calculate month start date
        try:
            account_perf_df['date'] = pd.to_datetime(account_perf_df['segments_date'])
            account_perf_df['month_start_date'] = account_perf_df['date'].dt.to_period('M').dt.start_time.dt.strftime('%Y-%m-%d')
        except Exception as e:
             logger.error(f"Error processing dates for account monthly aggregation: {e}")
             return

        # Define metrics to SUM
        sum_metrics = [
            'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
            'metrics_conversions', 'metrics_conversions_value',
            'metrics_all_conversions', 'metrics_all_conversions_value',
            'metrics_view_through_conversions', 'metrics_interactions'
        ]
        
        # Define metrics to AVERAGE (Simple Average - Use API for accuracy if possible)
        avg_metrics = [
             'metrics_search_impression_share', 'metrics_search_budget_lost_impression_share',
             'metrics_search_rank_lost_impression_share', 'metrics_content_impression_share',
             'metrics_content_budget_lost_impression_share', 'metrics_content_rank_lost_impression_share'
        ]

        # Ensure columns exist, fill missing with 0
        for metric_col in sum_metrics + avg_metrics:
            if metric_col not in account_perf_df.columns:
                account_perf_df[metric_col] = 0
            else:
                account_perf_df[metric_col] = pd.to_numeric(account_perf_df[metric_col], errors='coerce').fillna(0)

        # Group by Account and Month Start Date
        agg_funcs = {col: 'sum' for col in sum_metrics}
        for col in avg_metrics:
             agg_funcs[col] = 'mean' 
        agg_funcs['date'] = 'count' # Count days aggregated
        
        try:
             monthly_agg = account_perf_df.groupby(['customer_id', 'month_start_date'], as_index=False).agg(agg_funcs)
             monthly_agg = monthly_agg.rename(columns={'customer_id': 'account_id', 'date': 'days_aggregated'})
        except Exception as e:
            logger.error(f"Error during pandas aggregation for account monthly metrics: {e}")
            return
            
        # Calculate monthly ratios
        monthly_agg['ctr'] = (monthly_agg['metrics_clicks'] / monthly_agg['metrics_impressions']).fillna(0)
        monthly_agg['average_cpc'] = (monthly_agg['metrics_cost_micros'] / monthly_agg['metrics_clicks']).fillna(0)
        monthly_agg['cost_per_conversion'] = (monthly_agg['metrics_cost_micros'] / monthly_agg['metrics_conversions']).fillna(0)
        monthly_agg['value_per_conversion'] = (monthly_agg['metrics_conversions_value'] / monthly_agg['metrics_conversions']).fillna(0)
        monthly_agg['interaction_rate'] = (monthly_agg['metrics_interactions'] / monthly_agg['metrics_impressions']).fillna(0)
        
        monthly_agg.replace([pd.NA, float('inf'), -float('inf')], 0, inplace=True)

        # Prepare nodes and relationships
        with self.driver.session() as session:
            for start_idx in range(0, len(monthly_agg), self.BATCH_SIZE):
                batch = monthly_agg.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                monthly_metric_nodes = []
                relationships = []
                
                for _, row in batch.iterrows():
                    node = {
                        'account_id': row['account_id'],
                        'month_start_date': row['month_start_date'],
                        'days_aggregated': int(row['days_aggregated'])
                        # Add account_resource_name if needed (would require join with customer table before aggregation)
                    }
                    # Add summed metrics
                    for col in sum_metrics:
                        prop_name = col.replace('metrics_', '')
                        node[prop_name] = float(row[col])
                        if 'micros' in prop_name:
                           node[prop_name] /= 1000000
                            
                    # Add averaged metrics
                    for col in avg_metrics:
                        prop_name = col.replace('metrics_', '')
                        node[prop_name] = float(row[col])
                            
                    # Add calculated ratios
                    node['ctr'] = float(row['ctr'])
                    node['average_cpc'] = float(row['average_cpc'])
                    node['cost_per_conversion'] = float(row['cost_per_conversion'])
                    node['value_per_conversion'] = float(row['value_per_conversion'])
                    node['interaction_rate'] = float(row['interaction_rate'])
                    
                    # Clean up potential NaN/Inf
                    for key, value in node.items():
                        if pd.isna(value) or value == float('inf') or value == -float('inf'):
                            node[key] = 0.0
                            
                    monthly_metric_nodes.append(node)
                    relationships.append({
                        'start_key': 'account_id',
                        'start_value': row['account_id'],
                        'end_key': 'account_id',
                        'end_value': row['account_id'],
                        'month_key': 'month_start_date',
                        'month_value': row['month_start_date']
                    })

                if monthly_metric_nodes:
                    logger.debug(f"Creating batch of {len(monthly_metric_nodes)} AccountMonthlyMetric nodes.")
                    session.execute_write(self.create_entity_nodes_batch, 'AccountMonthlyMetric', monthly_metric_nodes)

                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:AdAccount {account_id: rel.start_value})
                    MATCH (end:AccountMonthlyMetric {account_id: rel.end_value, month_start_date: rel.month_value})
                    MERGE (start)-[r:HAS_MONTHLY_METRICS]->(end)
                    """
                    logger.debug(f"Creating batch of {len(relationships)} HAS_MONTHLY_METRICS relationships for Account.")
                    session.run(rel_query, {'relationships': relationships})
                    
            logger.info("Completed AccountMonthlyMetric transformation.")

    def transform_account_overall_metrics(self, account_perf_df: pd.DataFrame):
        """Aggregates daily Account performance data to overall metrics and creates AccountOverallMetric nodes."""
        logger.info(f"Starting AccountOverallMetric aggregation and transformation for {len(account_perf_df)} account performance records.")

        required_cols = ['customer_id', 'segments_date'] # Need date to know timeframe
        if not all(col in account_perf_df.columns for col in required_cols):
            logger.warning(f"Skipping AccountOverallMetric transformation: Missing required columns (need {required_cols}) from account_performance_report")
            return

        account_perf_df = account_perf_df.dropna(subset=['customer_id'])
        if account_perf_df.empty:
            logger.info("No valid account performance records with customer_id found for overall aggregation.")
            return

        # Define metrics to SUM
        sum_metrics = [
            'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
            'metrics_conversions', 'metrics_conversions_value',
            'metrics_all_conversions', 'metrics_all_conversions_value',
            'metrics_view_through_conversions', 'metrics_interactions'
        ]
        
        # Define metrics to AVERAGE (Simple Average - Use API for accuracy if possible)
        avg_metrics = [
             'metrics_search_impression_share', 'metrics_search_budget_lost_impression_share',
             'metrics_search_rank_lost_impression_share', 'metrics_content_impression_share',
             'metrics_content_budget_lost_impression_share', 'metrics_content_rank_lost_impression_share'
        ]

        # Ensure columns exist, fill missing with 0
        for metric_col in sum_metrics + avg_metrics:
            if metric_col not in account_perf_df.columns:
                account_perf_df[metric_col] = 0
            else:
                account_perf_df[metric_col] = pd.to_numeric(account_perf_df[metric_col], errors='coerce').fillna(0)

        # Group by Account ID
        agg_funcs = {col: 'sum' for col in sum_metrics}
        for col in avg_metrics:
            agg_funcs[col] = 'mean'
        agg_funcs['segments_date'] = ['min', 'max', 'count']
        
        try:
            overall_agg = account_perf_df.groupby('customer_id', as_index=False).agg(agg_funcs)
            # Flatten MultiIndex columns
            overall_agg.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) and col[1] else col[0] for col in overall_agg.columns.values]
            overall_agg = overall_agg.rename(columns={
                'customer_id': 'account_id', # Rename customer_id to account_id
                'segments_date_min': 'first_metric_date',
                'segments_date_max': 'last_metric_date',
                'segments_date_count': 'days_with_metrics'
            })
        except Exception as e:
            logger.error(f"Error during pandas aggregation for account overall metrics: {e}")
            return

        # Calculate overall ratios from the SUMS
        overall_agg['ctr'] = (overall_agg['metrics_clicks_sum'] / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['average_cpc'] = (overall_agg['metrics_cost_micros_sum'] / overall_agg['metrics_clicks_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['average_cpm'] = (overall_agg['metrics_cost_micros_sum'] * 1000 / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['cost_per_conversion'] = (overall_agg['metrics_cost_micros_sum'] / overall_agg['metrics_conversions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['value_per_conversion'] = (overall_agg['metrics_conversions_value_sum'] / overall_agg['metrics_conversions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['interaction_rate'] = (overall_agg['metrics_interactions_sum'] / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['all_conversions_value_per_cost'] = (overall_agg['metrics_all_conversions_value_sum'] / overall_agg['metrics_cost_micros_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        # Prepare nodes and relationships
        with self.driver.session() as session:
            for start_idx in range(0, len(overall_agg), self.BATCH_SIZE):
                batch = overall_agg.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                overall_metric_nodes = []
                relationships = []
                
                for _, row in batch.iterrows():
                    node = {
                        'account_id': row['account_id'],
                        'first_metric_date': str(row['first_metric_date']),
                        'last_metric_date': str(row['last_metric_date']),
                        'days_with_metrics': int(row['days_with_metrics'])
                    }
                    # Add summed metrics (convert micros)
                    for col in sum_metrics:
                        prop_name = col.replace('metrics_', '')
                        # Correctly access the aggregated column with _sum suffix
                        value = float(row[f'{col}_sum'])
                        if 'micros' in prop_name:
                            node[prop_name] = value / 1000000
                        else:
                            node[prop_name] = value
                            
                    # Add averaged metrics
                    for col in avg_metrics:
                        prop_name = col.replace('metrics_', '')
                        # Correctly access the aggregated column with _mean suffix
                        node[prop_name] = float(row[f'{col}_mean'])

                    # Add calculated ratios
                    node['ctr'] = float(row['ctr'])
                    node['average_cpc'] = float(row['average_cpc']) / 1000000 # Convert micros
                    node['average_cpm'] = float(row['average_cpm']) / 1000000 # Convert micros
                    node['cost_per_conversion'] = float(row['cost_per_conversion']) / 1000000 # Convert micros
                    node['value_per_conversion'] = float(row['value_per_conversion'])
                    node['interaction_rate'] = float(row['interaction_rate'])
                    node['all_conversions_value_per_cost'] = float(row['all_conversions_value_per_cost']) # Based on micros cost

                    # Clean up potential residual NaN/Inf
                    for key, value in node.items():
                        if isinstance(value, float) and (pd.isna(value) or value == float('inf') or value == -float('inf')):
                            node[key] = 0.0
                            
                    overall_metric_nodes.append(node)
                    relationships.append({
                        'start_key': 'account_id',
                        'start_value': row['account_id'],
                        'end_key': 'account_id',
                        'end_value': row['account_id']
                    })

                if overall_metric_nodes:
                    logger.debug(f"Creating batch of {len(overall_metric_nodes)} AccountOverallMetric nodes.")
                    session.execute_write(self.create_entity_nodes_batch, 'AccountOverallMetric', overall_metric_nodes)

                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:AdAccount {account_id: rel.start_value})
                    MATCH (end:AccountOverallMetric {account_id: rel.end_value})
                    MERGE (start)-[r:HAS_OVERALL_METRICS]->(end)
                    """
                    logger.debug(f"Creating batch of {len(relationships)} HAS_OVERALL_METRICS relationships for Account.")
                    session.run(rel_query, {'relationships': relationships})
                    
            logger.info("Completed AccountOverallMetric transformation.")

    def transform_campaign_weekly_metrics(self, campaign_df: pd.DataFrame):
        """Aggregates daily Campaign data to weekly metrics and creates WeeklyMetric nodes linked to Campaigns."""
        logger.info(f"Starting CampaignWeeklyMetric aggregation and transformation for {len(campaign_df)} campaign records.")

        required_cols = ['campaign_id', 'segments_date'] # Use campaign_id
        if not all(col in campaign_df.columns for col in required_cols):
            logger.warning(f"Skipping CampaignWeeklyMetric transformation: Missing required columns (need {required_cols}) from campaign table.")
            return

        # Drop rows missing key info
        campaign_df = campaign_df.dropna(subset=['campaign_id', 'segments_date'])
        if campaign_df.empty:
            logger.info("No valid campaign records with campaign_id and date found for weekly aggregation.")
            return
            
        # Convert segments_date to datetime and calculate week start date (Monday)
        try:
            campaign_df['date'] = pd.to_datetime(campaign_df['segments_date'])
            campaign_df['week_start_date'] = campaign_df['date'] - pd.to_timedelta(campaign_df['date'].dt.dayofweek, unit='d')
            campaign_df['week_start_date_str'] = campaign_df['week_start_date'].dt.strftime('%Y-%m-%d')
        except Exception as e:
             logger.error(f"Error processing dates for campaign weekly aggregation: {e}")
             return

        # Define metrics to SUM
        sum_metrics = [
            'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
            'metrics_conversions', 'metrics_conversions_value',
            'metrics_all_conversions', 'metrics_all_conversions_value',
            'metrics_view_through_conversions', 'metrics_interactions'
            # Add campaign specific metrics like search_impression_share etc. if available daily
        ]
        
        # Define metrics to AVERAGE (handle carefully, prefer direct weekly API query if possible, esp. for IS)
        avg_metrics = [
             'metrics_search_impression_share', 'metrics_search_budget_lost_impression_share',
             'metrics_search_rank_lost_impression_share', 'metrics_content_impression_share',
             'metrics_content_budget_lost_impression_share', 'metrics_content_rank_lost_impression_share'
        ]

        # Ensure metric columns exist, fill missing with 0 for aggregation
        for metric_col in sum_metrics + avg_metrics:
            if metric_col not in campaign_df.columns:
                campaign_df[metric_col] = 0 # Or pd.NA if preferred for averaging
            else:
                # Ensure numeric, coercing errors and filling NaN with 0
                campaign_df[metric_col] = pd.to_numeric(campaign_df[metric_col], errors='coerce').fillna(0)

        # Group by Campaign and Week Start Date
        agg_funcs = {col: 'sum' for col in sum_metrics}
        # Use mean for average metrics - NOTE: this is a simple average, weighted is better if possible
        for col in avg_metrics:
             agg_funcs[col] = 'mean' 
        agg_funcs['date'] = 'count' # Count days aggregated
        
        try:
             # Use campaign_id for grouping
             weekly_agg = campaign_df.groupby(['campaign_id', 'week_start_date_str'], as_index=False).agg(agg_funcs)
             weekly_agg = weekly_agg.rename(columns={'date': 'days_aggregated'})
        except Exception as e:
            logger.error(f"Error during pandas aggregation for campaign weekly metrics: {e}")
            return
            
        # Calculate weekly ratios from SUMS
        weekly_agg['ctr'] = (weekly_agg['metrics_clicks'] / weekly_agg['metrics_impressions']).fillna(0)
        weekly_agg['average_cpc'] = (weekly_agg['metrics_cost_micros'] / weekly_agg['metrics_clicks']).fillna(0)
        weekly_agg['cost_per_conversion'] = (weekly_agg['metrics_cost_micros'] / weekly_agg['metrics_conversions']).fillna(0)
        weekly_agg['value_per_conversion'] = (weekly_agg['metrics_conversions_value'] / weekly_agg['metrics_conversions']).fillna(0)
        weekly_agg['interaction_rate'] = (weekly_agg['metrics_interactions'] / weekly_agg['metrics_impressions']).fillna(0)
        
        # Replace inf with 0 
        weekly_agg.replace([pd.NA, float('inf'), -float('inf')], 0, inplace=True)

        # Prepare nodes and relationships for Neo4j
        with self.driver.session() as session:
            for start_idx in range(0, len(weekly_agg), self.BATCH_SIZE):
                batch = weekly_agg.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                weekly_metric_nodes = []
                relationships = []

                for _, row in batch.iterrows():
                    node = {
                        'campaign_id': row['campaign_id'], # Use campaign_id
                        'week_start_date': row['week_start_date_str'], 
                        'days_aggregated': int(row['days_aggregated']),
                        'entity_type': 'Campaign' # Add entity type identifier
                    }
                    # Add summed metrics 
                    for col in sum_metrics:
                        prop_name = col.replace('metrics_', '')
                        node[prop_name] = float(row[col])
                        if 'micros' in prop_name:
                           node[prop_name] /= 1000000
                            
                    # Add averaged metrics (e.g., Impression Share - use with caution)
                    for col in avg_metrics:
                        prop_name = col.replace('metrics_', '')
                        node[prop_name] = float(row[col]) # Result of .mean()
                            
                    # Add calculated ratios
                    node['ctr'] = float(row['ctr'])
                    node['average_cpc'] = float(row['average_cpc'])
                    node['cost_per_conversion'] = float(row['cost_per_conversion'])
                    node['value_per_conversion'] = float(row['value_per_conversion'])
                    node['interaction_rate'] = float(row['interaction_rate'])
                    
                    # Clean up potential NaN/Inf again 
                    for key, value in node.items():
                        if pd.isna(value) or value == float('inf') or value == -float('inf'):
                            node[key] = 0.0 # Use float 0.0
                            
                    weekly_metric_nodes.append(node)
                    relationships.append({
                        'start_key': 'campaign_id', # Use campaign_id
                        'start_value': row['campaign_id'],
                        'end_key': 'campaign_id',
                        'end_value': row['campaign_id'],
                        'week_key': 'week_start_date',
                        'week_value': row['week_start_date_str']
                    })

                if weekly_metric_nodes:
                    logger.debug(f"Creating batch of {len(weekly_metric_nodes)} Campaign WeeklyMetric nodes.")
                    # Use the generic :WeeklyMetric label here
                    session.execute_write(self.create_entity_nodes_batch, 'WeeklyMetric', weekly_metric_nodes)

                    # Adjust query to match Campaign and WeeklyMetric based on campaign_id and week
                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:Campaign {campaign_id: rel.start_value})
                    MATCH (end:WeeklyMetric {campaign_id: rel.end_value, week_start_date: rel.week_value, entity_type: 'Campaign'})
                    MERGE (start)-[r:HAS_WEEKLY_METRICS]->(end)
                    """
                    logger.debug(f"Creating batch of {len(relationships)} HAS_WEEKLY_METRICS relationships for Campaign.")
                    session.run(rel_query, {'relationships': relationships})
                    
            logger.info("Completed CampaignWeeklyMetric transformation.")

    def transform_campaign_overall_metrics(self, campaign_df: pd.DataFrame):
        """Aggregates daily Campaign performance data to overall metrics and creates CampaignOverallMetric nodes."""
        logger.info(f"Starting CampaignOverallMetric aggregation and transformation using {len(campaign_df)} campaign records.")

        required_cols = ['campaign_id', 'segments_date'] # Need date to know timeframe
        if not all(col in campaign_df.columns for col in required_cols):
            logger.warning(f"Skipping CampaignOverallMetric transformation: Missing required columns (need {required_cols}) from campaign table.")
            return

        campaign_df = campaign_df.dropna(subset=['campaign_id'])
        if campaign_df.empty:
            logger.info("No valid campaign records with campaign_id found for overall aggregation.")
            return

        # Define metrics to SUM (add campaign specific ones if needed)
        sum_metrics = [
            'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
            'metrics_conversions', 'metrics_conversions_value',
            'metrics_all_conversions', 'metrics_all_conversions_value',
            'metrics_view_through_conversions', 'metrics_interactions'
            # Add metrics like metrics_video_views if relevant to campaigns
        ]
        # Define metrics to AVERAGE (Simple Average - Use API for accuracy if possible, esp. IS)
        avg_metrics = [
             'metrics_search_impression_share', 'metrics_search_budget_lost_impression_share',
             'metrics_search_rank_lost_impression_share', 'metrics_content_impression_share',
             'metrics_content_budget_lost_impression_share', 'metrics_content_rank_lost_impression_share'
        ]

        # Ensure columns exist, fill missing with 0
        for metric_col in sum_metrics + avg_metrics:
            if metric_col not in campaign_df.columns:
                campaign_df[metric_col] = 0
            else:
                campaign_df[metric_col] = pd.to_numeric(campaign_df[metric_col], errors='coerce').fillna(0)

        # Group by Campaign ID
        agg_funcs = {col: 'sum' for col in sum_metrics}
        for col in avg_metrics:
            agg_funcs[col] = 'mean'
        agg_funcs['segments_date'] = ['min', 'max', 'count']
        
        try:
            overall_agg = campaign_df.groupby('campaign_id', as_index=False).agg(agg_funcs)
            # Flatten MultiIndex columns
            overall_agg.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) and col[1] else col[0] for col in overall_agg.columns.values]
            overall_agg = overall_agg.rename(columns={
                'segments_date_min': 'first_metric_date',
                'segments_date_max': 'last_metric_date',
                'segments_date_count': 'days_with_metrics'
            })
        except Exception as e:
            logger.error(f"Error during pandas aggregation for campaign overall metrics: {e}")
            return

        # Calculate overall ratios from the SUMS
        overall_agg['ctr'] = (overall_agg['metrics_clicks_sum'] / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['average_cpc'] = (overall_agg['metrics_cost_micros_sum'] / overall_agg['metrics_clicks_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['average_cpm'] = (overall_agg['metrics_cost_micros_sum'] * 1000 / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['cost_per_conversion'] = (overall_agg['metrics_cost_micros_sum'] / overall_agg['metrics_conversions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['value_per_conversion'] = (overall_agg['metrics_conversions_value_sum'] / overall_agg['metrics_conversions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['interaction_rate'] = (overall_agg['metrics_interactions_sum'] / overall_agg['metrics_impressions_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        overall_agg['all_conversions_value_per_cost'] = (overall_agg['metrics_all_conversions_value_sum'] / overall_agg['metrics_cost_micros_sum']).fillna(0).replace([float('inf'), -float('inf')], 0)
        # Prepare nodes and relationships
        with self.driver.session() as session:
            for start_idx in range(0, len(overall_agg), self.BATCH_SIZE):
                batch = overall_agg.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                overall_metric_nodes = []
                relationships = []
                
                for _, row in batch.iterrows():
                    node = {
                        'campaign_id': row['campaign_id'],
                        'first_metric_date': str(row['first_metric_date']),
                        'last_metric_date': str(row['last_metric_date']),
                        'days_with_metrics': int(row['days_with_metrics'])
                    }
                     # Add summed metrics (convert micros)
                    for col in sum_metrics:
                        prop_name = col.replace('metrics_', '')
                        # Correctly access the aggregated column with _sum suffix
                        agg_col_name = f"{col}_sum"
                        if agg_col_name in row: # Check if the suffixed column exists
                            # Use agg_col_name to access the row data
                            value = float(row[agg_col_name])
                            if 'micros' in prop_name:
                                node[prop_name] = value / 1000000
                            else:
                                node[prop_name] = value
                        else:
                             node[prop_name] = 0.0 # Default if column somehow missing

                    # Add averaged metrics (Impression Share, etc.)
                    for col in avg_metrics:
                        prop_name = col.replace('metrics_', '')
                        # Correctly access the aggregated column with _mean suffix
                        agg_col_name = f"{col}_mean"
                        if agg_col_name in row: # Check if the suffixed column exists
                            # Use agg_col_name to access the row data
                            node[prop_name] = float(row[agg_col_name])
                        else:
                            node[prop_name] = 0.0 # Default if column somehow missing

                    # Add calculated ratios
                    node['ctr'] = float(row['ctr'])
                    node['average_cpc'] = float(row['average_cpc']) / 1000000 
                    node['average_cpm'] = float(row['average_cpm']) / 1000000
                    node['cost_per_conversion'] = float(row['cost_per_conversion']) / 1000000
                    node['value_per_conversion'] = float(row['value_per_conversion'])
                    node['interaction_rate'] = float(row['interaction_rate'])
                    node['all_conversions_value_per_cost'] = float(row['all_conversions_value_per_cost'])

                    for key, value in node.items():
                        if isinstance(value, float) and (pd.isna(value) or value == float('inf') or value == -float('inf')):
                            node[key] = 0.0
                            
                    overall_metric_nodes.append(node)
                    relationships.append({
                        'start_key': 'campaign_id',
                        'start_value': row['campaign_id'],
                        'end_key': 'campaign_id',
                        'end_value': row['campaign_id']
                    })

                if overall_metric_nodes:
                    logger.debug(f"Creating batch of {len(overall_metric_nodes)} CampaignOverallMetric nodes.")
                    session.execute_write(self.create_entity_nodes_batch, 'CampaignOverallMetric', overall_metric_nodes)

                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:Campaign {campaign_id: rel.start_value})
                    MATCH (end:CampaignOverallMetric {campaign_id: rel.end_value})
                    MERGE (start)-[r:HAS_OVERALL_METRICS]->(end)
                    """
                    logger.debug(f"Creating batch of {len(relationships)} HAS_OVERALL_METRICS relationships for Campaign.")
                    session.run(rel_query, {'relationships': relationships})
                    
            logger.info("Completed CampaignOverallMetric transformation.")

    def transform_campaign_monthly_metrics(self, campaign_df: pd.DataFrame):
        """Aggregates daily Campaign performance data to monthly metrics and creates CampaignMonthlyMetric nodes."""
        logger.info(f"Starting CampaignMonthlyMetric aggregation and transformation using {len(campaign_df)} campaign records.")

        required_cols = ['campaign_id', 'segments_date']
        if not all(col in campaign_df.columns for col in required_cols):
            logger.warning(f"Skipping CampaignMonthlyMetric transformation: Missing required columns (need {required_cols}) from campaign table.")
            return

        campaign_df = campaign_df.dropna(subset=['campaign_id', 'segments_date'])
        if campaign_df.empty:
            logger.info("No valid campaign records with campaign_id and date found for monthly aggregation.")
            return

        # Convert segments_date to datetime and calculate month start date
        try:
            campaign_df['date'] = pd.to_datetime(campaign_df['segments_date'])
            campaign_df['month_start_date'] = campaign_df['date'].dt.to_period('M').dt.start_time.dt.strftime('%Y-%m-%d')
        except Exception as e:
             logger.error(f"Error processing dates for campaign monthly aggregation: {e}")
             return

        # Define metrics to SUM
        sum_metrics = [
            'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
            'metrics_conversions', 'metrics_conversions_value',
            'metrics_all_conversions', 'metrics_all_conversions_value',
            'metrics_view_through_conversions', 'metrics_interactions'
        ]
        # Define metrics to AVERAGE 
        avg_metrics = [
             'metrics_search_impression_share', 'metrics_search_budget_lost_impression_share',
             'metrics_search_rank_lost_impression_share', 'metrics_content_impression_share',
             'metrics_content_budget_lost_impression_share', 'metrics_content_rank_lost_impression_share'
        ]

        # Ensure columns exist, fill missing with 0
        for metric_col in sum_metrics + avg_metrics:
            if metric_col not in campaign_df.columns:
                campaign_df[metric_col] = 0
            else:
                campaign_df[metric_col] = pd.to_numeric(campaign_df[metric_col], errors='coerce').fillna(0)

        # Group by Campaign ID and Month Start Date
        agg_funcs = {col: 'sum' for col in sum_metrics}
        for col in avg_metrics:
            agg_funcs[col] = 'mean'
        agg_funcs['date'] = 'count' # Count days aggregated in month
        
        try:
             monthly_agg = campaign_df.groupby(['campaign_id', 'month_start_date'], as_index=False).agg(agg_funcs)
             monthly_agg = monthly_agg.rename(columns={'date': 'days_aggregated'})
        except Exception as e:
            logger.error(f"Error during pandas aggregation for campaign monthly metrics: {e}")
            return
            
        # Calculate monthly ratios from SUMS
        monthly_agg['ctr'] = (monthly_agg['metrics_clicks'] / monthly_agg['metrics_impressions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['average_cpc'] = (monthly_agg['metrics_cost_micros'] / monthly_agg['metrics_clicks']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['average_cpm'] = (monthly_agg['metrics_cost_micros'] * 1000 / monthly_agg['metrics_impressions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['cost_per_conversion'] = (monthly_agg['metrics_cost_micros'] / monthly_agg['metrics_conversions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['value_per_conversion'] = (monthly_agg['metrics_conversions_value'] / monthly_agg['metrics_conversions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['interaction_rate'] = (monthly_agg['metrics_interactions'] / monthly_agg['metrics_impressions']).fillna(0).replace([float('inf'), -float('inf')], 0)
        monthly_agg['all_conversions_value_per_cost'] = (monthly_agg['metrics_all_conversions_value'] / monthly_agg['metrics_cost_micros']).fillna(0).replace([float('inf'), -float('inf')], 0)

        # Prepare nodes and relationships
        with self.driver.session() as session:
            for start_idx in range(0, len(monthly_agg), self.BATCH_SIZE):
                batch = monthly_agg.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                monthly_metric_nodes = []
                relationships = []
                
                for _, row in batch.iterrows():
                    node = {
                        'campaign_id': row['campaign_id'],
                        'month_start_date': row['month_start_date'],
                        'days_aggregated': int(row['days_aggregated'])
                    }
                    # Add summed metrics
                    for col in sum_metrics:
                        prop_name = col.replace('metrics_', '')
                        value = float(row[col])
                        if 'micros' in prop_name:
                           node[prop_name] = value / 1000000
                        else:
                            node[prop_name] = value
                            
                    # Add averaged metrics
                    for col in avg_metrics:
                        prop_name = col.replace('metrics_', '')
                        node[prop_name] = float(row[col])

                    # Add calculated ratios
                    node['ctr'] = float(row['ctr'])
                    node['average_cpc'] = float(row['average_cpc']) / 1000000 
                    node['average_cpm'] = float(row['average_cpm']) / 1000000
                    node['cost_per_conversion'] = float(row['cost_per_conversion']) / 1000000
                    node['value_per_conversion'] = float(row['value_per_conversion'])
                    node['interaction_rate'] = float(row['interaction_rate'])
                    node['all_conversions_value_per_cost'] = float(row['all_conversions_value_per_cost'])

                    # Clean up potential NaN/Inf
                    for key, value in node.items():
                        if isinstance(value, float) and (pd.isna(value) or value == float('inf') or value == -float('inf')):
                            node[key] = 0.0
                            
                    monthly_metric_nodes.append(node)
                    relationships.append({
                        'start_key': 'campaign_id',
                        'start_value': row['campaign_id'],
                        'end_key': 'campaign_id',
                        'end_value': row['campaign_id'],
                        'month_key': 'month_start_date',
                        'month_value': row['month_start_date']
                    })

                if monthly_metric_nodes:
                    logger.debug(f"Creating batch of {len(monthly_metric_nodes)} CampaignMonthlyMetric nodes.")
                    session.execute_write(self.create_entity_nodes_batch, 'CampaignMonthlyMetric', monthly_metric_nodes)

                    rel_query = """
                    UNWIND $relationships as rel
                    MATCH (start:Campaign {campaign_id: rel.start_value})
                    MATCH (end:CampaignMonthlyMetric {campaign_id: rel.end_value, month_start_date: rel.month_value})
                    MERGE (start)-[r:HAS_MONTHLY_METRICS]->(end)
                    """
                    logger.debug(f"Creating batch of {len(relationships)} HAS_MONTHLY_METRICS relationships for Campaign.")
                    session.run(rel_query, {'relationships': relationships})
                    
            logger.info("Completed CampaignMonthlyMetric transformation.")

    def transform_keyword(self, keyword_df: pd.DataFrame, customer_id: str):
        """Transform keywords into graph format using batch processing"""
        with self.driver.session() as session:
            # Group keywords by ad_group_id
            keyword_groups = keyword_df.groupby('ad_group_id')
            
            for ad_group_id, group in keyword_groups:
                # Create a single KeywordGroup node for each ad group
                keyword_group = {
                    'ad_group_id': ad_group_id,
                    'keyword_count': len(group),
                    'keywords': group['ad_group_criterion_keyword_text'].tolist(),
                    'match_types': group['ad_group_criterion_keyword_match_type'].tolist(),
                    'statuses': group['ad_group_criterion_status'].tolist(),
                    'bid_modifiers': [float(row.get('ad_group_criterion_bid_modifier', 0)) for _, row in group.iterrows()],
                    'quality_scores': [int(row.get('ad_group_criterion_quality_info_quality_score', 0)) for _, row in group.iterrows()],
                    'criterion_ids': group['ad_group_criterion_criterion_id'].tolist()
                }
                
                # Create the KeywordGroup node
                session.execute_write(self.create_entity_nodes_batch, 'KeywordGroup', [keyword_group])
                
                # Create relationship to AdGroup
                relationships = [{
                    'start_key': 'ad_group_id',
                    'start_value': ad_group_id,
                    'end_key': 'ad_group_id',
                    'end_value': ad_group_id
                }]
                
                # Create relationship in batch
                session.execute_write(
                    self.create_relationships_batch,
                    'AdGroup',
                    'KeywordGroup',
                    'HAS_KEYWORDS',
                    relationships
                )

    def transform_asset(self, asset_df: pd.DataFrame):
        """Transform asset data into graph format using batch processing"""
        with self.driver.session() as session:
            for start_idx in range(0, len(asset_df), self.BATCH_SIZE):
                batch = asset_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                # Create asset nodes in batch
                nodes = [{
                    'asset_id': row['asset_id'],
                    'asset_type': row['asset_type'],
                    'name': row['asset_name'],
                    'file_hash': row.get('asset_file_hash', '')
                } for _, row in batch.iterrows()]
                
                session.execute_write(self.create_entity_nodes_batch, 'Asset', nodes)

    def transform_conversion_action(self, conversion_df: pd.DataFrame):
        """Transform conversion action data into graph format using batch processing"""
        with self.driver.session() as session:
            # Group conversion actions by account_id
            account_groups = conversion_df.groupby('customer_id')
            
            for account_id, group in account_groups:
                # Create a single ConversionAction node for each account
                conversion_actions = []
                for _, row in group.iterrows():
                    conversion_action = {
                        'conversion_action_id': row['conversion_action_id'],
                        'name': row['conversion_action_name'],
                        'category': row.get('conversion_action_category', ''),
                        'type': row.get('conversion_action_type', ''),
                        'value_per_conversion': row.get('conversion_action_value_per_conversion', 0),
                        'counting_type': row.get('conversion_action_counting_type', '')
                    }
                    conversion_actions.append(conversion_action)
                
                # Serialize the conversion_actions list to a JSON string
                conversion_actions_json = json.dumps(conversion_actions)
                
                # Create a single node with all conversion actions for this account
                node = {
                    'account_id': account_id,
                    'conversion_actions_json': conversion_actions_json,
                    'conversion_count': len(conversion_actions)
                }
                
                # Create the ConversionAction node
                session.execute_write(self.create_entity_nodes_batch, 'ConversionAction', [node])
                
                # Create relationship to AdAccount
                relationships = [{
                    'start_key': 'account_id',
                    'start_value': account_id,
                    'end_key': 'account_id',
                    'end_value': account_id
                }]
                
                # Create relationship in batch
                session.execute_write(
                    self.create_relationships_batch,
                    'AdAccount',
                    'ConversionAction',
                    'HAS_CONVERSION_ACTIONS',
                    relationships
                )

    def transform_audience(self, audience_df: pd.DataFrame):
        """Transforms audience data, parsing dimensions into separate component nodes."""
        logger.info(f"Starting Audience transformation for {len(audience_df)} records.")
        
        # Ensure required columns are present
        required_cols = ['audience_id', 'audience_resource_name', 'audience_name', 'audience_status', 'audience_dimensions', 'customer_id']
        if not all(col in audience_df.columns for col in required_cols):
            logger.warning(f"Skipping Audience transformation: Missing required columns (need {required_cols}).")
            return

        # Clean the dataframe: drop duplicates and rows with missing essential IDs
        audience_df = audience_df.drop_duplicates(subset=['audience_id'])
        audience_df = audience_df.dropna(subset=['audience_id', 'audience_resource_name', 'customer_id'])
        
        if audience_df.empty:
            logger.info("No valid audience records found after cleaning.")
            return
            
        with self.driver.session() as session:
            # Prepare batches for different node types and relationships
            audience_nodes_batch = []
            adaccount_relationships_batch = []
            age_range_nodes_batch = []
            age_range_rels_batch = []
            gender_nodes_batch = []
            gender_rels_batch = []
            user_interest_nodes_batch = []
            user_interest_rels_batch = []
            custom_audience_nodes_batch = []
            custom_audience_rels_batch = []
            # Add other component batches if needed (e.g., parental status, location)

            for _, row in audience_df.iterrows():
                audience_id = row['audience_id']
                customer_id = row['customer_id']
                audience_resource_name = row['audience_resource_name']

                # 1. Create main Audience node data
                audience_node = {
                    'audience_id': audience_id, # FIX 1: Use lowercase 'audience_id' to match id_property mapping
                    'resourceName': audience_resource_name,
                    'name': row.get('audience_name', ''),
                    'description': row.get('audience_description', ''),
                    'status': row.get('audience_status', '')
                    # Keep it simple, dimensions handled separately
                }
                audience_nodes_batch.append(audience_node)

                # 2. Create relationship to AdAccount
                adaccount_relationships_batch.append({
                    'start_key': 'account_id',
                    'start_value': customer_id,
                    'end_key': 'audience_id', # Match Audience node property
                    'end_value': audience_id
                })

                # 3. Parse dimensions and create component nodes/relationships
                dimensions_value = row.get('audience_dimensions')
                dimensions_to_parse = []

                # FIX 2: Handle list of strings or single string
                if isinstance(dimensions_value, list):
                    dimensions_to_parse = dimensions_value
                elif isinstance(dimensions_value, str) and dimensions_value.strip():
                    # Attempt to parse if it's a single JSON string representing a list
                    try:
                        parsed_list = json.loads(dimensions_value)
                        if isinstance(parsed_list, list):
                            dimensions_to_parse = parsed_list
                        else:
                             logger.warning(f"Audience {audience_id}: Parsed 'audience_dimensions' string is not a list. Skipping. Data: {dimensions_value}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Audience {audience_id}: Failed to decode 'audience_dimensions' as a single JSON list string: {e}. Data: {dimensions_value}")
                elif dimensions_value: # It exists but isn't a string or list
                     logger.warning(f"Audience {audience_id}: 'audience_dimensions' field is not a list or string. Skipping. Type: {type(dimensions_value)}, Value: {dimensions_value}")

                # Now iterate through the prepared list (which might contain strings or already parsed dicts)
                for item in dimensions_to_parse:
                    dim_data = None
                    if isinstance(item, str): # If item is a string, parse it as JSON
                        try:
                            dim_data = json.loads(item)
                        except json.JSONDecodeError as e:
                           logger.error(f"Audience {audience_id}: Failed to decode dimension item JSON string: {e}. Item: {item}")
                           continue # Skip this dimension item
                    elif isinstance(item, dict): # If item is already a dictionary
                        dim_data = item
                    else:
                        logger.warning(f"Audience {audience_id}: Unexpected item type in dimensions list: {type(item)}. Item: {item}")
                        continue # Skip this dimension item

                    if not isinstance(dim_data, dict):
                         logger.warning(f"Audience {audience_id}: Parsed dimension item is not a dictionary. Item: {dim_data}")
                         continue # Skip this dimension item

                    # --- Process the parsed dimension data (dim_data) ---
                    # Age Range - Adjusting key access based on example
                    if 'age' in dim_data and 'ageRanges' in dim_data['age'] and isinstance(dim_data['age']['ageRanges'], list) and len(dim_data['age']['ageRanges']) > 0:
                        # Assuming we take the first age range specified
                        age_info = dim_data['age']['ageRanges'][0]
                        if isinstance(age_info, dict):
                             min_age = age_info.get('minAge')
                             max_age = age_info.get('maxAge')
                             if min_age is not None and max_age is not None:
                                 try:
                                     age_node = {'minAge': int(min_age), 'maxAge': int(max_age)}
                                     if not any(n == age_node for n in age_range_nodes_batch):
                                         age_range_nodes_batch.append(age_node)
                                     age_range_rels_batch.append({
                                         'audience_id': audience_id,
                                         'min_age': int(min_age),
                                         'max_age': int(max_age)
                                     })
                                 except ValueError:
                                     logger.warning(f"Audience {audience_id}: Could not convert age range to int. min: {min_age}, max: {max_age}")
                             else:
                                 logger.debug(f"Audience {audience_id}: Skipping age range due to missing min/max age in age_info dict. Data: {age_info}")
                        else:
                            logger.debug(f"Audience {audience_id}: First item in ageRanges is not a dictionary. Data: {age_info}")
                    # Use elif to avoid processing the same dim_data block multiple times
                    # Gender - Adjusting key access based on example
                    elif 'gender' in dim_data and 'genders' in dim_data['gender'] and isinstance(dim_data['gender']['genders'], list) and len(dim_data['gender']['genders']) > 0:
                        # Assuming we take the first gender specified
                        gender_type = dim_data['gender']['genders'][0]
                        if isinstance(gender_type, str) and gender_type:
                             gender_node = {'genderType': gender_type}
                             if not any(n['genderType'] == gender_type for n in gender_nodes_batch):
                                 gender_nodes_batch.append(gender_node)
                             gender_rels_batch.append({
                                 'audience_id': audience_id,
                                 'gender_type': gender_type
                             })
                        else:
                            logger.debug(f"Audience {audience_id}: Skipping gender due to missing or non-string type in genders list. Data: {gender_type}")

                    # Audience Segments - Logic seems okay, structure matched example
                    elif 'audienceSegments' in dim_data:
                         # Check if the value is a dictionary and contains the 'segments' key
                         if isinstance(dim_data['audienceSegments'], dict) and 'segments' in dim_data['audienceSegments']:
                             segments = dim_data['audienceSegments']['segments'] # Access the nested list
                             if not isinstance(segments, list):
                                 logger.warning(f"Audience {audience_id}: Value under 'audienceSegments.segments' is not a list. Skipping segments. Data: {segments}")
                                 continue # Skip to next item in dimensions_to_parse
                         elif isinstance(dim_data['audienceSegments'], list): 
                             # Handle case where it might sometimes be a direct list 
                             segments = dim_data['audienceSegments']
                             logger.debug(f"Audience {audience_id}: 'audienceSegments' was directly a list.")
                         else:
                             logger.warning(f"Audience {audience_id}: 'audienceSegments' value is not a dictionary with a 'segments' key or a direct list. Skipping segments. Data: {dim_data['audienceSegments']}")
                             continue # Skip to next item in dimensions_to_parse
                         
                         # Process segments list 
                         for segment in segments:
                            if not isinstance(segment, dict):
                                logger.warning(f"Audience {audience_id}: Item in segments list is not a dictionary. Segment: {segment}")
                                continue # Skip this segment
                                 
                            # User Interest within Segments
                            if 'userInterest' in segment:
                                interest_info = segment['userInterest']
                                if isinstance(interest_info, dict):
                                     interest_id_str = interest_info.get('userInterestCategory')
                                     if interest_id_str and isinstance(interest_id_str, str) and interest_id_str.startswith('userInterests/'):
                                         try:
                                             criterion_id = int(interest_id_str.split('/')[-1])
                                             interest_node = {'criterionId': criterion_id}
                                             if not any(n['criterionId'] == criterion_id for n in user_interest_nodes_batch):
                                                 user_interest_nodes_batch.append(interest_node)
                                             user_interest_rels_batch.append({
                                                 'audience_id': audience_id,
                                                 'criterion_id': criterion_id
                                             })
                                         except ValueError:
                                             logger.warning(f"Audience {audience_id}: Could not parse criterionId from segment userInterestCategory: {interest_id_str}")
                                     else:
                                         logger.debug(f"Audience {audience_id}: Skipping segment user interest due to missing/invalid category ID string. Data: {interest_id_str}")
                                else:
                                    logger.debug(f"Audience {audience_id}: userInterest value in segment is not a dictionary. Data: {interest_info}")
                            
                            # Custom Audience within Segments
                            elif 'customAudience' in segment:
                                custom_info = segment['customAudience']
                                if isinstance(custom_info, dict):
                                    custom_id_str = custom_info.get('customAudience')
                                    if custom_id_str and isinstance(custom_id_str, str) and custom_id_str.startswith('customAudiences/'):
                                        try:
                                            custom_audience_id = int(custom_id_str.split('/')[-1])
                                            custom_node = {'customAudienceId': custom_audience_id}
                                            if not any(n['customAudienceId'] == custom_audience_id for n in custom_audience_nodes_batch):
                                                custom_audience_nodes_batch.append(custom_node)
                                            custom_audience_rels_batch.append({
                                                'audience_id': audience_id,
                                                'custom_audience_id': custom_audience_id
                                            })
                                        except ValueError:
                                            logger.warning(f"Audience {audience_id}: Could not parse customAudienceId from segment customAudience: {custom_id_str}")
                                    else:
                                        logger.debug(f"Audience {audience_id}: Skipping segment custom audience due to missing/invalid ID string. Data: {custom_id_str}")
                                else:
                                     logger.debug(f"Audience {audience_id}: customAudience value in segment is not a dictionary. Data: {custom_info}")
                            
                            # Add other segment types (userList, etc.) here if needed
                            else:
                                logger.debug(f"Audience {audience_id}: Unhandled segment type within audienceSegments: {list(segment.keys())}")

                    # NOTE: Removing the top-level userInterest/customAudience handling as the example 
                    # suggests they are nested within audienceSegments. If top-level ones can also exist,
                    # uncomment and potentially adapt the logic below.
                    # elif 'userInterest' in dim_data: ...
                    # elif 'customAudience' in dim_data: ...

                    # Add elif blocks here for other top-level dimension types (ParentalStatus, LifeEvent, etc.) if needed
                    
                    else:
                        # This will catch cases where the dimension is neither age, gender, nor audienceSegments
                        logger.debug(f"Audience {audience_id}: Unhandled top-level dimension type: {list(dim_data.keys())}")

            # --- Batch Write Everything ---
            # Function to execute batch writes, simplifying the loop
            def execute_batch_write(entity_type, nodes):
                if nodes:
                    logger.debug(f"Creating batch of {len(nodes)} {entity_type} nodes.")
                    # Use a copy to avoid modifying the list during iteration if needed elsewhere
                    session.execute_write(self.create_entity_nodes_batch, entity_type, list(nodes)) 
            
            def execute_rel_batch_write(start_label, end_label, rel_type, rels, start_key, end_key, end_key_map):
                 if rels:
                     query = f"""
                     UNWIND $relationships as rel
                     MATCH (start:{start_label} {{{start_key}: rel.{start_key}}})
                     MATCH (end:{end_label} {{{end_key_map['id_prop']}: rel.{end_key}}})
                     MERGE (start)-[r:{rel_type}]->(end)
                     """
                     logger.debug(f"Creating batch of {len(rels)} {rel_type} relationships from {start_label} to {end_label}.")
                     session.run(query, {'relationships': rels})

            # --- Write Nodes ---
            execute_batch_write('Audience', audience_nodes_batch)
            execute_batch_write('AgeRange', age_range_nodes_batch)
            execute_batch_write('Gender', gender_nodes_batch)
            execute_batch_write('UserInterest', user_interest_nodes_batch)
            execute_batch_write('CustomAudience', custom_audience_nodes_batch)

            # --- Write Relationships ---
            # AdAccount -> Audience
            if adaccount_relationships_batch:
                session.execute_write(
                    self.create_relationships_batch,
                    'AdAccount', 'Audience', 'DEFINED_AUDIENCE', adaccount_relationships_batch
                )

            # Audience -> AgeRange
            if age_range_rels_batch:
                query_age = """
                UNWIND $relationships as rel
                MATCH (start:Audience {audience_id: rel.audience_id})
                MATCH (end:AgeRange {minAge: rel.min_age, maxAge: rel.max_age})
                MERGE (start)-[r:HAS_AGE_RANGE]->(end)
                """
                logger.debug(f"Creating batch of {len(age_range_rels_batch)} HAS_AGE_RANGE relationships.")
                session.run(query_age, {'relationships': age_range_rels_batch})

            # Audience -> Gender
            if gender_rels_batch:
                query_gender = """
                UNWIND $relationships as rel
                MATCH (start:Audience {audience_id: rel.audience_id})
                MATCH (end:Gender {genderType: rel.gender_type})
                MERGE (start)-[r:HAS_GENDER]->(end)
                """
                logger.debug(f"Creating batch of {len(gender_rels_batch)} HAS_GENDER relationships.")
                session.run(query_gender, {'relationships': gender_rels_batch})

            # Audience -> UserInterest
            if user_interest_rels_batch:
                query_interest = """
                UNWIND $relationships as rel
                MATCH (start:Audience {audience_id: rel.audience_id})
                MATCH (end:UserInterest {criterionId: rel.criterion_id})
                MERGE (start)-[r:INCLUDES_SEGMENT]->(end)
                """
                logger.debug(f"Creating batch of {len(user_interest_rels_batch)} INCLUDES_SEGMENT relationships to UserInterest.")
                session.run(query_interest, {'relationships': user_interest_rels_batch})

            # Audience -> CustomAudience
            if custom_audience_rels_batch:
                 query_custom = """
                 UNWIND $relationships as rel
                 MATCH (start:Audience {audience_id: rel.audience_id})
                 MATCH (end:CustomAudience {customAudienceId: rel.custom_audience_id})
                 MERGE (start)-[r:INCLUDES_SEGMENT]->(end)
                 """
                 logger.debug(f"Creating batch of {len(custom_audience_rels_batch)} INCLUDES_SEGMENT relationships to CustomAudience.")
                 session.run(query_custom, {'relationships': custom_audience_rels_batch})

        logger.info("Completed Audience transformation (with component nodes).")

    def transform_label(self, label_df: pd.DataFrame, customer_label_df: pd.DataFrame = None):
        """Transform label data into graph format using batch processing"""
        with self.driver.session() as session:
            # Remove duplicates based on label_id
            label_df = label_df.drop_duplicates(subset=['label_id'])
            
            for start_idx in range(0, len(label_df), self.BATCH_SIZE):
                batch = label_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                # Create label nodes in batch
                nodes = [{
                    'label_id': row['label_id'],
                    'name': row['label_name'],
                    'status': row.get('label_status', ''),
                    'resource_name': row.get('label_resource_name', ''),
                    'description': row.get('label_text_label_description', ''),
                    'background_color': row.get('label_text_label_background_color', '')
                } for _, row in batch.iterrows()]
                
                session.execute_write(self.create_entity_nodes_batch, 'Label', nodes)
            
            # Create relationships to AdAccount if customer_label data is available
            if customer_label_df is not None and not customer_label_df.empty:
                # Remove duplicates
                customer_label_df = customer_label_df.drop_duplicates(subset=['customer_id', 'customer_label_label'])
                
                for start_idx in range(0, len(customer_label_df), self.BATCH_SIZE):
                    batch = customer_label_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                    
                    # Create relationships to AdAccount
                    relationships = [{
                        'start_key': 'account_id',
                        'start_value': row['customer_id'],
                        'end_key': 'label_id',
                        'end_value': row['customer_label_label']
                    } for _, row in batch.iterrows()]
                    
                    session.execute_write(
                        self.create_relationships_batch,
                        'AdAccount',
                        'Label',
                        'HAS_LABEL',
                        relationships
                    )

    def transform_campaign_criterion(self, criterion_df: pd.DataFrame):
        """Transform campaign criterion data using batch processing with specific node types."""
        with self.driver.session() as session:
            # Prepare lists for batching GeoLocation nodes and relationships
            geo_location_nodes_batch = []
            location_relationships_batch = []

            for start_idx in range(0, len(criterion_df), self.BATCH_SIZE):
                batch = criterion_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                # Process each criterion type separately within the batch
                for _, row in batch.iterrows():
                    criterion_type = row.get('campaign_criterion_type')
                    campaign_id = row.get('campaign_id')
                    criterion_id = row.get('campaign_criterion_criterion_id') # Get criterion ID
                    resource_name = row.get('campaign_criterion_resource_name')
                    is_negative = row.get('campaign_criterion_negative', False)
                    display_name = row.get('campaign_criterion_display_name') # Primary name source
                    status = row.get('campaign_criterion_status')

                    if not criterion_type or not campaign_id or not criterion_id:
                        logger.debug(f"Skipping criterion row due to missing type, campaign_id, or criterion_id: {row.to_dict()}")
                        continue
                    
                    node = {}
                    relationship_type = ""
                    node_type = ""
                    end_key = ''
                    end_value = ''
                    
                    # --- Handle GeoLocation Criteria --- 
                    if criterion_type == 'LOCATION':
                        node_type = 'GeoLocation'
                        node = {
                            'criterionId': criterion_id,
                            # Attempt to get name from display_name first, fallback needed if not present
                            'name': display_name if pd.notna(display_name) else f"Location_{criterion_id}", # Fallback name
                            # 'locationType': # Requires enrichment 
                            # 'canonicalName': # Requires enrichment
                            # 'countryCode': # Requires enrichment
                            # 'status': # Could potentially use criterion status if GeoTargetConstant status isn't available
                        }
                        relationship_type = 'EXCLUDES_LOCATION' if is_negative else 'TARGETS_LOCATION'
                        end_key = 'criterionId'
                        end_value = criterion_id

                        # Add node to batch (ensure uniqueness within the batch processing)
                        # Simple check based on criterionId to avoid duplicates in this batch
                        if not any(n['criterionId'] == criterion_id for n in geo_location_nodes_batch):
                           geo_location_nodes_batch.append(node)
                           
                        # Add relationship to batch
                        location_relationships_batch.append({
                            'start_key': 'campaign_id',
                            'start_value': campaign_id,
                            'end_key': end_key,
                            'end_value': end_value,
                            'rel_type': relationship_type # Store rel type here
                        })
                        
                    # --- Handle Other Criterion Types (Simplified example, expand as needed) ---
                    elif criterion_type == 'LANGUAGE':
                        # Add logic for Language nodes if needed
                        logger.debug(f"Skipping non-LOCATION criterion type: {criterion_type}")
                        pass 
                    elif criterion_type == 'KEYWORD' and is_negative:
                        # Handle Negative Campaign Keywords if necessary (logic moved from older version)
                        logger.debug(f"Skipping non-LOCATION criterion type: {criterion_type}")
                        pass
                    # Add elif blocks for other specific types you want to handle (AD_SCHEDULE, NETWORK, etc.)
                    else:
                        # Log skipped types for review
                        logger.debug(f"Skipping unhandled criterion type: {criterion_type} for campaign {campaign_id}")
                        continue # Skip to next row if type is not handled

            # --- Batch Write GeoLocation Nodes and Relationships --- 
            if geo_location_nodes_batch:
                logger.debug(f"Creating batch of {len(geo_location_nodes_batch)} GeoLocation nodes.")
                session.execute_write(self.create_entity_nodes_batch, 'GeoLocation', geo_location_nodes_batch)
            
            if location_relationships_batch:
                # Need to group relationships by type for batching
                rels_by_type = {}
                for rel_data in location_relationships_batch:
                    rel_type = rel_data.pop('rel_type') # Get and remove rel_type from dict
                    if rel_type not in rels_by_type:
                        rels_by_type[rel_type] = []
                    rels_by_type[rel_type].append(rel_data)
                
                for rel_type, relationships in rels_by_type.items():
                    logger.debug(f"Creating batch of {len(relationships)} {rel_type} relationships to GeoLocation.")
                    session.execute_write(
                        self.create_relationships_batch,
                        'Campaign',
                        'GeoLocation',
                        rel_type, # Use the specific relationship type (TARGETS or EXCLUDES)
                        relationships
                    )
        logger.info("Completed CampaignCriterion transformation (including GeoLocation).")

    def transform_campaign_budget(self, budget_df: pd.DataFrame):
        """Transform campaign budget data using batch processing"""
        with self.driver.session() as session:
            for start_idx in range(0, len(budget_df), self.BATCH_SIZE):
                batch = budget_df.iloc[start_idx:start_idx + self.BATCH_SIZE]
                
                # Create campaign budget nodes in batch
                nodes = [{
                    'budget_id': row['campaign_budget_id'],
                    'resource_name': row['campaign_budget_resource_name'],
                    'name': row['campaign_budget_name'],
                    'amount': float(row['campaign_budget_amount_micros']) / 1000000,  # Convert micros to actual amount
                    'delivery_method': row['campaign_budget_delivery_method'],
                    'status': row['campaign_budget_status'],
                    'type': row['campaign_budget_type'],
                    'has_shared_set': row['campaign_budget_has_recommended_budget'],
                    'explicitly_shared': row['campaign_budget_explicitly_shared']
                } for _, row in batch.iterrows()]
                
                session.execute_write(self.create_entity_nodes_batch, 'CampaignBudget', nodes)
                
                # Create relationships to Campaign
                relationships = [{
                    'start_key': 'campaign_id',
                    'start_value': row['campaign_id'],
                    'end_key': 'budget_id',
                    'end_value': row['campaign_budget_id']
                } for _, row in batch.iterrows()]
                
                session.execute_write(
                    self.create_relationships_batch,
                    'Campaign',
                    'CampaignBudget',
                    'USES_BUDGET',
                    relationships
                )

    def run_pipeline(self, sql_data: Dict[str, pd.DataFrame]):
        """Run the complete transformation pipeline in a dependency-aware order."""
        try:
            # == Step 1: Setup ==
            logger.info("Pipeline Step 1: Creating Neo4j constraints")
            self.create_constraints()
            logger.info("Pipeline Step 2: Creating Neo4j indexes")
            self.create_indexes()

            # == Step 2: Determine Primary Account ID ==
            customer_id = None
            if 'customer' in sql_data and not sql_data['customer'].empty:
                customer_id = sql_data['customer']['customer_id'].iloc[0]
                logger.info(f"Pipeline Step 3: Determined customer_id: {customer_id}")
            else:
                logger.warning("Could not determine primary customer_id from the 'customer' table. Some steps might be skipped.")
                # Optionally, decide if the pipeline should halt here
                # return

            # == Step 3: Core Entities (Top-Down) ==

            # AdAccount (Requires 'customer' table)
            if customer_id:
                logger.info("Pipeline Step 4: Starting AdAccount transformation")
                self.transform_adaccount(sql_data['customer'])
            else:
                 logger.warning("Skipping AdAccount transformation as customer_id is unknown.")

            # Audience (Requires 'audience' table and links to AdAccount)
            if 'audience' in sql_data:
                logger.info("Pipeline Step 5: Starting Audience transformation")
                if customer_id: # Check if AdAccount exists
                     self.transform_audience(sql_data['audience'])
                else:
                     logger.warning("Skipping Audience transformation as customer_id is unknown.")
            else:
                logger.warning("Skipping Audience transformation - 'audience' data missing.")

            # Label (Requires 'label', optionally 'customer_label' tables and links to AdAccount)
            if 'label' in sql_data:
                logger.info("Pipeline Step 6: Starting Label transformation")
                if customer_id: # Check if AdAccount exists
                    self.transform_label(sql_data['label'], sql_data.get('customer_label'))
                else:
                     logger.warning("Skipping Label transformation as customer_id is unknown.")
            else:
                logger.warning("Skipping Label transformation - 'label' data missing.")

            # Campaign (Requires 'campaign' table and links to AdAccount)
            if 'campaign' in sql_data:
                logger.info("Pipeline Step 7: Starting Campaign transformation")
                if customer_id:
                    self.transform_campaign(sql_data['campaign'], customer_id)
                else:
                    logger.warning("Skipping Campaign transformation as customer_id is unknown.")
            else:
                 logger.warning("Skipping Campaign transformation - 'campaign' data missing.")

            # CampaignBudget (Requires 'campaign_budget' table and links to Campaign)
            if 'campaign_budget' in sql_data:
                logger.info("Pipeline Step 8: Starting CampaignBudget transformation")
                if 'campaign' in sql_data: # Check if Campaign data exists
                    self.transform_campaign_budget(sql_data['campaign_budget'])
                else:
                    logger.warning("Skipping CampaignBudget transformation - 'campaign' data missing.")
            else:
                logger.warning("Skipping CampaignBudget transformation - 'campaign_budget' data missing.")

            # CampaignCriterion (Requires 'campaign_criterion' table and links to Campaign)
            if 'campaign_criterion' in sql_data:
                logger.info("Pipeline Step 9: Starting CampaignCriterion transformation")
                if 'campaign' in sql_data: # Check if Campaign data exists
                     self.transform_campaign_criterion(sql_data['campaign_criterion'])
                else:
                     logger.warning("Skipping CampaignCriterion transformation - 'campaign' data missing.")
            else:
                logger.warning("Skipping CampaignCriterion transformation - 'campaign_criterion' data missing.")

            # AdGroup (Requires 'ad_group' table and links to Campaign)
            if 'ad_group' in sql_data:
                logger.info("Pipeline Step 10: Starting AdGroup transformation")
                if 'campaign' in sql_data: # Check if Campaign data exists
                    self.transform_adgroup(sql_data['ad_group'])
                else:
                    logger.warning("Skipping AdGroup transformation - 'campaign' data missing.")
            else:
                logger.warning("Skipping AdGroup transformation - 'ad_group' data missing.") # Adjusted indentation
            
            # Ad (Requires 'ad_group_ad' table and links to AdGroup)
            if 'ad_group_ad' in sql_data:
                logger.info("Pipeline Step 11: Starting Ad transformation")
                if 'ad_group' in sql_data: # Check if AdGroup data exists
                    # Note: transform_ad currently takes customer_id, review if needed
                    if customer_id:
                        self.transform_ad(sql_data['ad_group_ad'], customer_id)
                    else:
                         logger.warning("Skipping Ad transformation as customer_id is unknown (required by current implementation).")
                else:
                    logger.warning("Skipping Ad transformation - 'ad_group' data missing.") # Adjusted indentation
            else:
                 logger.warning("Skipping Ad transformation - 'ad_group_ad' data missing.")

            # KeywordGroup (Requires 'ad_group_criterion', filtered for keywords, links to AdGroup)
            if 'ad_group_criterion' in sql_data:
                keyword_df = sql_data['ad_group_criterion'][
                    sql_data['ad_group_criterion']['ad_group_criterion_type'] == 'KEYWORD'
                ].copy() # Use copy to avoid SettingWithCopyWarning
                if not keyword_df.empty:
                    logger.info(f"Pipeline Step 12: Starting KeywordGroup transformation for {len(keyword_df)} keywords")
                    if 'ad_group' in sql_data: # Check if AdGroup data exists
                        # Note: transform_keyword currently takes customer_id, review if needed
                        if customer_id:
                            self.transform_keyword(keyword_df, customer_id)
                        else:
                            logger.warning("Skipping KeywordGroup transformation as customer_id is unknown (required by current implementation).")
                    else:
                        logger.warning("Skipping KeywordGroup transformation - 'ad_group' data missing.")
                else:
                    logger.info("No keyword criteria found for KeywordGroup transformation.")
            else:
                logger.warning("Skipping KeywordGroup transformation - 'ad_group_criterion' data missing.")

            # Asset (Derived from 'ad_group_ad')
            if 'asset' in sql_data: # Check if derived data exists
                logger.info("Pipeline Step 13: Starting Asset transformation")
                self.transform_asset(sql_data['asset'])
            else:
                logger.warning("Skipping Asset transformation - derived 'asset' data missing (check main() logic).")

            # ConversionAction (Derived from 'campaign_budget' in main, links to AdAccount)
            if 'conversion_action' in sql_data: # Check if derived data exists
                logger.info("Pipeline Step 14: Starting ConversionAction transformation")
                if customer_id: # Check if AdAccount exists
                    self.transform_conversion_action(sql_data['conversion_action'])
                else:
                     logger.warning("Skipping ConversionAction transformation as customer_id is unknown.")
            else:
                logger.warning("Skipping ConversionAction transformation - derived 'conversion_action' data missing (check main() logic).")

            # Product (Requires 'shopping_performance_view' table and links to Campaign)
            if 'shopping_performance_view' in sql_data:
                 logger.info("Pipeline Step 14.5: Starting Product transformation")
                 if 'campaign' in sql_data: # Check if Campaign data exists
                     self.transform_product(sql_data['shopping_performance_view'])
                 else:
                     logger.warning("Skipping Product transformation - 'campaign' data missing.")
            else:
                 logger.warning("Skipping Product transformation - 'shopping_performance_view' data missing.")


            # == Step 4: Metric Nodes (linking to existing core entities) ==

            # AccountMonthlyMetric (Requires 'account_performance_report' table and links to AdAccount)
            if 'account_performance_report' in sql_data:
                 logger.info("Pipeline Step 15: Starting AccountMonthlyMetric transformation")
                 if customer_id: # Check if AdAccount exists
                     self.transform_account_monthly_metrics(sql_data['account_performance_report'])
                 else:
                     logger.warning("Skipping AccountMonthlyMetric transformation as customer_id is unknown.")
            else:
                 logger.warning("Skipping AccountMonthlyMetric transformation - 'account_performance_report' data missing.")

            # AccountOverallMetric (Requires 'account_performance_report' table and links to AdAccount)
            if 'account_performance_report' in sql_data:
                logger.info("Pipeline Step 15.1: Starting AccountOverallMetric transformation")
                if customer_id: # Check if AdAccount exists
                    self.transform_account_overall_metrics(sql_data['account_performance_report'])
                else:
                    logger.warning("Skipping AccountOverallMetric transformation as customer_id is unknown.")
            else:
                logger.warning("Skipping AccountOverallMetric transformation - 'account_performance_report' data missing.")

            # CampaignWeeklyMetric (Requires 'campaign' table and links to Campaign)
            # ---> PROBLEM: 'campaign' table lacks daily metrics for aggregation. Use a real performance table if available.
            if 'campaign' in sql_data:
                 logger.info("Pipeline Step 16: Starting CampaignWeeklyMetric transformation (using 'campaign' table - may lack metrics)")
                 # Check if Campaign node creation was attempted
                 if 'campaign' in sql_data:
                     self.transform_campaign_weekly_metrics(sql_data['campaign']) # This uses the 'campaign' table
                 else:
                     logger.warning("Skipping CampaignWeeklyMetric transformation - 'campaign' data missing.")
            else:
                 logger.warning("Skipping CampaignWeeklyMetric transformation - 'campaign' data missing.") # Redundant check but safe

            # CampaignOverallMetric (Requires 'campaign' table)
            if 'campaign' in sql_data:
                logger.info("Pipeline Step 16.1: Starting CampaignOverallMetric transformation")
                self.transform_campaign_overall_metrics(sql_data['campaign'])
            else:
                logger.warning("Skipping CampaignOverallMetric transformation - 'campaign' data missing.")

            # CampaignMonthlyMetric (Requires 'campaign' table)
            if 'campaign' in sql_data:
                logger.info("Pipeline Step 16.2: Starting CampaignMonthlyMetric transformation")
                self.transform_campaign_monthly_metrics(sql_data['campaign'])
            else:
                logger.warning("Skipping CampaignMonthlyMetric transformation - 'campaign' data missing.")

            # AdDailyMetric (Requires 'ad_group_ad_legacy' table and links to Ad)
            # Now using ad_group_ad_legacy table
            if 'ad_group_ad_legacy' in sql_data:
                logger.info("Pipeline Step 17: Starting AdDailyMetric transformation (using 'ad_group_ad_legacy' table)") # Renamed Step
                 # Check if Ad node creation was attempted
                if 'ad_group_ad' in sql_data: # Still need Ad nodes to exist
                    self.transform_ad_daily_metrics(sql_data['ad_group_ad_legacy']) # Renamed Function Called
                else:
                     logger.warning("Skipping AdDailyMetric transformation - Ad nodes missing (needs 'ad_group_ad' table data).")
            else:
                 logger.warning("Skipping AdDailyMetric transformation - 'ad_group_ad_legacy' data missing.")

            # AdOverallMetric (Requires 'ad_group_ad_legacy' table and links to Ad)
            if 'ad_group_ad_legacy' in sql_data:
                logger.info("Pipeline Step 18: Starting AdOverallMetric transformation (using 'ad_group_ad_legacy' table)")
                # Check if Ad nodes exist (needed for relationship)
                if 'ad_group_ad' in sql_data:
                    self.transform_ad_overall_metrics(sql_data['ad_group_ad_legacy'])
                else:
                    logger.warning("Skipping AdOverallMetric transformation - Ad nodes missing (needs 'ad_group_ad' table data).")
            else:
                logger.warning("Skipping AdOverallMetric transformation - 'ad_group_ad_legacy' data missing.")

            # AdMonthlyMetric (Requires 'ad_group_ad_legacy' table and links to Ad)
            if 'ad_group_ad_legacy' in sql_data:
                logger.info("Pipeline Step 19: Starting AdMonthlyMetric transformation (using 'ad_group_ad_legacy' table)")
                # Check if Ad nodes exist (needed for relationship)
                if 'ad_group_ad' in sql_data:
                    self.transform_ad_monthly_metrics(sql_data['ad_group_ad_legacy'])
                else:
                    logger.warning("Skipping AdMonthlyMetric transformation - Ad nodes missing (needs 'ad_group_ad' table data).")
            else:
                logger.warning("Skipping AdMonthlyMetric transformation - 'ad_group_ad_legacy' data missing.")


            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Error in pipeline execution: {str(e)}")
            # Log the traceback for more details
            import traceback
            logger.error(traceback.format_exc())
            raise # Re-raise the exception after logging
        finally:
            # Ensure close is called even if errors occur mid-pipeline
            logger.info("Closing Neo4j connection in finally block.")
            self.close()

    def create_indexes(self):
        """Create indexes for Neo4j"""
        indexes = [
            # Campaign indexes
            "CREATE INDEX IF NOT EXISTS FOR (c:Campaign) ON (c.campaign_id)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Campaign) ON (c.name)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Campaign) ON (c.status)",
            
            # CampaignCriterion indexes (Removed generic, specific nodes handle their own indexes if needed)
            # "CREATE INDEX IF NOT EXISTS FOR (cc:CampaignCriterion) ON (cc.resource_name)",
            # "CREATE INDEX IF NOT EXISTS FOR (cc:CampaignCriterion) ON (cc.criterion_id)",
            # "CREATE INDEX IF NOT EXISTS FOR (cc:CampaignCriterion) ON (cc.type)",
            
            # Specific CampaignCriterion type indexes (Add if needed, e.g., TargetedLocation by location_id)
            "CREATE INDEX IF NOT EXISTS FOR (tl:TargetedLocation) ON (tl.location_id)",
            "CREATE INDEX IF NOT EXISTS FOR (l:Language) ON (l.language_id)",
            "CREATE INDEX IF NOT EXISTS FOR (nk:NegativeKeyword) ON (nk.keyword_id)",
            # Add others as needed

            # CampaignBudget indexes
            "CREATE INDEX IF NOT EXISTS FOR (cb:CampaignBudget) ON (cb.budget_id)",
            "CREATE INDEX IF NOT EXISTS FOR (cb:CampaignBudget) ON (cb.name)",
            "CREATE INDEX IF NOT EXISTS FOR (cb:CampaignBudget) ON (cb.status)",
            
            # Ad indexes
            "CREATE INDEX IF NOT EXISTS FOR (a:Ad) ON (a.ad_id)",
            # AdGroup indexes
            "CREATE INDEX IF NOT EXISTS FOR (ag:AdGroup) ON (ag.ad_group_id)",
            # KeywordGroup indexes (Uses ad_group_id)
            "CREATE INDEX IF NOT EXISTS FOR (kg:KeywordGroup) ON (kg.ad_group_id)",
            # Audience indexes
            "CREATE INDEX IF NOT EXISTS FOR (a:Audience) ON (a.audience_id)",
            # Label indexes
            "CREATE INDEX IF NOT EXISTS FOR (l:Label) ON (l.label_id)",
            # AdAccount indexes
            "CREATE INDEX IF NOT EXISTS FOR (aa:AdAccount) ON (aa.account_id)",
            # Asset indexes
            "CREATE INDEX IF NOT EXISTS FOR (ast:Asset) ON (ast.asset_id)", # Added for consistency
            # ConversionAction indexes
            "CREATE INDEX IF NOT EXISTS FOR (ca:ConversionAction) ON (ca.account_id)", # Added for consistency

            # DailyMetric indexes (New)
            # "CREATE INDEX IF NOT EXISTS FOR (dm:DailyMetric) ON (dm.ad_id)",
            # "CREATE INDEX IF NOT EXISTS FOR (dm:DailyMetric) ON (dm.date)"
            
            # AdDailyMetric indexes
            "CREATE INDEX IF NOT EXISTS FOR (adm:AdDailyMetric) ON (adm.ad_id)",
            "CREATE INDEX IF NOT EXISTS FOR (adm:AdDailyMetric) ON (adm.date)",

            # AdOverallMetric indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (aom:AdOverallMetric) ON (aom.ad_id)",

            # AdMonthlyMetric indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (amm:AdMonthlyMetric) ON (amm.ad_id)",
            "CREATE INDEX IF NOT EXISTS FOR (amm:AdMonthlyMetric) ON (amm.month_start_date)",

            # AccountDailyMetric indexes
            "CREATE INDEX IF NOT EXISTS FOR (acm:AccountDailyMetric) ON (acm.account_id)",

            # AdGroupWeeklyMetric indexes
            "CREATE INDEX IF NOT EXISTS FOR (agwm:AdGroupWeeklyMetric) ON (agwm.ad_group_id)",
            "CREATE INDEX IF NOT EXISTS FOR (agwm:AdGroupWeeklyMetric) ON (agwm.week_start_date)",

            # CampaignWeeklyMetric indexes (Using :WeeklyMetric label)
            "CREATE INDEX IF NOT EXISTS FOR (cwm:WeeklyMetric) ON (cwm.campaign_id)",
            "CREATE INDEX IF NOT EXISTS FOR (cwm:WeeklyMetric) ON (cwm.week_start_date)", # Added missing comma
            # Add index on entity_type if querying across different weekly metrics
            # "CREATE INDEX IF NOT EXISTS FOR (wm:WeeklyMetric) ON (wm.entity_type)" 

            # CampaignOverallMetric indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (com:CampaignOverallMetric) ON (com.campaign_id)",

            # CampaignMonthlyMetric indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (cmm:CampaignMonthlyMetric) ON (cmm.campaign_id)",
            "CREATE INDEX IF NOT EXISTS FOR (cmm:CampaignMonthlyMetric) ON (cmm.month_start_date)",

            # Product indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.itemId)",

            # GeoLocation indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (gl:GeoLocation) ON (gl.criterionId)",

            # AdGroupBiddingSettings indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (agbs:AdGroupBiddingSettings) ON (agbs.adGroupResourceName)",
            
            # Audience Component Indexes (New)
            "CREATE INDEX IF NOT EXISTS FOR (ar:AgeRange) ON (ar.minAge, ar.maxAge)",
            "CREATE INDEX IF NOT EXISTS FOR (g:Gender) ON (g.genderType)",
            "CREATE INDEX IF NOT EXISTS FOR (ui:UserInterest) ON (ui.criterionId)",
            "CREATE INDEX IF NOT EXISTS FOR (ca:CustomAudience) ON (ca.customAudienceId)",

            # AccountMetrics Aggregates (New)
            "CREATE INDEX IF NOT EXISTS FOR (acm:AccountOverallMetric) ON (acm.account_id)",

            # AccountMonthlyMetric Indexes (Existing - Correctly placed)
            "CREATE INDEX IF NOT EXISTS FOR (amm:AccountMonthlyMetric) ON (amm.account_id)",
            "CREATE INDEX IF NOT EXISTS FOR (amm:AccountMonthlyMetric) ON (amm.month_start_date)",
        ]
        
        with self.driver.session() as session:
            for index in indexes:
                session.run(index)

    def transform_product(self, shopping_perf_df: pd.DataFrame):
        """Transform product data from shopping_performance_view into Product nodes and link them to Campaigns."""
        logger.info(f"Starting Product transformation for {len(shopping_perf_df)} shopping performance records.")

        required_cols = [
            'campaign_id', 
            'segments_product_item_id', 
            'segments_product_title',
            'segments_product_brand',
            'segments_product_condition',
            'segments_product_channel',
            'segments_product_merchant_id'
            # Add other necessary segment columns here (e.g., categories, types, custom attributes)
        ]
        if not all(col in shopping_perf_df.columns for col in required_cols):
            logger.warning(f"Skipping Product transformation: Missing required columns (need {required_cols}) from shopping_performance_view")
            return

        # Drop rows where product_item_id or campaign_id is missing, as they are essential for node creation and linking
        shopping_perf_df = shopping_perf_df.dropna(subset=['campaign_id', 'segments_product_item_id'])
        if shopping_perf_df.empty:
            logger.info("No valid shopping performance records with campaign_id and product_item_id found.")
            return
            
        # Select only relevant product-related columns and drop duplicates to create unique product nodes
        # We group by itemId to get one node per product
        product_cols = [
            'segments_product_item_id', 
            'segments_product_title', 
            'segments_product_brand',
            'segments_product_condition',
            'segments_product_channel',
            'segments_product_merchant_id',
            'segments_product_category_level1',
            'segments_product_category_level2',
            'segments_product_category_level3',
            'segments_product_category_level4',
            'segments_product_category_level5',
            'segments_product_type_l1',
            'segments_product_type_l2',
            'segments_product_type_l3',
            'segments_product_type_l4',
            'segments_product_type_l5',
            'segments_product_custom_attribute0',
            'segments_product_custom_attribute1',
            'segments_product_custom_attribute2',
            'segments_product_custom_attribute3',
            'segments_product_custom_attribute4'
            # Add other product segment columns as needed
        ]
        # Filter columns that actually exist in the DataFrame to avoid errors
        existing_product_cols = [col for col in product_cols if col in shopping_perf_df.columns]
        unique_products_df = shopping_perf_df[existing_product_cols].drop_duplicates(subset=['segments_product_item_id']).copy()

        logger.info(f"Identified {len(unique_products_df)} unique products.")

        # Prepare product nodes
        product_nodes = []
        for _, row in unique_products_df.iterrows():
            node = {
                'itemId': row['segments_product_item_id'],
                'title': row.get('segments_product_title', ''),
                'brand': row.get('segments_product_brand', ''),
                'condition': row.get('segments_product_condition', ''),
                'channel': row.get('segments_product_channel', ''),
                'merchantId': row.get('segments_product_merchant_id', ''),
                'categoryL1': row.get('segments_product_category_level1', ''),
                'categoryL2': row.get('segments_product_category_level2', ''),
                'categoryL3': row.get('segments_product_category_level3', ''),
                'categoryL4': row.get('segments_product_category_level4', ''),
                'categoryL5': row.get('segments_product_category_level5', ''),
                'productTypeL1': row.get('segments_product_type_l1', ''),
                'productTypeL2': row.get('segments_product_type_l2', ''),
                'productTypeL3': row.get('segments_product_type_l3', ''),
                'productTypeL4': row.get('segments_product_type_l4', ''),
                'productTypeL5': row.get('segments_product_type_l5', ''),
                'customAttribute0': row.get('segments_product_custom_attribute0', ''),
                'customAttribute1': row.get('segments_product_custom_attribute1', ''),
                'customAttribute2': row.get('segments_product_custom_attribute2', ''),
                'customAttribute3': row.get('segments_product_custom_attribute3', ''),
                'customAttribute4': row.get('segments_product_custom_attribute4', '')
                # Add other properties, potentially cleaning/converting types
            }
            # Fill NaN with empty strings or appropriate defaults
            product_nodes.append({k: '' if pd.isna(v) else v for k, v in node.items()})

        # Prepare Campaign -> Product relationships
        # Need unique pairs of (campaign_id, segments_product_item_id)
        relationships_df = shopping_perf_df[['campaign_id', 'segments_product_item_id']].drop_duplicates()
        relationships = []
        for _, row in relationships_df.iterrows():
            relationships.append({
                'start_key': 'campaign_id',
                'start_value': row['campaign_id'],
                'end_key': 'itemId',
                'end_value': row['segments_product_item_id']
            })
        
        # Execute in batches
        with self.driver.session() as session:
            # Create Product nodes
            if product_nodes:
                 for start_idx in range(0, len(product_nodes), self.BATCH_SIZE):
                     batch_nodes = product_nodes[start_idx : start_idx + self.BATCH_SIZE]
                     logger.debug(f"Creating batch of {len(batch_nodes)} Product nodes.")
                     session.execute_write(self.create_entity_nodes_batch, 'Product', batch_nodes)
            
            # Create Relationships
            if relationships:
                 for start_idx in range(0, len(relationships), self.BATCH_SIZE):
                     batch_rels = relationships[start_idx : start_idx + self.BATCH_SIZE]
                     logger.debug(f"Creating batch of {len(batch_rels)} ADVERTISES_PRODUCT relationships.")
                     session.execute_write(
                         self.create_relationships_batch,
                         'Campaign', 
                         'Product', 
                         'ADVERTISES_PRODUCT', 
                         batch_rels
                     )

        logger.info(f"Completed Product transformation. Created {len(product_nodes)} nodes and {len(relationships)} relationships.")

def main():
    logger.info("Starting data pipeline execution")
    
    # Neo4j connection details
    NEO4J_URI = "neo4j+s://2557c6ca.databases.neo4j.io"
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "pg8JVNkM25tYoxJA9Gg4orjBu-mX0S5GaNAYJ8Xv2mU")
    
    logger.info("Connecting to PostgreSQL database")
    try:
        # Create PostgreSQL connection
        pg_conn = psycopg2.connect(**{
            "database": "defaultdb",
            "user": "avnadmin",
            "password": "AVNS_yxwIBw5haAiSockuoja",
            "host": "pg-243dee0d-srinivasansridhar918-e25a.k.aivencloud.com",
            "port": "28021"
        })
        logger.info("Successfully connected to PostgreSQL database")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL database: {str(e)}")
        raise
    
    # Initialize transformer
    try:
        transformer = GraphTransformer(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        logger.info("Successfully initialized GraphTransformer")
    except Exception as e:
        logger.error(f"Failed to initialize GraphTransformer: {str(e)}")
        raise

    # Function to check if a table exists
    def table_exists(table_name):
        logger.debug(f"Checking if table exists: {table_name}")
        cursor = pg_conn.cursor()
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name = %s
                );
            """, (table_name,))
            exists = cursor.fetchone()[0]
            logger.debug(f"Table '{table_name}' exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking table existence for '{table_name}': {str(e)}")
            raise
        finally:
            cursor.close()

    # Load SQL data
    sql_data = {}
    
    # List of actual tables from schema.json
    tables = [
        "account_performance_report",
        "ad_group",
        "ad_group_ad",
        "ad_group_ad_label",
        "ad_group_criterion",
        "audience",
        "campaign",
        "campaign_budget",
        "campaign_criterion",
        "campaign_label",
        "click_view",
        "customer",
        "customer_label",
        "geographic_view",
        "keyword_view",
        "label",
        "shopping_performance_view",
        "topic_view",
        "user_interest",
        "user_location_view",
        "ad_group_ad_legacy",
        "ad_group_bidding_strategy"
    ]
    
    logger.info("Starting to load data from PostgreSQL tables")
    # Check each table and load data if it exists
    for table in tables:
        if table_exists(table):
            logger.info(f"Loading data from table: {table}")
            try:
                sql_data[table] = pd.read_sql(f"SELECT * FROM {table}", pg_conn)
                logger.info(f"Successfully loaded {len(sql_data[table])} records from {table}")
            except Exception as e:
                logger.error(f"Failed to load data from table '{table}': {str(e)}")
                raise
        else:
            logger.warning(f"Table '{table}' does not exist in the database. Skipping.")
    
    # Extract data for entities that don't have direct tables
    logger.info("Processing entities without direct tables")
    
    # For Asset - extract from ad_group_ad table
    if 'ad_group_ad' in sql_data:
        logger.info("Extracting Asset data from ad_group_ad table")
        try:
            # Extract image assets
            image_assets = sql_data['ad_group_ad'][
                sql_data['ad_group_ad']['ad_group_ad_ad_image_ad_image_url'].notna()
            ].copy()
            
            if not image_assets.empty:
                image_assets['asset_id'] = image_assets['ad_group_ad_ad_id'].astype(str) + '_image'
                image_assets['asset_type'] = 'IMAGE'
                image_assets['asset_name'] = image_assets['ad_group_ad_ad_image_ad_name'].fillna('Unnamed Image')
                image_assets['file_hash'] = image_assets['ad_group_ad_ad_image_ad_image_url']
                
                sql_data['asset'] = image_assets[['asset_id', 'asset_type', 'asset_name', 'file_hash']]
                logger.info(f"Extracted {len(image_assets)} image assets")
            else:
                logger.info("No image assets found in ad_group_ad table")
        except Exception as e:
            logger.error("Failed to extract asset data: {str(e)}")
            raise
    
    # For ConversionAction - extract from campaign_budget table
    if 'campaign_budget' in sql_data:
        logger.info("Extracting ConversionAction data from campaign_budget table")
        try:
            # Create a simple conversion action for each campaign budget
            conversion_actions = sql_data['campaign_budget'].copy()
            conversion_actions['conversion_action_id'] = conversion_actions['campaign_budget_id']
            conversion_actions['conversion_action_name'] = 'Default Conversion'
            conversion_actions['conversion_action_category'] = 'DEFAULT'
            conversion_actions['conversion_action_type'] = 'WEBSITE'
            conversion_actions['conversion_action_value_per_conversion'] = 0
            conversion_actions['conversion_action_counting_type'] = 'ONE_PER_CONVERSION'
            
            sql_data['conversion_action'] = conversion_actions[[
                'conversion_action_id', 'conversion_action_name', 
                'conversion_action_category', 'conversion_action_type',
                'conversion_action_value_per_conversion', 'conversion_action_counting_type',
                'customer_id'
            ]]
            logger.info(f"Extracted {len(conversion_actions)} conversion actions")
        except Exception as e:
            logger.error(f"Failed to extract conversion action data: {str(e)}")
            raise
    
    # Run pipeline
    try:
        logger.info("Starting pipeline execution")
        transformer.run_pipeline(sql_data)
        logger.info("Pipeline execution completed successfully")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
    finally:
        # Close connections
        logger.info("Closing database connections")
        pg_conn.close()
        transformer.close()

if __name__ == "__main__":
    try:
        main()
        logger.info("Program completed successfully")
    except Exception as e:
        logger.error(f"Program failed with error: {str(e)}")
        raise
