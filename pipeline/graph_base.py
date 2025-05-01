import json
import pandas as pd
from neo4j import GraphDatabase
from typing import Dict, List, Any
import logging
import os

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
                    
    def create_indexes(self):
        """Create indexes for Neo4j"""
        indexes = [
            # Campaign indexes
            "CREATE INDEX IF NOT EXISTS FOR (c:Campaign) ON (c.campaign_id)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Campaign) ON (c.name)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Campaign) ON (c.status)",
            
            # Specific CampaignCriterion type indexes
            "CREATE INDEX IF NOT EXISTS FOR (tl:TargetedLocation) ON (tl.location_id)",
            "CREATE INDEX IF NOT EXISTS FOR (l:Language) ON (l.language_id)",
            "CREATE INDEX IF NOT EXISTS FOR (nk:NegativeKeyword) ON (nk.keyword_id)",

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
            "CREATE INDEX IF NOT EXISTS FOR (cwm:WeeklyMetric) ON (cwm.week_start_date)",

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
                try:
                    session.run(index)
                    logger.info(f"Created index: {index}")
                except Exception as e:
                    logger.error(f"Error creating index {index}: {str(e)}")
                    
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
        MATCH (start:{start_type} {{{{rel.start_key}}: rel.start_value}})
        MATCH (end:{end_type} {{{{rel.end_key}}: rel.end_value}})
        MERGE (start)-[r:{rel_type}]->(end)
        """
        tx.run(query, {'relationships': relationships})
