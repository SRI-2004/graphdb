import os
import logging
import psycopg2
import pandas as pd
from typing import Dict, List, Optional

# Import the base GraphTransformer
from pipeline.graph_base import GraphTransformer

# Import the transformers
from pipeline.transformers.account_transformer import (
    transform_adaccount,
    transform_account_monthly_metrics,
    transform_account_overall_metrics,
    transform_conversion_action
)
from pipeline.transformers.campaign_transformer import (
    transform_campaign,
    transform_campaign_budget,
    transform_campaign_criterion,
    transform_campaign_weekly_metrics,
    transform_campaign_monthly_metrics,
    transform_campaign_overall_metrics
)
from pipeline.transformers.adgroup_transformer import transform_adgroup
from pipeline.transformers.ad_transformer import (
    transform_ad,
    transform_ad_daily_metrics,
    transform_ad_monthly_metrics,
    transform_ad_overall_metrics,
    transform_asset
)
from pipeline.transformers.audience_transformer import transform_audience
from pipeline.transformers.keyword_transformer import transform_keyword
from pipeline.transformers.label_transformer import transform_label
from pipeline.transformers.product_transformer import transform_product

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def table_exists(cursor, table_name: str) -> bool:
    """Check if a table exists in the database."""
    logger.debug(f"Checking if table exists: {table_name}")
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

def load_data_from_postgres(connection_params: Dict) -> Dict[str, pd.DataFrame]:
    """Load data from PostgreSQL tables."""
    sql_data = {}
    
    # List of tables to load
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
    
    try:
        # Create PostgreSQL connection
        pg_conn = psycopg2.connect(**connection_params)
        logger.info("Successfully connected to PostgreSQL database")
        
        cursor = pg_conn.cursor()
        
        # Check each table and load data if it exists
        for table in tables:
            if table_exists(cursor, table):
                logger.info(f"Loading data from table: {table}")
                try:
                    sql_data[table] = pd.read_sql(f"SELECT * FROM {table}", pg_conn)
                    logger.info(f"Successfully loaded {len(sql_data[table])} records from {table}")
                except Exception as e:
                    logger.error(f"Failed to load data from table '{table}': {str(e)}")
                    raise
            else:
                logger.warning(f"Table '{table}' does not exist in the database. Skipping.")
        
        # Process derived entities
        process_derived_entities(sql_data)
        
        # Close connection
        pg_conn.close()
        
    except Exception as e:
        logger.error(f"Failed to connect or load data: {str(e)}")
        raise
    
    return sql_data

def process_derived_entities(sql_data: Dict[str, pd.DataFrame]) -> None:
    """Process entities that don't have direct tables."""
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
            logger.error(f"Failed to extract asset data: {str(e)}")
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

def run_pipeline(transformer: GraphTransformer, sql_data: Dict[str, pd.DataFrame]) -> None:
    """Run the complete transformation pipeline in a dependency-aware order."""
    try:
        # == Step 1: Setup ==
        logger.info("Pipeline Step 1: Creating Neo4j constraints")
        transformer.create_constraints()
        logger.info("Pipeline Step 2: Creating Neo4j indexes")
        transformer.create_indexes()

        # == Step 2: Determine Primary Account ID ==
        customer_id = None
        if 'customer' in sql_data and not sql_data['customer'].empty:
            customer_id = sql_data['customer']['customer_id'].iloc[0]
            logger.info(f"Pipeline Step 3: Determined customer_id: {customer_id}")
        else:
            logger.warning("Could not determine primary customer_id from the 'customer' table. Some steps might be skipped.")

        # == Step 3: Core Entities (Top-Down) ==

        # AdAccount (Requires 'customer' table)
        if customer_id:
            logger.info("Pipeline Step 4: Starting AdAccount transformation")
            transform_adaccount(transformer, sql_data['customer'])
        else:
            logger.warning("Skipping AdAccount transformation as customer_id is unknown.")

        # Audience (Requires 'audience' table and links to AdAccount)
        if 'audience' in sql_data:
            logger.info("Pipeline Step 5: Starting Audience transformation")
            if customer_id:  # Check if AdAccount exists
                transform_audience(transformer, sql_data['audience'])
            else:
                logger.warning("Skipping Audience transformation as customer_id is unknown.")
        else:
            logger.warning("Skipping Audience transformation - 'audience' data missing.")

        # Label (Requires 'label', optionally 'customer_label' tables and links to AdAccount)
        if 'label' in sql_data:
            logger.info("Pipeline Step 6: Starting Label transformation")
            if customer_id:  # Check if AdAccount exists
                transform_label(transformer, sql_data['label'], sql_data.get('customer_label'))
            else:
                logger.warning("Skipping Label transformation as customer_id is unknown.")
        else:
            logger.warning("Skipping Label transformation - 'label' data missing.")

        # Campaign (Requires 'campaign' table and links to AdAccount)
        if 'campaign' in sql_data:
            logger.info("Pipeline Step 7: Starting Campaign transformation")
            if customer_id:
                transform_campaign(transformer, sql_data['campaign'], customer_id)
            else:
                logger.warning("Skipping Campaign transformation as customer_id is unknown.")
        else:
            logger.warning("Skipping Campaign transformation - 'campaign' data missing.")

        # CampaignBudget (Requires 'campaign_budget' table and links to Campaign)
        if 'campaign_budget' in sql_data:
            logger.info("Pipeline Step 8: Starting CampaignBudget transformation")
            if 'campaign' in sql_data:  # Check if Campaign data exists
                transform_campaign_budget(transformer, sql_data['campaign_budget'])
            else:
                logger.warning("Skipping CampaignBudget transformation - 'campaign' data missing.")
        else:
            logger.warning("Skipping CampaignBudget transformation - 'campaign_budget' data missing.")

        # CampaignCriterion (Requires 'campaign_criterion' table and links to Campaign)
        if 'campaign_criterion' in sql_data:
            logger.info("Pipeline Step 9: Starting CampaignCriterion transformation")
            if 'campaign' in sql_data:  # Check if Campaign data exists
                transform_campaign_criterion(transformer, sql_data['campaign_criterion'])
            else:
                logger.warning("Skipping CampaignCriterion transformation - 'campaign' data missing.")
        else:
            logger.warning("Skipping CampaignCriterion transformation - 'campaign_criterion' data missing.")

        # AdGroup (Requires 'ad_group' table and links to Campaign)
        if 'ad_group' in sql_data:
            logger.info("Pipeline Step 10: Starting AdGroup transformation")
            if 'campaign' in sql_data:  # Check if Campaign data exists
                transform_adgroup(transformer, sql_data['ad_group'])
            else:
                logger.warning("Skipping AdGroup transformation - 'campaign' data missing.")
        else:
            logger.warning("Skipping AdGroup transformation - 'ad_group' data missing.")
        
        # Ad (Requires 'ad_group_ad' table and links to AdGroup)
        if 'ad_group_ad' in sql_data:
            logger.info("Pipeline Step 11: Starting Ad transformation")
            if 'ad_group' in sql_data:  # Check if AdGroup data exists
                if customer_id:
                    transform_ad(transformer, sql_data['ad_group_ad'], customer_id)
                else:
                    logger.warning("Skipping Ad transformation as customer_id is unknown (required by current implementation).")
            else:
                logger.warning("Skipping Ad transformation - 'ad_group' data missing.")
        else:
            logger.warning("Skipping Ad transformation - 'ad_group_ad' data missing.")

        # KeywordGroup (Requires 'ad_group_criterion', filtered for keywords, links to AdGroup)
        if 'ad_group_criterion' in sql_data:
            keyword_df = sql_data['ad_group_criterion'][
                sql_data['ad_group_criterion']['ad_group_criterion_type'] == 'KEYWORD'
            ].copy()  # Use copy to avoid SettingWithCopyWarning
            if not keyword_df.empty:
                logger.info(f"Pipeline Step 12: Starting KeywordGroup transformation for {len(keyword_df)} keywords")
                if 'ad_group' in sql_data:  # Check if AdGroup data exists
                    if customer_id:
                        transform_keyword(transformer, keyword_df, customer_id)
                    else:
                        logger.warning("Skipping KeywordGroup transformation as customer_id is unknown (required by current implementation).")
                else:
                    logger.warning("Skipping KeywordGroup transformation - 'ad_group' data missing.")
            else:
                logger.info("No keyword criteria found for KeywordGroup transformation.")
        else:
            logger.warning("Skipping KeywordGroup transformation - 'ad_group_criterion' data missing.")

        # Asset (Derived from 'ad_group_ad')
        if 'asset' in sql_data:  # Check if derived data exists
            logger.info("Pipeline Step 13: Starting Asset transformation")
            transform_asset(transformer, sql_data['asset'])
        else:
            logger.warning("Skipping Asset transformation - derived 'asset' data missing (check main() logic).")

        # ConversionAction (Derived from 'campaign_budget' in main, links to AdAccount)
        if 'conversion_action' in sql_data:  # Check if derived data exists
            logger.info("Pipeline Step 14: Starting ConversionAction transformation")
            if customer_id:  # Check if AdAccount exists
                transform_conversion_action(transformer, sql_data['conversion_action'])
            else:
                logger.warning("Skipping ConversionAction transformation as customer_id is unknown.")
        else:
            logger.warning("Skipping ConversionAction transformation - derived 'conversion_action' data missing (check main() logic).")

        # Product (Requires 'shopping_performance_view' table and links to Campaign)
        if 'shopping_performance_view' in sql_data:
            logger.info("Pipeline Step 14.5: Starting Product transformation")
            if 'campaign' in sql_data:  # Check if Campaign data exists
                transform_product(transformer, sql_data['shopping_performance_view'])
            else:
                logger.warning("Skipping Product transformation - 'campaign' data missing.")
        else:
            logger.warning("Skipping Product transformation - 'shopping_performance_view' data missing.")

        # == Step 4: Metric Nodes (linking to existing core entities) ==

        # AccountMonthlyMetric (Requires 'account_performance_report' table and links to AdAccount)
        if 'account_performance_report' in sql_data:
            logger.info("Pipeline Step 15: Starting AccountMonthlyMetric transformation")
            if customer_id:  # Check if AdAccount exists
                transform_account_monthly_metrics(transformer, sql_data['account_performance_report'])
            else:
                logger.warning("Skipping AccountMonthlyMetric transformation as customer_id is unknown.")
        else:
            logger.warning("Skipping AccountMonthlyMetric transformation - 'account_performance_report' data missing.")

        # AccountOverallMetric (Requires 'account_performance_report' table and links to AdAccount)
        if 'account_performance_report' in sql_data:
            logger.info("Pipeline Step 15.1: Starting AccountOverallMetric transformation")
            if customer_id:  # Check if AdAccount exists
                transform_account_overall_metrics(transformer, sql_data['account_performance_report'])
            else:
                logger.warning("Skipping AccountOverallMetric transformation as customer_id is unknown.")
        else:
            logger.warning("Skipping AccountOverallMetric transformation - 'account_performance_report' data missing.")

        # CampaignWeeklyMetric (Requires 'campaign' table and links to Campaign)
        if 'campaign' in sql_data:
            logger.info("Pipeline Step 16: Starting CampaignWeeklyMetric transformation (using 'campaign' table - may lack metrics)")
            # Check if Campaign node creation was attempted
            if 'campaign' in sql_data:
                transform_campaign_weekly_metrics(transformer, sql_data['campaign'])
            else:
                logger.warning("Skipping CampaignWeeklyMetric transformation - 'campaign' data missing.")
        else:
            logger.warning("Skipping CampaignWeeklyMetric transformation - 'campaign' data missing.")

        # CampaignOverallMetric (Requires 'campaign' table)
        if 'campaign' in sql_data:
            logger.info("Pipeline Step 16.1: Starting CampaignOverallMetric transformation")
            transform_campaign_overall_metrics(transformer, sql_data['campaign'])
        else:
            logger.warning("Skipping CampaignOverallMetric transformation - 'campaign' data missing.")

        # CampaignMonthlyMetric (Requires 'campaign' table)
        if 'campaign' in sql_data:
            logger.info("Pipeline Step 16.2: Starting CampaignMonthlyMetric transformation")
            transform_campaign_monthly_metrics(transformer, sql_data['campaign'])
        else:
            logger.warning("Skipping CampaignMonthlyMetric transformation - 'campaign' data missing.")

        # AdDailyMetric (Requires 'ad_group_ad_legacy' table and links to Ad)
        if 'ad_group_ad_legacy' in sql_data:
            logger.info("Pipeline Step 17: Starting AdDailyMetric transformation (using 'ad_group_ad_legacy' table)")
            # Check if Ad node creation was attempted
            if 'ad_group_ad' in sql_data:  # Still need Ad nodes to exist
                transform_ad_daily_metrics(transformer, sql_data['ad_group_ad_legacy'])
            else:
                logger.warning("Skipping AdDailyMetric transformation - Ad nodes missing (needs 'ad_group_ad' table data).")
        else:
            logger.warning("Skipping AdDailyMetric transformation - 'ad_group_ad_legacy' data missing.")

        # AdOverallMetric (Requires 'ad_group_ad_legacy' table and links to Ad)
        if 'ad_group_ad_legacy' in sql_data:
            logger.info("Pipeline Step 18: Starting AdOverallMetric transformation (using 'ad_group_ad_legacy' table)")
            # Check if Ad nodes exist (needed for relationship)
            if 'ad_group_ad' in sql_data:
                transform_ad_overall_metrics(transformer, sql_data['ad_group_ad_legacy'])
            else:
                logger.warning("Skipping AdOverallMetric transformation - Ad nodes missing (needs 'ad_group_ad' table data).")
        else:
            logger.warning("Skipping AdOverallMetric transformation - 'ad_group_ad_legacy' data missing.")

        # AdMonthlyMetric (Requires 'ad_group_ad_legacy' table and links to Ad)
        if 'ad_group_ad_legacy' in sql_data:
            logger.info("Pipeline Step 19: Starting AdMonthlyMetric transformation (using 'ad_group_ad_legacy' table)")
            # Check if Ad nodes exist (needed for relationship)
            if 'ad_group_ad' in sql_data:
                transform_ad_monthly_metrics(transformer, sql_data['ad_group_ad_legacy'])
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
        raise

def main():
    logger.info("Starting data pipeline execution")
    
    # Neo4j connection details
    NEO4J_URI = "neo4j+s://2557c6ca.databases.neo4j.io"
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "pg8JVNkM25tYoxJA9Gg4orjBu-mX0S5GaNAYJ8Xv2mU")
    
    # PostgreSQL connection parameters
    pg_params = {
        "database": "defaultdb",
        "user": "avnadmin",
        "password": "AVNS_yxwIBw5haAiSockuoja",
        "host": "pg-243dee0d-srinivasansridhar918-e25a.k.aivencloud.com",
        "port": "28021"
    }
    
    # Initialize transformer
    try:
        transformer = GraphTransformer(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        logger.info("Successfully initialized GraphTransformer")
    except Exception as e:
        logger.error(f"Failed to initialize GraphTransformer: {str(e)}")
        raise
    
    try:
        # Load data from PostgreSQL
        sql_data = load_data_from_postgres(pg_params)
        
        # Run pipeline
        logger.info("Starting pipeline execution")
        run_pipeline(transformer, sql_data)
        logger.info("Pipeline execution completed successfully")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
    finally:
        # Close connections
        logger.info("Closing Neo4j connection")
        transformer.close()

if __name__ == "__main__":
    try:
        main()
        logger.info("Program completed successfully")
    except Exception as e:
        logger.error(f"Program failed with error: {str(e)}")
        raise
