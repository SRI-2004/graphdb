import json
import pandas as pd
from neo4j import GraphDatabase
from typing import Dict, List, Any
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class FacebookPipeline:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """Initialize the pipeline with Neo4j connection details"""
        logger.info("Initializing FacebookPipeline")
        self.driver = None # Initialize driver to None
        try:
            self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
            # Ensure the driver is valid before proceeding
            self.driver.verify_connectivity()
            logger.info("Successfully connected to Neo4j database for Facebook pipeline")
            self.BATCH_SIZE = 1000 # Define batch size
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j database or verify connectivity: {str(e)}")
            # Optionally close if partially initialized
            if self.driver:
                self.driver.close()
            raise # Re-raise the exception to halt execution if connection fails

    def close(self):
        """Close the Neo4j connection"""
        if self.driver:
            try:
                self.driver.close()
                logger.info("Closed Neo4j connection for Facebook pipeline")
            except Exception as e:
                 logger.error(f"Error closing Neo4j connection: {str(e)}")
            finally:
                 self.driver = None # Ensure driver is set to None after closing

    def create_constraints(self):
        """Create constraints for Neo4j (Facebook specific)"""
        logger.info("Creating Facebook constraints")
        constraints = [
            # AdAccount constraints
            "CREATE CONSTRAINT fb_adaccount_id_unique IF NOT EXISTS FOR (fa:FbAdAccount) REQUIRE fa.account_id IS UNIQUE",
            # Campaign constraints
            "CREATE CONSTRAINT fb_campaign_id_unique IF NOT EXISTS FOR (fc:FbCampaign) REQUIRE fc.id IS UNIQUE",
            # AdSet constraints
            "CREATE CONSTRAINT fb_adset_id_unique IF NOT EXISTS FOR (fas:FbAdSet) REQUIRE fas.id IS UNIQUE",
            # Ad constraints
            "CREATE CONSTRAINT fb_ad_id_unique IF NOT EXISTS FOR (fa:FbAd) REQUIRE fa.id IS UNIQUE",
            # AdCreative constraints
            "CREATE CONSTRAINT fb_adcreative_id_unique IF NOT EXISTS FOR (fac:FbAdCreative) REQUIRE fac.id IS UNIQUE",
            # Label constraints
            "CREATE CONSTRAINT fb_adlabel_id_unique IF NOT EXISTS FOR (fl:FbAdLabel) REQUIRE fl.id IS UNIQUE",
            # Image constraints
            "CREATE CONSTRAINT fb_image_id_unique IF NOT EXISTS FOR (fi:FbImage) REQUIRE fi.id IS UNIQUE",
            # Weekly Insight constraints
            "CREATE CONSTRAINT fb_weekly_insight_id_unique IF NOT EXISTS FOR (fwi:FbWeeklyInsight) REQUIRE fwi.insight_id IS UNIQUE",
            # Monthly Insight constraints
            "CREATE CONSTRAINT fb_monthly_insight_id_unique IF NOT EXISTS FOR (fmi:FbMonthlyInsight) REQUIRE fmi.insight_id IS UNIQUE",
            # Weekly Campaign Insight constraints
            "CREATE CONSTRAINT fb_weekly_campaign_insight_id_unique IF NOT EXISTS FOR (fwci:FbWeeklyCampaignInsight) REQUIRE fwci.insight_id IS UNIQUE",
            # Monthly Campaign Insight constraints
            "CREATE CONSTRAINT fb_monthly_campaign_insight_id_unique IF NOT EXISTS FOR (fmci:FbMonthlyCampaignInsight) REQUIRE fmci.insight_id IS UNIQUE",
        ]
        with self.driver.session(database="neo4j") as session: # Specify the database if needed
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    logger.error(f"Error creating constraint {constraint}: {str(e)}")

    def create_indexes(self):
        """Create indexes for Neo4j (Facebook specific)"""
        logger.info("Creating Facebook indexes")
        indexes = [
            # AdAccount indexes
            "CREATE INDEX fb_adaccount_id_index IF NOT EXISTS FOR (fa:FbAdAccount) ON (fa.account_id)",
            "CREATE INDEX fb_adaccount_name_index IF NOT EXISTS FOR (fa:FbAdAccount) ON (fa.name)",
            # Campaign indexes
            "CREATE INDEX fb_campaign_id_index IF NOT EXISTS FOR (fc:FbCampaign) ON (fc.id)",
            "CREATE INDEX fb_campaign_name_index IF NOT EXISTS FOR (fc:FbCampaign) ON (fc.name)",
            "CREATE INDEX fb_campaign_status_index IF NOT EXISTS FOR (fc:FbCampaign) ON (fc.status)",
            "CREATE INDEX fb_campaign_objective_index IF NOT EXISTS FOR (fc:FbCampaign) ON (fc.objective)",
            # AdSet indexes
            "CREATE INDEX fb_adset_id_index IF NOT EXISTS FOR (fas:FbAdSet) ON (fas.id)",
            "CREATE INDEX fb_adset_name_index IF NOT EXISTS FOR (fas:FbAdSet) ON (fas.name)", # Add if name is available
            # Ad indexes
            "CREATE INDEX fb_ad_id_index IF NOT EXISTS FOR (fa:FbAd) ON (fa.id)",
            "CREATE INDEX fb_ad_name_index IF NOT EXISTS FOR (fa:FbAd) ON (fa.name)",
            "CREATE INDEX fb_ad_status_index IF NOT EXISTS FOR (fa:FbAd) ON (fa.status)",
            # AdCreative indexes
            "CREATE INDEX fb_adcreative_id_index IF NOT EXISTS FOR (fac:FbAdCreative) ON (fac.id)",
            "CREATE INDEX fb_adcreative_name_index IF NOT EXISTS FOR (fac:FbAdCreative) ON (fac.name)",
            "CREATE INDEX fb_adcreative_status_index IF NOT EXISTS FOR (fac:FbAdCreative) ON (fac.status)",
            # Label indexes
            "CREATE INDEX fb_adlabel_id_index IF NOT EXISTS FOR (fl:FbAdLabel) ON (fl.id)",
            "CREATE INDEX fb_adlabel_name_index IF NOT EXISTS FOR (fl:FbAdLabel) ON (fl.name)",
            # Image indexes
            "CREATE INDEX fb_image_id_index IF NOT EXISTS FOR (fi:FbImage) ON (fi.id)",
            "CREATE INDEX fb_image_hash_index IF NOT EXISTS FOR (fi:FbImage) ON (fi.hash)", # Index hash for potential lookups
            # Weekly Insight indexes
            "CREATE INDEX fb_weekly_insight_id_index IF NOT EXISTS FOR (fwi:FbWeeklyInsight) ON (fwi.insight_id)",
            "CREATE INDEX fb_weekly_insight_ad_id_index IF NOT EXISTS FOR (fwi:FbWeeklyInsight) ON (fwi.ad_id)",
            # Monthly Insight indexes
            "CREATE INDEX fb_monthly_insight_id_index IF NOT EXISTS FOR (fmi:FbMonthlyInsight) ON (fmi.insight_id)",
            "CREATE INDEX fb_monthly_insight_ad_id_index IF NOT EXISTS FOR (fmi:FbMonthlyInsight) ON (fmi.ad_id)",
            "CREATE INDEX fb_monthly_insight_date_index IF NOT EXISTS FOR (fmi:FbMonthlyInsight) ON (fmi.period_start)",
            "CREATE INDEX fb_monthly_insight_type_index IF NOT EXISTS FOR (fmi:FbMonthlyInsight) ON (fmi.insight_type)",
            # Weekly Campaign Insight indexes
            "CREATE INDEX fb_weekly_campaign_insight_id_index IF NOT EXISTS FOR (fwci:FbWeeklyCampaignInsight) ON (fwci.insight_id)",
            "CREATE INDEX fb_weekly_campaign_insight_campaign_id_index IF NOT EXISTS FOR (fwci:FbWeeklyCampaignInsight) ON (fwci.campaign_id)",
            "CREATE INDEX fb_weekly_campaign_insight_date_index IF NOT EXISTS FOR (fwci:FbWeeklyCampaignInsight) ON (fwci.period_start)",
            "CREATE INDEX fb_weekly_campaign_insight_type_index IF NOT EXISTS FOR (fwci:FbWeeklyCampaignInsight) ON (fwci.insight_type)",
            # Monthly Campaign Insight indexes
            "CREATE INDEX fb_monthly_campaign_insight_id_index IF NOT EXISTS FOR (fmci:FbMonthlyCampaignInsight) ON (fmci.insight_id)",
            "CREATE INDEX fb_monthly_campaign_insight_campaign_id_index IF NOT EXISTS FOR (fmci:FbMonthlyCampaignInsight) ON (fmci.campaign_id)",
            "CREATE INDEX fb_monthly_campaign_insight_date_index IF NOT EXISTS FOR (fmci:FbMonthlyCampaignInsight) ON (fmci.period_start)",
            "CREATE INDEX fb_monthly_campaign_insight_type_index IF NOT EXISTS FOR (fmci:FbMonthlyCampaignInsight) ON (fmci.insight_type)",
        ]
        with self.driver.session(database="neo4j") as session: # Specify the database if needed
            for index in indexes:
                try:
                    session.run(index)
                    logger.info(f"Created index: {index}")
                except Exception as e:
                    logger.error(f"Error creating index {index}: {str(e)}")

    def create_entity_nodes_batch(self, tx, entity_type: str, nodes: List[Dict[str, Any]], id_property: str | List[str]):
        """Create multiple entity nodes in a single transaction using UNWIND"""
        if not nodes:
             logger.debug(f"No nodes provided for entity type: {entity_type}. Skipping batch creation.")
             return

        if not id_property:
            raise ValueError(f"id_property must be specified for entity type: {entity_type}")

        # Handle composite key case
        if isinstance(id_property, list):
            if not id_property: # Ensure list is not empty
                 raise ValueError(f"id_property list cannot be empty for entity type: {entity_type}")
            # Check if all nodes have all composite key properties
            for node in nodes:
                if not all(prop in node for prop in id_property):
                     logger.error(f"Node missing composite key property for {entity_type}: {node}. Keys required: {id_property}")
                     # Option: skip this node or raise error
                     # For now, let it fail during MERGE to see Neo4j error
                     pass # Or raise ValueError("Node missing composite key property")
            merge_clause = f"MERGE (e:{entity_type} {{{', '.join(f'{prop}: node.{prop}' for prop in id_property)}}})"
        else: # Handle single property key case
             # Check if all nodes have the single key property
             for node in nodes:
                 if id_property not in node:
                     logger.error(f"Node missing ID property '{id_property}' for {entity_type}: {node}")
                     # Option: skip or raise
                     pass # Or raise ValueError("Node missing ID property")
             merge_clause = f"MERGE (e:{entity_type} {{{id_property}: node.{id_property}}})"

        query = f"""
        UNWIND $nodes as node
        {merge_clause}
        SET e += node
        """
        try:
            tx.run(query, {'nodes': nodes})
            logger.debug(f"Successfully processed batch for entity type: {entity_type}")
        except Exception as e:
             logger.error(f"Error processing node batch for {entity_type}: {e}")
             # Log the first few nodes for debugging
             logger.error(f"First few nodes in failing batch: {nodes[:3]}")
             raise # Re-raise to potentially abort transaction

    def create_relationships_batch(self, tx, start_type: str, end_type: str, rel_type: str,
                                 relationships: List[Dict[str, Any]], start_key: str = 'id', end_key: str = 'id'):
        """Create multiple relationships in a single transaction using UNWIND.
           Assumes standard 'id' key unless specified.
        """
        if not relationships:
             logger.debug(f"No relationships provided for type {rel_type} from {start_type} to {end_type}. Skipping batch creation.")
             return

        # Validate relationships format (basic check)
        for rel in relationships:
             if not all(k in rel for k in ['start_value', 'end_value']):
                 logger.error(f"Relationship missing start_value or end_value: {rel}")
                 # Option: skip or raise
                 continue # Skip this malformed relationship

        query = f"""
        UNWIND $relationships as rel
        MATCH (start:{start_type} {{{start_key}: rel.start_value}})
        MATCH (end:{end_type} {{{end_key}: rel.end_value}})
        MERGE (start)-[r:{rel_type}]->(end)
        """
        # Add relationship properties if needed: SET r += rel.properties
        try:
            tx.run(query, {'relationships': relationships})
            logger.debug(f"Successfully processed batch for relationship type: {rel_type} between {start_type} and {end_type}")
        except Exception as e:
             logger.error(f"Error processing relationship batch for {rel_type} ({start_type} -> {end_type}): {e}")
             # Log the first few relationships for debugging
             logger.error(f"First few relationships in failing batch: {relationships[:3]}")
             raise # Re-raise

    # --- Transformation Methods (Placeholders) ---

    def transform_adaccount(self, adaccount_df: pd.DataFrame):
        """Transform AdAccount data"""
        if adaccount_df.empty:
            logger.info("AdAccount dataframe is empty. Skipping transformation.")
            return
        
        entity_type = "FbAdAccount"
        id_property = "account_id" # Using 'account_id' from the schema
        logger.info(f"Transforming {len(adaccount_df)} {entity_type} records...")

        # Select and rename columns, handle missing values
        # Adjust column names based on your actual DataFrame columns after loading
        required_cols = {
            'account_id': 'account_id', # This is the crucial ID
            'id': 'graph_api_id',       # Facebook Graph API ID, different from account_id
            'name': 'name',
            'account_status': 'status', # Map account_status to 'status'
            'currency': 'currency',
            'timezone_id': 'timezone_id',
            'timezone_name': 'timezone_name',
            'created_time': 'created_time',
            'business_name': 'business_name',
            'business_country_code': 'business_country_code',
            'owner': 'owner_id', # Assuming 'owner' contains the user ID
            'amount_spent': 'amount_spent',
            'balance': 'balance',
            'spend_cap': 'spend_cap',
            # Add other relevant fields, ensuring they exist in the DataFrame
            'age': 'age',
             'tax_id': 'tax_id',
             'partner': 'partner',
             'user_tasks': 'user_tasks', # May need JSON serialization if complex
             'is_personal': 'is_personal',
             'tax_id_type': 'tax_id_type',
             'business_city': 'business_city',
             'capabilities': 'capabilities', # May need JSON serialization
             'disable_reason': 'disable_reason',
             'funding_source': 'funding_source', # May need JSON serialization
             'is_prepay_account': 'is_prepay_account',
             'is_tax_id_required': 'is_tax_id_required',
             'has_migrated_permissions': 'has_migrated_permissions'
        }
        
        # Filter DataFrame to only include columns that actually exist
        cols_to_select = {df_col: node_prop for df_col, node_prop in required_cols.items() if df_col in adaccount_df.columns}
        
        if id_property not in cols_to_select:
             logger.error(f"Critical Error: The ID property '{id_property}' is not present in the adaccount_df columns. Cannot proceed with {entity_type} transformation.")
             return

        processed_df = adaccount_df[list(cols_to_select.keys())].copy()
        processed_df.rename(columns=cols_to_select, inplace=True)

        # Convert potential complex types (like lists/dicts) to JSON strings if necessary
        for col in ['user_tasks', 'capabilities', 'funding_source']: # Example columns
             if col in processed_df.columns:
                 # Check if column contains non-scalar types before attempting JSON conversion
                 if processed_df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    processed_df[col] = processed_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)


        # Convert to list of dictionaries and handle NaN/NaT
        # Fill NaN with empty string, NaT with None (or empty string)
        processed_df = processed_df.fillna('') 
        nodes = processed_df.to_dict('records')

        # Clean node dictionaries (replace numpy types if any, ensure basic types)
        cleaned_nodes = []
        for node in nodes:
             cleaned_node = {}
             for key, value in node.items():
                 if pd.isna(value) or value == '':
                     cleaned_node[key] = '' # Use empty string for missing values
                 elif isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
                      cleaned_node[key] = str(value) if pd.notna(value) else ''
                 elif isinstance(value, (int, float, bool, str)):
                     cleaned_node[key] = value
                 else:
                     # Attempt to convert other types to string, log warning
                     try:
                          cleaned_node[key] = str(value)
                          # logger.warning(f"Converted unexpected type {type(value)} to string for key '{key}' in {entity_type}")
                     except Exception as e:
                         logger.error(f"Could not convert value for key '{key}' in {entity_type}: {e}. Value: {value}. Setting to empty string.")
                         cleaned_node[key] = ''
             # Ensure the ID property is present and not empty
             if id_property not in cleaned_node or cleaned_node[id_property] == '':
                 logger.warning(f"Skipping node due to missing or empty ID ('{id_property}'): {cleaned_node}")
                 continue
             cleaned_nodes.append(cleaned_node)

        # Create nodes in batches
        with self.driver.session(database="neo4j") as session: # Specify the database if needed
            for i in range(0, len(cleaned_nodes), self.BATCH_SIZE):
                batch_nodes = cleaned_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed batch {i // self.BATCH_SIZE + 1} for {entity_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                     logger.error(f"Error writing batch for {entity_type}: {e}")
                     # Decide if you want to stop the whole process or just log the error
                     # raise # Uncomment to stop on error
        
        logger.info(f"Finished transforming {entity_type} records.")

    def transform_campaign(self, campaign_df: pd.DataFrame):
        """Transform Campaign data and link to AdAccount and Labels"""
        if campaign_df.empty:
            logger.info("Campaign dataframe is empty. Skipping transformation.")
            return
        
        entity_type = "FbCampaign"
        id_property = "id" # Using 'id' from the campaigns schema as the unique identifier
        logger.info(f"Transforming {len(campaign_df)} {entity_type} records...")

        # Select and rename columns
        required_cols = {
            'id': 'id', # Campaign ID
            'account_id': 'account_id', # Needed for relationship to FbAdAccount
            'name': 'name',
            'status': 'status',
            'effective_status': 'effective_status',
            'objective': 'objective',
            'spend_cap': 'spend_cap',
            'start_time': 'start_time',
            'stop_time': 'stop_time',
            'created_time': 'created_time',
            'updated_time': 'updated_time',
            'buying_type': 'buying_type',
            'bid_strategy': 'bid_strategy',
            'daily_budget': 'daily_budget',
            'lifetime_budget': 'lifetime_budget',
            'budget_remaining': 'budget_remaining',
            'budget_rebalance_flag': 'budget_rebalance_flag',
            'special_ad_category': 'special_ad_category',
            'adlabels': 'adlabels', # May need JSON serialization
            'issues_info': 'issues_info' # May need JSON serialization
            # Add other relevant fields like 'source_campaign_id', 'boosted_object_id' if needed
        }
        
        # Filter DataFrame
        cols_to_select = {df_col: node_prop for df_col, node_prop in required_cols.items() if df_col in campaign_df.columns}
        
        # Check for essential columns (id for node, account_id for relationship)
        if id_property not in cols_to_select:
            logger.error(f"Critical Error: The ID property '{id_property}' is not present in the campaign_df columns. Cannot proceed with {entity_type} transformation.")
            return
        if 'account_id' not in cols_to_select:
             logger.error(f"Critical Error: The 'account_id' property is not present in the campaign_df columns. Cannot create relationship for {entity_type}.")
             return

        processed_df = campaign_df[list(cols_to_select.keys())].copy()
        processed_df.rename(columns=cols_to_select, inplace=True)
        
        # --- Data Cleaning and Preparation ---
        # Keep original adlabels column before filling NA for parsing
        processed_df['adlabels_raw'] = campaign_df['adlabels'] 
        
        # Convert complex types to JSON strings (excluding adlabels_raw)
        for col in ['issues_info']: # Only serialize non-label complex fields here
            if col in processed_df.columns:
                if processed_df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    processed_df[col] = processed_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        # Convert numeric fields, handle potential errors
        for col in ['spend_cap', 'daily_budget', 'lifetime_budget', 'budget_remaining']:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)

        # Convert to list of dictionaries, handle NaN/NaT
        processed_df = processed_df.fillna('')
        nodes = processed_df.to_dict('records')

        # Clean nodes, prepare relationships to Account and Labels
        cleaned_nodes = []
        account_relationships = []
        label_relationships = []
        unique_labels = {} # Use dict to store unique labels {label_id: {id:.., name:..}}
        for node in nodes:
             cleaned_node = {}
             account_id_val = None # Store account_id for relationship
             campaign_id_val = None # Store campaign id for relationship

             for key, value in node.items():
                 if key == 'account_id':
                     account_id_val = str(value) if pd.notna(value) else None
                     continue # Don't add account_id as property to Campaign node itself if not desired, but keep for relationship
                 elif key == id_property:
                      campaign_id_val = str(value) if pd.notna(value) else None
                      cleaned_node[key] = campaign_id_val
                 elif key == 'adlabels': # Handle the processed (potentially JSON string) adlabels
                     # Store as string, actual parsing happens from adlabels_raw below
                     cleaned_node[key] = str(value)
                 elif key == 'adlabels_raw': # Skip adding raw data to node properties
                     continue 
                 elif pd.isna(value) or value == '':
                     cleaned_node[key] = ''
                 elif isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
                      cleaned_node[key] = str(value) if pd.notna(value) else ''
                 elif isinstance(value, (int, float, bool, str)):
                     cleaned_node[key] = value
                 else:
                     try:
                          cleaned_node[key] = str(value)
                     except Exception as e:
                         logger.error(f"Could not convert value for key '{key}' in {entity_type}: {e}. Value: {value}.")
                         cleaned_node[key] = ''
             
             # Check if essential IDs are present for node and relationship
             if not campaign_id_val:
                 logger.warning(f"Skipping campaign node due to missing or empty ID ('{id_property}'): {node}")
                 continue
             if not account_id_val:
                  logger.warning(f"Skipping relationship for campaign '{campaign_id_val}' due to missing account_id.")
                  # Still create the node, just skip the relationship
             else:
                 account_relationships.append({
                      'start_value': account_id_val, # account_id from FbAdAccount
                      'end_value': campaign_id_val     # id from FbCampaign
                  })
                 
             cleaned_nodes.append(cleaned_node)

             # Parse adlabels_raw for label nodes and relationships
             adlabels_data = node.get('adlabels_raw')
             if pd.notna(adlabels_data) and adlabels_data:
                 parsed_labels = []
                 if isinstance(adlabels_data, str):
                     try:
                         parsed_labels = json.loads(adlabels_data)
                     except json.JSONDecodeError:
                         logger.warning(f"Could not parse adlabels JSON string for campaign {campaign_id_val}: {adlabels_data}")
                 elif isinstance(adlabels_data, list):
                     parsed_labels = adlabels_data # Assume it's already a list of dicts
                 
                 if isinstance(parsed_labels, list):
                     for label_item in parsed_labels:
                         if isinstance(label_item, dict) and 'id' in label_item:
                             label_id = str(label_item['id'])
                             label_name = str(label_item.get('name', '')) # Get name if available
                             if label_id:
                                 # Add label to unique set for node creation later
                                 if label_id not in unique_labels:
                                     unique_labels[label_id] = {'id': label_id, 'name': label_name}
                                 # Prepare relationship
                                 label_relationships.append({
                                     'start_value': campaign_id_val, # Campaign ID
                                     'end_value': label_id         # Label ID
                                 })
                         else:
                              logger.warning(f"Unexpected item format in parsed adlabels for campaign {campaign_id_val}: {label_item}")
                 else:
                     logger.warning(f"Parsed adlabels is not a list for campaign {campaign_id_val}: {parsed_labels}")

        # Create nodes and relationships in batches
        with self.driver.session(database="neo4j") as session:
            # Create Campaign Nodes
            for i in range(0, len(cleaned_nodes), self.BATCH_SIZE):
                batch_nodes = cleaned_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed node batch {i // self.BATCH_SIZE + 1} for {entity_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                     logger.error(f"Error writing node batch for {entity_type}: {e}")
                     # Decide behavior on error

            # Create Relationships (Account -> Campaign)
            rel_type = "HAS_CAMPAIGN"
            start_node_type = "FbAdAccount"
            start_node_key = "account_id"
            end_node_type = entity_type # "FbCampaign"
            end_node_key = id_property # "id"
            
            for i in range(0, len(account_relationships), self.BATCH_SIZE):
                batch_rels = account_relationships[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_relationships_batch,
                        start_node_type,
                        end_node_type,
                        rel_type,
                        batch_rels,
                        start_key=start_node_key,
                        end_key=end_node_key
                    )
                    logger.info(f"Processed relationship batch {i // self.BATCH_SIZE + 1} for {rel_type} ({len(batch_rels)} rels)")
                except Exception as e:
                     logger.error(f"Error writing relationship batch for {rel_type}: {e}")
                     # Decide behavior on error

            # Create Label Nodes (if any were found)
            if unique_labels:
                label_nodes = list(unique_labels.values()) # Convert dict values to list
                label_entity_type = "FbAdLabel"
                label_id_property = "id"
                logger.info(f"Creating/Merging {len(label_nodes)} unique {label_entity_type} nodes.")
                for i in range(0, len(label_nodes), self.BATCH_SIZE):
                    batch_label_nodes = label_nodes[i:i + self.BATCH_SIZE]
                    try:
                        session.execute_write(
                            self.create_entity_nodes_batch,
                            label_entity_type,
                            batch_label_nodes,
                            label_id_property
                        )
                        logger.info(f"Processed node batch {i // self.BATCH_SIZE + 1} for {label_entity_type} ({len(batch_label_nodes)} nodes)")
                    except Exception as e:
                        logger.error(f"Error writing node batch for {label_entity_type}: {e}")

            # Create Relationships (Campaign -> Label)
            if label_relationships:
                label_rel_type = "HAS_LABEL"
                label_start_node_type = entity_type # "FbCampaign"
                label_start_node_key = id_property # "id"
                label_end_node_type = "FbAdLabel"
                label_end_node_key = "id"

                # Deduplicate relationships
                unique_label_rels = { (rel['start_value'], rel['end_value']) for rel in label_relationships }
                final_label_relationships = [ {'start_value': start, 'end_value': end} for start, end in unique_label_rels ]
                logger.info(f"Creating {len(final_label_relationships)} unique relationships for {label_rel_type}")

                for i in range(0, len(final_label_relationships), self.BATCH_SIZE):
                    batch_label_rels = final_label_relationships[i:i + self.BATCH_SIZE]
                    try:
                        session.execute_write(
                            self.create_relationships_batch,
                            label_start_node_type,
                            label_end_node_type,
                            label_rel_type,
                            batch_label_rels,
                            start_key=label_start_node_key,
                            end_key=label_end_node_key
                        )
                        logger.info(f"Processed relationship batch {i // self.BATCH_SIZE + 1} for {label_rel_type} ({len(batch_label_rels)} rels)")
                    except Exception as e:
                        logger.error(f"Error writing relationship batch for {label_rel_type}: {e}")
                        logger.error(f"First few rels in failing batch: {batch_label_rels[:3]}")

        logger.info(f"Finished transforming {entity_type} records.")

    def transform_adset(self, ads_df: pd.DataFrame, ads_insights_df: pd.DataFrame):
        """Transform AdSet data (derived from ads table) and link to Campaign."""
        # We derive AdSet info primarily from the ads table as requested.
        # ads_insights_df is passed but not used for properties here.
        if ads_df.empty:
             logger.info("Ads dataframe is empty. Cannot derive AdSets. Skipping transformation.")
             return
         
        entity_type = "FbAdSet"
        id_property = "id" # AdSet ID
        logger.info(f"Deriving and transforming {entity_type} records from ads table...")

        # --- Extract Unique AdSets --- 
        # Essential columns: adset_id, campaign_id
        required_cols = ['adset_id', 'campaign_id']
        # Optional: Check if 'adset_name' exists in the ads_df
        if 'adset_name' in ads_df.columns:
            required_cols.append('adset_name')
        else:
             # Attempt to get name from insights df as a fallback (less ideal)
             if not ads_insights_df.empty and 'adset_id' in ads_insights_df.columns and 'adset_name' in ads_insights_df.columns:
                 logger.info("'adset_name' not found in ads_df, attempting fallback lookup from ads_insights_df.")
                 name_map = ads_insights_df.drop_duplicates(subset=['adset_id'])[[ 'adset_id', 'adset_name' ]].set_index('adset_id')['adset_name']
                 ads_df['adset_name'] = ads_df['adset_id'].map(name_map).fillna('Unknown AdSet')
                 required_cols.append('adset_name')
             else:
                 logger.warning("'adset_name' column not found in ads_df or ads_insights_df. AdSet nodes will lack names.")

        # Check if essential columns exist
        if 'adset_id' not in ads_df.columns or 'campaign_id' not in ads_df.columns:
            logger.error("Critical Error: 'adset_id' or 'campaign_id' missing in ads_df. Cannot derive AdSets.")
            return
        
        # Select relevant columns and drop duplicates based on adset_id
        adset_data = ads_df[required_cols].copy()
        adset_data = adset_data.drop_duplicates(subset=['adset_id'])
        adset_data = adset_data.dropna(subset=['adset_id', 'campaign_id']) # Ensure key fields are not null

        if adset_data.empty:
            logger.info("No valid unique AdSet records found in ads_df after cleaning.")
            return
            
        logger.info(f"Found {len(adset_data)} unique AdSet records to process.")

        # --- Prepare Nodes and Relationships --- 
        nodes = []
        relationships = []
        for _, row in adset_data.iterrows():
            adset_id = str(row['adset_id']) # Ensure string
            campaign_id = str(row['campaign_id']) # Ensure string

            node = {
                id_property: adset_id,
                # Include name if available
                'name': str(row['adset_name']) if 'adset_name' in row and pd.notna(row['adset_name']) else f"AdSet {adset_id}" # Fallback name
                # Add other non-metric AdSet properties here if they become available in ads_df
                # e.g., 'start_time', 'end_time', 'bid_strategy' if they are denormalized onto ads table
            }
            nodes.append(node)

            relationships.append({
                'start_value': campaign_id, # campaign_id from FbCampaign
                'end_value': adset_id      # id from FbAdSet
            })

        # --- Create Nodes and Relationships in Batches --- 
        with self.driver.session(database="neo4j") as session:
            # Create Nodes
            for i in range(0, len(nodes), self.BATCH_SIZE):
                batch_nodes = nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed node batch {i // self.BATCH_SIZE + 1} for {entity_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                    logger.error(f"Error writing node batch for {entity_type}: {e}")
                    # Decide behavior

            # Create Relationships (Campaign -> AdSet)
            rel_type = "HAS_ADSET"
            start_node_type = "FbCampaign"
            start_node_key = "id"
            end_node_type = entity_type # "FbAdSet"
            end_node_key = id_property # "id"

            for i in range(0, len(relationships), self.BATCH_SIZE):
                batch_rels = relationships[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_relationships_batch,
                        start_node_type,
                        end_node_type,
                        rel_type,
                        batch_rels,
                        start_key=start_node_key,
                        end_key=end_node_key
                    )
                    logger.info(f"Processed relationship batch {i // self.BATCH_SIZE + 1} for {rel_type} ({len(batch_rels)} rels)")
                except Exception as e:
                    logger.error(f"Error writing relationship batch for {rel_type}: {e}")
                    # Decide behavior

        logger.info(f"Finished transforming {entity_type} records.")

    def transform_ad(self, ads_df: pd.DataFrame):
        """Transform Ad data and link to AdSet. Returns the processed DataFrame with creative_id."""
        if ads_df.empty:
            logger.info("Ads dataframe is empty. Skipping transformation.")
            return ads_df # Return the empty dataframe
        
        entity_type = "FbAd"
        id_property = "id" # Ad ID from the ads schema
        logger.info(f"Transforming {len(ads_df)} {entity_type} records...")

        # Select and rename columns
        required_cols = {
            'id': 'id', # Ad ID
            'adset_id': 'adset_id', # Needed for relationship to FbAdSet
            'campaign_id': 'campaign_id', # Useful for context, though relationship is via AdSet
            'account_id': 'account_id', # Useful for context
            'name': 'name',
            'status': 'status',
            'effective_status': 'effective_status',
            'created_time': 'created_time',
            'updated_time': 'updated_time',
            'adlabels': 'adlabels', # Complex, needs serialization
            'bid_info': 'bid_info', # Complex, needs serialization
            'bid_type': 'bid_type',
            'bid_amount': 'bid_amount',
            'creative': 'creative', # Complex, contains creative_id
            'targeting': 'targeting', # Complex, needs serialization
            'tracking_specs': 'tracking_specs', # Complex, needs serialization
            'conversion_specs': 'conversion_specs', # Complex, needs serialization
            'recommendations': 'recommendations', # Complex, needs serialization
            'source_ad_id': 'source_ad_id'
        }
        
        # Filter DataFrame
        cols_to_select = {df_col: node_prop for df_col, node_prop in required_cols.items() if df_col in ads_df.columns}
        
        # Check for essential columns
        if id_property not in cols_to_select:
            logger.error(f"Critical Error: The ID property '{id_property}' is not present in the ads_df columns. Cannot proceed with {entity_type} transformation.")
            return ads_df # Return the empty dataframe
        if 'adset_id' not in cols_to_select:
             logger.error(f"Critical Error: The 'adset_id' property is not present in the ads_df columns. Cannot create relationship for {entity_type}.")
             return ads_df # Return the empty dataframe
        
        processed_df = ads_df[list(cols_to_select.keys())].copy()
        processed_df.rename(columns=cols_to_select, inplace=True)
        
        # Extract creative_id from the 'creative' column/object
        if 'creative' in processed_df.columns:
            def extract_creative_id(creative_data):
                if isinstance(creative_data, dict) and 'creative_id' in creative_data:
                    return creative_data['creative_id']
                elif isinstance(creative_data, str): # Handle if it's already a string ID or JSON string
                    try:
                        data = json.loads(creative_data)
                        if isinstance(data, dict) and 'creative_id' in data:
                            return data['creative_id']
                    except json.JSONDecodeError:
                        # If it's not JSON but a plain string, assume it might be the ID itself
                        return creative_data 
                return None # Or some default placeholder
            processed_df['creative_id'] = processed_df['creative'].apply(extract_creative_id)
            # Convert original creative column to JSON string for storage
            if processed_df['creative'].apply(lambda x: isinstance(x, (list, dict))).any():
                processed_df['creative'] = processed_df['creative'].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        else:
             processed_df['creative_id'] = None
             logger.warning("'creative' column not found in ads_df. FbAd nodes will lack 'creative_id'.")
        
        # Convert other complex types to JSON strings
        for col in ['adlabels', 'bid_info', 'targeting', 'tracking_specs', 'conversion_specs', 'recommendations']:
            if col in processed_df.columns:
                 if processed_df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                     processed_df[col] = processed_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        # Convert numeric fields
        if 'bid_amount' in processed_df.columns:
            processed_df['bid_amount'] = pd.to_numeric(processed_df['bid_amount'], errors='coerce') # Keep NaN for missing bids?

        # Convert to list of dictionaries, handle NaN/NaT
        # Fill NaN with empty string for most, but maybe None for bid_amount?
        processed_df = processed_df.fillna('') # Adjust fill value if needed
        nodes = processed_df.to_dict('records')

        # Clean nodes and prepare relationships
        cleaned_nodes = []
        relationships = []
        for node in nodes:
             cleaned_node = {}
             adset_id_val = None
             ad_id_val = None

             for key, value in node.items():
                 if key == 'adset_id':
                      adset_id_val = str(value) if pd.notna(value) and value != '' else None
                      continue # Don't store adset_id directly on FbAd node, use relationship
                 elif key == id_property:
                      ad_id_val = str(value) if pd.notna(value) and value != '' else None
                      cleaned_node[key] = ad_id_val
                 elif pd.isna(value) or value == '':
                     cleaned_node[key] = ''
                 elif isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
                      cleaned_node[key] = str(value) if pd.notna(value) else ''
                 # Handle creative_id explicitly if extracted
                 elif key == 'creative_id':
                      cleaned_node[key] = str(value) if pd.notna(value) else ''
                 elif isinstance(value, (int, float, bool, str)):
                     cleaned_node[key] = value
                 else:
                     try:
                          cleaned_node[key] = str(value)
                     except Exception as e:
                         logger.error(f"Could not convert value for key '{key}' in {entity_type}: {e}. Value: {value}.")
                         cleaned_node[key] = ''
             
             # Check IDs for node and relationship
             if not ad_id_val:
                 logger.warning(f"Skipping ad node due to missing or empty ID ('{id_property}'): {node}")
                 continue
             if not adset_id_val:
                  logger.warning(f"Skipping relationship for ad '{ad_id_val}' due to missing adset_id.")
             else:
                 relationships.append({
                     'start_value': adset_id_val, # id from FbAdSet
                     'end_value': ad_id_val     # id from FbAd
                 })
             
             cleaned_nodes.append(cleaned_node)

        # Create nodes and relationships in batches
        with self.driver.session(database="neo4j") as session:
            # Create Nodes
            for i in range(0, len(cleaned_nodes), self.BATCH_SIZE):
                batch_nodes = cleaned_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed node batch {i // self.BATCH_SIZE + 1} for {entity_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                    logger.error(f"Error writing node batch for {entity_type}: {e}")

            # Create Relationships (AdSet -> Ad)
            rel_type = "CONTAINS_AD"
            start_node_type = "FbAdSet"
            start_node_key = "id"
            end_node_type = entity_type # "FbAd"
            end_node_key = id_property # "id"
            
            for i in range(0, len(relationships), self.BATCH_SIZE):
                batch_rels = relationships[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_relationships_batch,
                        start_node_type,
                        end_node_type,
                        rel_type,
                        batch_rels,
                        start_key=start_node_key,
                        end_key=end_node_key
                    )
                    logger.info(f"Processed relationship batch {i // self.BATCH_SIZE + 1} for {rel_type} ({len(batch_rels)} rels)")
                except Exception as e:
                     logger.error(f"Error writing relationship batch for {rel_type}: {e}")

        logger.info(f"Finished transforming {entity_type} records.")
        return processed_df # Return the dataframe with creative_id

    def transform_adcreative(self, adcreative_df: pd.DataFrame, ads_df: pd.DataFrame):
        """Transform AdCreative data and link to Ad"""
        if adcreative_df.empty:
            logger.info("AdCreative dataframe is empty. Skipping transformation.")
            return
        # We need ads_df to map creative_id back to ad_id for the relationship
        if ads_df.empty:
            logger.warning("Ads dataframe is empty. Cannot create relationships for AdCreatives. Skipping relationships.")
            # Proceed with node creation, but skip relationships
            create_relationships = False
        else:
            create_relationships = True
            # Create a mapping from creative_id to ad_id(s)
            # An ad creative might be used by multiple ads
            ad_creative_mapping = ads_df[ads_df['creative_id'].notna()][['id', 'creative_id']].copy()
            # Ensure IDs are strings
            ad_creative_mapping['id'] = ad_creative_mapping['id'].astype(str)
            ad_creative_mapping['creative_id'] = ad_creative_mapping['creative_id'].astype(str)
            ad_creative_mapping = ad_creative_mapping.groupby('creative_id')['id'].apply(list).to_dict()

        entity_type = "FbAdCreative"
        id_property = "id" # Creative ID
        logger.info(f"Transforming {len(adcreative_df)} {entity_type} records...")

        # Select and rename columns
        required_cols = {
            'id': 'id',
            'name': 'name',
            'account_id': 'account_id', # Context
            'status': 'status',
            'body': 'body',
            'title': 'title',
            'link_url': 'link_url',
            'video_id': 'video_id',
            'image_url': 'image_url',
            'image_hash': 'image_hash',
            'thumbnail_url': 'thumbnail_url',
            'object_id': 'object_id',
            'object_type': 'object_type',
            'object_url': 'object_url',
            'object_story_id': 'object_story_id',
            'instagram_actor_id': 'instagram_actor_id',
            'instagram_story_id': 'instagram_story_id',
            'call_to_action_type': 'call_to_action_type',
            'adlabels': 'adlabels',
            'asset_feed_spec': 'asset_feed_spec', # Complex
            'object_story_spec': 'object_story_spec' # Complex
            # Add others like url_tags, image_crops, template_url_spec etc. if needed
        }

        # Filter DataFrame
        cols_to_select = {df_col: node_prop for df_col, node_prop in required_cols.items() if df_col in adcreative_df.columns}
        if id_property not in cols_to_select:
            logger.error(f"Critical Error: The ID property '{id_property}' is not present in adcreative_df. Cannot transform {entity_type}.")
            return

        processed_df = adcreative_df[list(cols_to_select.keys())].copy()
        processed_df.rename(columns=cols_to_select, inplace=True)

        # Convert complex types to JSON strings
        for col in ['adlabels', 'asset_feed_spec', 'object_story_spec']: # Add others as needed
            if col in processed_df.columns:
                if processed_df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    processed_df[col] = processed_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        # Convert to list of dictionaries, handle NaN/NaT
        processed_df = processed_df.fillna('')
        nodes = processed_df.to_dict('records')

        # Clean nodes and prepare relationships
        cleaned_nodes = []
        relationships = []
        for node in nodes:
            cleaned_node = {}
            creative_id_val = None

            for key, value in node.items():
                if key == id_property:
                    creative_id_val = str(value) if pd.notna(value) and value != '' else None
                    cleaned_node[key] = creative_id_val
                elif pd.isna(value) or value == '':
                    cleaned_node[key] = ''
                elif isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
                    cleaned_node[key] = str(value) if pd.notna(value) else ''
                elif isinstance(value, (int, float, bool, str)):
                    cleaned_node[key] = value
                else:
                    try:
                        cleaned_node[key] = str(value)
                    except Exception as e:
                        logger.error(f"Could not convert value for key '{key}' in {entity_type}: {e}. Value: {value}.")
                        cleaned_node[key] = ''
            
            # Check ID for node creation
            if not creative_id_val:
                logger.warning(f"Skipping creative node due to missing or empty ID ('{id_property}'): {node}")
                continue
            cleaned_nodes.append(cleaned_node)

            # Prepare relationships if possible
            if create_relationships and creative_id_val in ad_creative_mapping:
                ad_ids = ad_creative_mapping[creative_id_val]
                for ad_id in ad_ids:
                    relationships.append({
                        'start_value': ad_id,          # id from FbAd
                        'end_value': creative_id_val # id from FbAdCreative
                    })

        # Create nodes and relationships in batches
        with self.driver.session(database="neo4j") as session:
            # Create Nodes
            for i in range(0, len(cleaned_nodes), self.BATCH_SIZE):
                batch_nodes = cleaned_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed node batch {i // self.BATCH_SIZE + 1} for {entity_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                    logger.error(f"Error writing node batch for {entity_type}: {e}")

            # Create Relationships (Ad -> Creative)
            if create_relationships:
                rel_type = "HAS_CREATIVE"
                start_node_type = "FbAd"
                start_node_key = "creative_id" # Match the property on FbAd node
                end_node_type = entity_type    # "FbAdCreative"
                end_node_key = id_property    # "id"
                
                # We need a custom query here because the start node key ('creative_id') 
                # does not match the start node label's primary key ('id')
                # Also, one creative can link to multiple Ads. 
                # It might be better to iterate through the mapping?
                # Let's refine the relationship creation

                logger.info(f"Preparing {len(relationships)} relationships for {rel_type}")
                
                # Use a dictionary to store unique relationships to avoid duplicates if one ad uses the same creative multiple times (unlikely but possible)
                unique_rels = { (rel['start_value'], rel['end_value']) for rel in relationships }
                final_relationships = [ {'start_value': start, 'end_value': end} for start, end in unique_rels ]
                
                logger.info(f"Creating {len(final_relationships)} unique relationships for {rel_type}")

                for i in range(0, len(final_relationships), self.BATCH_SIZE):
                    batch_rels = final_relationships[i:i + self.BATCH_SIZE]
                    try:
                        # We link from Ad (start) to Creative (end)
                        session.execute_write(
                            self.create_relationships_batch,
                            "FbAd",          # Start Node Type
                            entity_type,     # End Node Type
                            rel_type,        # Relationship Type
                            batch_rels,      # Batch data
                            start_key="id",  # Match FbAd by its actual ID
                            end_key="id"     # Match FbAdCreative by its ID
                        )
                        logger.info(f"Processed relationship batch {i // self.BATCH_SIZE + 1} for {rel_type} ({len(batch_rels)} rels)")
                    except Exception as e:
                        logger.error(f"Error writing relationship batch for {rel_type}: {e}")
                        # Log first few rels in failing batch for debugging
                        logger.error(f"First few rels in failing batch: {batch_rels[:3]}")

        logger.info(f"Finished transforming {entity_type} records.")

    def transform_image(self, image_df: pd.DataFrame, adcreative_df: pd.DataFrame):
        """Transform Image data and link to AdCreative"""
        if image_df.empty:
            logger.info("Image dataframe is empty. Skipping transformation.")
            return
        # adcreative_df is passed in case it's needed for context, but relationships rely on 'creatives' column in image_df

        entity_type = "FbImage"
        id_property = "id" # Image ID
        logger.info(f"Transforming {len(image_df)} {entity_type} records...")

        # Select and rename columns
        required_cols = {
            'id': 'id',
            'name': 'name',
            'account_id': 'account_id', # Context
            'status': 'status',
            'hash': 'hash',
            'url': 'url',
            'url_128': 'url_128',
            'permalink_url': 'permalink_url',
            'width': 'width',
            'height': 'height',
            'original_width': 'original_width',
            'original_height': 'original_height',
            'filename': 'filename',
            'created_time': 'created_time',
            'updated_time': 'updated_time',
            'creatives': 'creatives' # Crucial for relationship
        }

        # Filter DataFrame
        cols_to_select = {df_col: node_prop for df_col, node_prop in required_cols.items() if df_col in image_df.columns}
        if id_property not in cols_to_select:
            logger.error(f"Critical Error: The ID property '{id_property}' is not present in image_df. Cannot transform {entity_type}.")
            return
        if 'creatives' not in cols_to_select:
             logger.warning(f"'creatives' column not found in image_df. Cannot create relationships for {entity_type}.")
             create_relationships = False
        else:
            create_relationships = True

        processed_df = image_df[list(cols_to_select.keys())].copy()
        processed_df.rename(columns=cols_to_select, inplace=True)

        # Convert numeric fields
        for col in ['width', 'height', 'original_width', 'original_height']:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0).astype(int)

        # Convert to list of dictionaries, handle NaN/NaT
        # Keep 'creatives' column for relationship processing
        processed_df_no_creatives = processed_df.drop(columns=['creatives'], errors='ignore').fillna('')
        nodes_data = processed_df_no_creatives.to_dict('records')
        
        # Keep the original df slice with creatives for relationship mapping
        rel_mapping_df = processed_df[[id_property, 'creatives']].copy()

        # Clean nodes
        cleaned_nodes = []
        for node in nodes_data:
            cleaned_node = {}
            image_id_val = None
            for key, value in node.items():
                if key == id_property:
                    image_id_val = str(value) if pd.notna(value) and value != '' else None
                    cleaned_node[key] = image_id_val
                elif pd.isna(value) or value == '':
                    cleaned_node[key] = ''
                elif isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
                    cleaned_node[key] = str(value) if pd.notna(value) else ''
                elif isinstance(value, (int, float, bool, str)):
                    cleaned_node[key] = value
                else:
                    try:
                        cleaned_node[key] = str(value)
                    except Exception as e:
                        logger.error(f"Could not convert value for key '{key}' in {entity_type}: {e}. Value: {value}.")
                        cleaned_node[key] = ''
            
            if not image_id_val:
                logger.warning(f"Skipping image node due to missing or empty ID ('{id_property}'): {node}")
                continue
            cleaned_nodes.append(cleaned_node)

        # Prepare relationships
        relationships = []
        if create_relationships:
            logger.info("Parsing 'creatives' column for relationships...")
            rel_mapping_df = rel_mapping_df.dropna(subset=[id_property, 'creatives']) # Drop rows where essential info is missing
            for _, row in rel_mapping_df.iterrows():
                image_id = str(row[id_property])
                creatives_data = row['creatives']
                creative_ids = []

                # Attempt to parse the creatives data (might be JSON string, list, etc.)
                if isinstance(creatives_data, str):
                    try:
                        parsed_data = json.loads(creatives_data)
                        if isinstance(parsed_data, list):
                            # Assuming list of strings or dicts with id
                            for item in parsed_data:
                                if isinstance(item, str):
                                    creative_ids.append(item)
                                elif isinstance(item, dict) and 'id' in item:
                                    creative_ids.append(str(item['id']))
                        # Add more parsing logic if the structure is different
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse 'creatives' JSON string for image {image_id}: {creatives_data}")
                elif isinstance(creatives_data, list):
                    for item in creatives_data:
                        if isinstance(item, str):
                            creative_ids.append(item)
                        elif isinstance(item, dict) and 'id' in item:
                            creative_ids.append(str(item['id']))
                # Add other potential type handling if needed
                else:
                     logger.warning(f"Unexpected type for 'creatives' column for image {image_id}: {type(creatives_data)}. Skipping relationships for this image.")

                # Create relationship dicts
                for creative_id in creative_ids:
                    if creative_id: # Ensure creative_id is not empty
                        relationships.append({
                            'start_value': str(creative_id), # id from FbAdCreative
                            'end_value': image_id         # id from FbImage
                        })

        # Create nodes and relationships in batches
        with self.driver.session(database="neo4j") as session:
            # Create Nodes
            for i in range(0, len(cleaned_nodes), self.BATCH_SIZE):
                batch_nodes = cleaned_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed node batch {i // self.BATCH_SIZE + 1} for {entity_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                    logger.error(f"Error writing node batch for {entity_type}: {e}")

            # Create Relationships (Creative -> Image)
            if create_relationships and relationships:
                rel_type = "USES_IMAGE"
                start_node_type = "FbAdCreative"
                start_node_key = "id"
                end_node_type = entity_type # "FbImage"
                end_node_key = id_property # "id"

                # Deduplicate relationships before batching
                unique_rels = { (rel['start_value'], rel['end_value']) for rel in relationships }
                final_relationships = [ {'start_value': start, 'end_value': end} for start, end in unique_rels ]
                logger.info(f"Creating {len(final_relationships)} unique relationships for {rel_type}")

                for i in range(0, len(final_relationships), self.BATCH_SIZE):
                    batch_rels = final_relationships[i:i + self.BATCH_SIZE]
                    try:
                        session.execute_write(
                            self.create_relationships_batch,
                            start_node_type,
                            end_node_type,
                            rel_type,
                            batch_rels,
                            start_key=start_node_key,
                            end_key=end_node_key
                        )
                        logger.info(f"Processed relationship batch {i // self.BATCH_SIZE + 1} for {rel_type} ({len(batch_rels)} rels)")
                    except Exception as e:
                        logger.error(f"Error writing relationship batch for {rel_type}: {e}")
                        logger.error(f"First few rels in failing batch: {batch_rels[:3]}")

        logger.info(f"Finished transforming {entity_type} records.")

    def _generate_insight_id(self, entity_id: str, period_start: str, granularity: str, 
                            insight_type: str, entity_type: str = "ad", breakdown_data: Dict[str, str] = None) -> str:
        """
        Generate a deterministic unique ID for insight nodes that combines:
        - entity_id: The ID of the ad or campaign this insight belongs to
        - period_start: YYYY-MM-DD date string representing the start of the period
        - granularity: 'daily', 'weekly', or 'monthly'
        - insight_type: Type of insight (e.g., 'basic', 'age_gender')
        - entity_type: Type of entity ('ad' or 'campaign')
        - breakdown_data: Optional dict of dimension values (e.g., {'age': '25-34', 'gender': 'female'})
        
        Returns a string like: "ad_123456_daily_2023-01-15_basic" or "campaign_123456_daily_2023-01-15_age_gender_age=25-34_gender=female"
        """
        # Start with the base components
        id_parts = [f"{entity_type}_{entity_id}", granularity, period_start, insight_type]
        
        # Add breakdown values if present, ensuring they're in a consistent order
        if breakdown_data:
            # Sort keys to ensure consistent order
            sorted_keys = sorted(breakdown_data.keys())
            for key in sorted_keys:
                if pd.notna(breakdown_data[key]) and breakdown_data[key]:
                    # Clean the value and make it safe for an ID
                    value = str(breakdown_data[key]).strip()
                    id_parts.append(f"{key}={value}")
        
        # Join with underscores
        return "_".join(id_parts)

    def transform_ads_weekly_insight(self, insight_df: pd.DataFrame, insight_type: str = "basic"):
        """
        Transform weekly aggregated Insight data and link to Ad.
        
        Args:
            insight_df: DataFrame containing insight data
            insight_type: Type of insight data ('basic', 'age_gender', etc.)
        """
        if insight_df.empty:
            logger.info(f"Weekly Insight dataframe ({insight_type}) is empty. Skipping transformation.")
            return
        
        entity_type = "FbWeeklyInsight"
        id_property = "insight_id"  # Generated composite key
        logger.info(f"Transforming {len(insight_df)} {insight_type} records into weekly aggregated insights...")
        
        # Define breakdown columns based on insight_type
        breakdown_cols = []
        if insight_type == "age_gender":
            # Include both age and gender for the age_gender breakdown
            breakdown_cols = ['age', 'gender']
        # Add cases for other insight types as needed
        
        # Define metrics to aggregate (sum for most, possibly compute averages for others)
        sum_metric_cols = [
            'spend', 'impressions', 'clicks', 'reach', 'social_spend'
        ]
        
        # Validate required columns exist
        required_cols = ['ad_id', 'date_start']
        for col in required_cols:
            if col not in insight_df.columns:
                logger.error(f"Required column '{col}' missing from insight dataframe. Cannot process {insight_type} weekly insights.")
                return
        
        # For breakdowns, ensure the breakdown columns exist
        if breakdown_cols:
            missing_breakdowns = [col for col in breakdown_cols if col not in insight_df.columns]
            if missing_breakdowns:
                logger.error(f"Breakdown columns {missing_breakdowns} missing from {insight_type} dataframe. Cannot process weekly insights.")
                return
        
        # Filter out metrics that are not present in the dataframe
        available_metrics = [col for col in sum_metric_cols if col in insight_df.columns]
        if not available_metrics:
            logger.warning(f"No metrics found for {insight_type} weekly insights. Will only create structural nodes.")
        
        # Copy only needed columns and prepare DataFrame
        selected_cols = ['ad_id', 'date_start'] + breakdown_cols + available_metrics
        processed_df = insight_df[selected_cols].copy()
        
        # Ensure ID and breakdown fields are strings, metrics are numeric
        processed_df['ad_id'] = processed_df['ad_id'].astype(str)
        for col in breakdown_cols:
            processed_df[col] = processed_df[col].astype(str)
        
        # Convert date_start to datetime for aggregation
        try:
            processed_df['date_start'] = pd.to_datetime(processed_df['date_start'])
        except Exception as e:
            logger.error(f"Error converting date_start to datetime for weekly aggregation: {e}")
            return
        
        # Calculate week start date (Monday-based)
        # Pandas dayofweek is Monday=0, Sunday=6, so we can directly use start_time - days to get to Monday
        processed_df['week_day'] = processed_df['date_start'].dt.dayofweek
        processed_df['week_start_date'] = processed_df['date_start'] - pd.to_timedelta(processed_df['week_day'], unit='d')
        processed_df['week_start_date'] = processed_df['week_start_date'].dt.strftime('%Y-%m-%d')
        
        # Convert metrics to numeric for aggregation
        for col in available_metrics:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        
        # Group by ad_id, week_start_date, and any breakdown columns
        group_cols = ['ad_id', 'week_start_date'] + breakdown_cols
        logger.info(f"Aggregating {insight_type} insights by {group_cols}...")
        
        # Aggregate data
        try:
            # Sum metrics
            agg_dict = {metric: 'sum' for metric in available_metrics}
            weekly_df = processed_df.groupby(group_cols).agg(agg_dict).reset_index()
            
            # Add derived metrics after aggregation
            if 'clicks' in weekly_df.columns and 'impressions' in weekly_df.columns:
                # Impressions can be 0, safely handle division
                weekly_df['ctr'] = weekly_df.apply(
                    lambda x: (x['clicks'] / x['impressions'] * 100) if x['impressions'] > 0 else 0, 
                    axis=1
                )
            
            if 'spend' in weekly_df.columns and 'clicks' in weekly_df.columns:
                # Clicks can be 0, safely handle division
                weekly_df['cpc'] = weekly_df.apply(
                    lambda x: (x['spend'] / x['clicks']) if x['clicks'] > 0 else 0, 
                    axis=1
                )
            
            if 'spend' in weekly_df.columns and 'impressions' in weekly_df.columns:
                # Calculate CPM (cost per 1000 impressions)
                weekly_df['cpm'] = weekly_df.apply(
                    lambda x: (x['spend'] / x['impressions'] * 1000) if x['impressions'] > 0 else 0, 
                    axis=1
                )
            
            logger.info(f"Successfully aggregated into {len(weekly_df)} weekly records for {insight_type}.")
        except Exception as e:
            logger.error(f"Error during weekly aggregation for {insight_type}: {e}")
            return
        
        # Generate insight_id for each aggregated row
        def generate_weekly_insight_id(row):
            # Create breakdown dict for the row if breakdown columns are present
            breakdown_data = None
            if breakdown_cols:
                breakdown_data = {col: row[col] for col in breakdown_cols}
            
            return self._generate_insight_id(
                entity_id=row['ad_id'],
                period_start=row['week_start_date'],
                granularity='weekly',
                insight_type=insight_type,
                entity_type='ad',
                breakdown_data=breakdown_data
            )
        
        weekly_df['insight_id'] = weekly_df.apply(generate_weekly_insight_id, axis=1)
        
        # Create node data dictionaries and relationship data
        weekly_insight_nodes = []
        relationships = []
        
        # Convert the DataFrame to records and process each aggregated row
        weekly_records = weekly_df.fillna('').to_dict('records')
        
        for row in weekly_records:
            insight_node = {
                'insight_id': row['insight_id'],
                'ad_id': row['ad_id'],
                'insight_type': insight_type,
                'granularity': 'weekly',
                'period_start': row['week_start_date']
            }
            
            # Add breakdown information if applicable
            for col in breakdown_cols:
                if col in row and pd.notna(row[col]) and row[col] != '':
                    insight_node[col] = str(row[col])
            
            # Add aggregated metrics
            for col in weekly_df.columns:
                if col not in group_cols + ['insight_id'] and pd.notna(row[col]):
                    if isinstance(row[col], (int, float)):
                        insight_node[col] = row[col]
                    else:
                        insight_node[col] = str(row[col])
            
            weekly_insight_nodes.append(insight_node)
            
            # Create relationship between Ad and Weekly Insight
            relationships.append({
                'start_value': row['ad_id'],      # Ad ID
                'end_value': row['insight_id']    # Insight ID
            })
        
        # Create nodes and relationships in batches
        with self.driver.session(database="neo4j") as session:
            # Create Weekly Insight Nodes
            logger.info(f"Creating {len(weekly_insight_nodes)} {entity_type} nodes for {insight_type}...")
            for i in range(0, len(weekly_insight_nodes), self.BATCH_SIZE):
                batch_nodes = weekly_insight_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed {entity_type} node batch {i // self.BATCH_SIZE + 1} for {insight_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                    logger.error(f"Error writing {entity_type} node batch for {insight_type}: {e}")
            
            # Create Relationships (Ad -> WeeklyInsight)
            rel_type = "HAS_WEEKLY_INSIGHT"
            start_node_type = "FbAd"
            start_node_key = "id"
            end_node_type = entity_type # "FbWeeklyInsight"
            end_node_key = id_property # "insight_id"
            
            logger.info(f"Creating {len(relationships)} {rel_type} relationships for {insight_type}...")
            
            # Deduplicate relationships before batching
            unique_rels = {(rel['start_value'], rel['end_value']) for rel in relationships}
            final_relationships = [{'start_value': start, 'end_value': end} for start, end in unique_rels]
            
            for i in range(0, len(final_relationships), self.BATCH_SIZE):
                batch_rels = final_relationships[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_relationships_batch,
                        start_node_type,
                        end_node_type,
                        rel_type,
                        batch_rels,
                        start_key=start_node_key,
                        end_key=end_node_key
                    )
                    logger.info(f"Processed {rel_type} relationship batch {i // self.BATCH_SIZE + 1} for {insight_type} ({len(batch_rels)} rels)")
                except Exception as e:
                    logger.error(f"Error writing {rel_type} relationship batch for {insight_type}: {e}")
        
        logger.info(f"Finished transforming {len(weekly_insight_nodes)} {entity_type} records for {insight_type}.")

    def transform_ads_monthly_insight(self, insight_df: pd.DataFrame, insight_type: str = "basic"):
        """
        Transform monthly aggregated Insight data and link to Ad.
        
        Args:
            insight_df: DataFrame containing insight data
            insight_type: Type of insight data ('basic', 'age_gender', etc.)
        """
        if insight_df.empty:
            logger.info(f"Monthly Insight dataframe ({insight_type}) is empty. Skipping transformation.")
            return
        
        entity_type = "FbMonthlyInsight"
        id_property = "insight_id"  # Generated composite key
        logger.info(f"Transforming {len(insight_df)} {insight_type} records into monthly aggregated insights...")
        
        # Define breakdown columns based on insight_type
        breakdown_cols = []
        if insight_type == "age_gender":
            # Include both age and gender for the age_gender breakdown
            breakdown_cols = ['age', 'gender']
        # Add cases for other insight types as needed
        
        # Define metrics to aggregate (sum for most, possibly compute averages for others)
        sum_metric_cols = [
            'spend', 'impressions', 'clicks', 'reach', 'social_spend'
        ]
        
        # Validate required columns exist
        required_cols = ['ad_id', 'date_start']
        for col in required_cols:
            if col not in insight_df.columns:
                logger.error(f"Required column '{col}' missing from insight dataframe. Cannot process {insight_type} monthly insights.")
                return
        
        # For breakdowns, ensure the breakdown columns exist
        if breakdown_cols:
            missing_breakdowns = [col for col in breakdown_cols if col not in insight_df.columns]
            if missing_breakdowns:
                logger.error(f"Breakdown columns {missing_breakdowns} missing from {insight_type} dataframe. Cannot process monthly insights.")
                return
        
        # Filter out metrics that are not present in the dataframe
        available_metrics = [col for col in sum_metric_cols if col in insight_df.columns]
        if not available_metrics:
            logger.warning(f"No metrics found for {insight_type} monthly insights. Will only create structural nodes.")
        
        # Copy only needed columns and prepare DataFrame
        selected_cols = ['ad_id', 'date_start'] + breakdown_cols + available_metrics
        processed_df = insight_df[selected_cols].copy()
        
        # Ensure ID and breakdown fields are strings, metrics are numeric
        processed_df['ad_id'] = processed_df['ad_id'].astype(str)
        for col in breakdown_cols:
            processed_df[col] = processed_df[col].astype(str)
        
        # Convert date_start to datetime for aggregation
        try:
            processed_df['date_start'] = pd.to_datetime(processed_df['date_start'])
        except Exception as e:
            logger.error(f"Error converting date_start to datetime for monthly aggregation: {e}")
            return
        
        # Calculate month start date (first day of the month)
        processed_df['month_start_date'] = processed_df['date_start'].dt.strftime('%Y-%m-01')
        
        # Convert metrics to numeric for aggregation
        for col in available_metrics:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        
        # --- Debugging Check: Log rows with clicks > impressions before aggregation ---
        invalid_ctr_rows = processed_df[processed_df['clicks'] > processed_df['impressions']]
        if not invalid_ctr_rows.empty:
            logger.warning(f"Found {len(invalid_ctr_rows)} rows in input data for monthly AD insights ({insight_type}) where clicks > impressions BEFORE aggregation. Example rows:")
            logger.warning(invalid_ctr_rows.head().to_string())
        # --- End Debugging Check ---

        # Group by ad_id, month_start_date, and any breakdown columns
        group_cols = ['ad_id', 'month_start_date'] + breakdown_cols # CORRECT: Group by ad_id
        logger.info(f"Aggregating {insight_type} insights by {group_cols}...")
        
        # Aggregate data
        try:
            # Sum metrics
            agg_dict = {metric: 'sum' for metric in available_metrics}
            monthly_df = processed_df.groupby(group_cols).agg(agg_dict).reset_index()
            
            # Add derived metrics after aggregation
            if 'clicks' in monthly_df.columns and 'impressions' in monthly_df.columns:
                # Impressions can be 0, safely handle division
                monthly_df['ctr'] = monthly_df.apply(
                    lambda x: (x['clicks'] / x['impressions'] * 100) if x['impressions'] > 0 else 0, 
                    axis=1
                )
            
            if 'spend' in monthly_df.columns and 'clicks' in monthly_df.columns:
                # Clicks can be 0, safely handle division
                monthly_df['cpc'] = monthly_df.apply(
                    lambda x: (x['spend'] / x['clicks']) if x['clicks'] > 0 else 0, 
                    axis=1
                )
            
            if 'spend' in monthly_df.columns and 'impressions' in monthly_df.columns:
                # Calculate CPM (cost per 1000 impressions)
                monthly_df['cpm'] = monthly_df.apply(
                    lambda x: (x['spend'] / x['impressions'] * 1000) if x['impressions'] > 0 else 0, 
                    axis=1
                )
            
            logger.info(f"Successfully aggregated into {len(monthly_df)} monthly records for {insight_type}.")
        except Exception as e:
            logger.error(f"Error during monthly aggregation for {insight_type}: {e}")
            return
        
        # Generate insight_id for each aggregated row
        def generate_monthly_insight_id(row):
            # Create breakdown dict for the row if breakdown columns are present
            breakdown_data = None
            if breakdown_cols:
                breakdown_data = {col: row[col] for col in breakdown_cols}
            
            return self._generate_insight_id(
                entity_id=row['ad_id'],
                period_start=row['month_start_date'],
                granularity='monthly',
                insight_type=insight_type,
                entity_type='ad',
                breakdown_data=breakdown_data
            )
        
        monthly_df['insight_id'] = monthly_df.apply(generate_monthly_insight_id, axis=1)
        
        # Create node data dictionaries and relationship data
        monthly_insight_nodes = []
        relationships = []
        
        # Convert the DataFrame to records and process each aggregated row
        monthly_records = monthly_df.fillna('').to_dict('records')
        
        for row in monthly_records:
            insight_node = {
                'insight_id': row['insight_id'],
                'ad_id': row['ad_id'],
                'insight_type': insight_type,
                'granularity': 'monthly',
                'period_start': row['month_start_date']
            }
            
            # Add breakdown information if applicable
            for col in breakdown_cols:
                if col in row and pd.notna(row[col]) and row[col] != '':
                    insight_node[col] = str(row[col])
            
            # Add aggregated metrics
            for col in monthly_df.columns:
                if col not in group_cols + ['insight_id'] and pd.notna(row[col]):
                    if isinstance(row[col], (int, float)):
                        insight_node[col] = row[col]
                    else:
                        insight_node[col] = str(row[col])
            
            monthly_insight_nodes.append(insight_node)
            
            # Create relationship between Ad and Monthly Insight
            relationships.append({
                'start_value': row['ad_id'],      # Ad ID
                'end_value': row['insight_id']    # Insight ID
            })
        
        # Create nodes and relationships in batches
        with self.driver.session(database="neo4j") as session:
            # Create Monthly Insight Nodes
            logger.info(f"Creating {len(monthly_insight_nodes)} {entity_type} nodes for {insight_type}...")
            for i in range(0, len(monthly_insight_nodes), self.BATCH_SIZE):
                batch_nodes = monthly_insight_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed {entity_type} node batch {i // self.BATCH_SIZE + 1} for {insight_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                    logger.error(f"Error writing {entity_type} node batch for {insight_type}: {e}")
            
            # Create Relationships (Ad -> MonthlyInsight)
            rel_type = "HAS_MONTHLY_INSIGHT"
            start_node_type = "FbAd"
            start_node_key = "id"
            end_node_type = entity_type # "FbMonthlyInsight"
            end_node_key = id_property # "insight_id"
            
            logger.info(f"Creating {len(relationships)} {rel_type} relationships for {insight_type}...")
            
            # Deduplicate relationships before batching
            unique_rels = {(rel['start_value'], rel['end_value']) for rel in relationships}
            final_relationships = [{'start_value': start, 'end_value': end} for start, end in unique_rels]
            
            for i in range(0, len(final_relationships), self.BATCH_SIZE):
                batch_rels = final_relationships[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_relationships_batch,
                        start_node_type,
                        end_node_type,
                        rel_type,
                        batch_rels,
                        start_key=start_node_key,
                        end_key=end_node_key
                    )
                    logger.info(f"Processed {rel_type} relationship batch {i // self.BATCH_SIZE + 1} for {insight_type} ({len(batch_rels)} rels)")
                except Exception as e:
                    logger.error(f"Error writing {rel_type} relationship batch for {insight_type}: {e}")
        
        logger.info(f"Finished transforming {len(monthly_insight_nodes)} {entity_type} records for {insight_type}.")

    def transform_insight(self, insight_df: pd.DataFrame, insight_type: str = "basic"):
        """Transform Insight data and link to Ad and Campaign"""
        if insight_df.empty:
             logger.info(f"Insight dataframe ({insight_type}) is empty. Skipping transformation.")
             return
        logger.info(f"Transforming {len(insight_df)} {insight_type} Insight records...")
        
        # Check if we have campaign_id for campaign-level insights
        has_campaign_data = 'campaign_id' in insight_df.columns
        
        # For basic insights and age_gender, create ad-level weekly and monthly insights
        if insight_type in ["basic", "age_gender"]:
            # Process weekly ad-level insights
            self.transform_ads_weekly_insight(insight_df, insight_type)
            # Process monthly ad-level insights
            self.transform_ads_monthly_insight(insight_df, insight_type)
            
            # If we have campaign_id, also create campaign-level insights
            if has_campaign_data:
                logger.info(f"Creating campaign-level aggregated insights for {insight_type}...")
                # Process weekly campaign-level insights
                self.transform_campaign_weekly_insight(insight_df, insight_type)
                # Process monthly campaign-level insights
                self.transform_campaign_monthly_insight(insight_df, insight_type)
            else:
                logger.warning(f"Cannot create campaign-level insights for {insight_type}: 'campaign_id' column missing.")
        
        # For other breakdown types (country, region, device, etc.), add implementation later
        else:
            logger.info(f"Processing for insight type '{insight_type}' not yet implemented.")

    def transform_campaign_weekly_insight(self, insight_df: pd.DataFrame, insight_type: str = "basic"):
        """
        Transform weekly aggregated Campaign Insight data and link to Campaign.
        
        Args:
            insight_df: DataFrame containing insight data
            insight_type: Type of insight data ('basic', 'age_gender', etc.)
        """
        if insight_df.empty:
            logger.info(f"Weekly Campaign Insight dataframe ({insight_type}) is empty. Skipping transformation.")
            return
        
        entity_type = "FbWeeklyCampaignInsight"
        id_property = "insight_id"  # Generated composite key
        logger.info(f"Transforming {len(insight_df)} {insight_type} records into weekly aggregated campaign insights...")
        
        # Define breakdown columns based on insight_type
        breakdown_cols = []
        if insight_type == "age_gender":
            # Include both age and gender for the age_gender breakdown
            breakdown_cols = ['age', 'gender']
        # Add cases for other insight types as needed
        
        # Define metrics to aggregate (sum for most, possibly compute averages for others)
        sum_metric_cols = [
            'spend', 'impressions', 'clicks', 'reach', 'social_spend'
        ]
        
        # Validate required columns exist
        required_cols = ['ad_id', 'campaign_id', 'date_start']
        for col in required_cols:
            if col not in insight_df.columns:
                logger.error(f"Required column '{col}' missing from insight dataframe. Cannot process {insight_type} weekly campaign insights.")
                return
        
        # For breakdowns, ensure the breakdown columns exist
        if breakdown_cols:
            missing_breakdowns = [col for col in breakdown_cols if col not in insight_df.columns]
            if missing_breakdowns:
                logger.error(f"Breakdown columns {missing_breakdowns} missing from {insight_type} dataframe. Cannot process weekly campaign insights.")
                return
        
        # Filter out metrics that are not present in the dataframe
        available_metrics = [col for col in sum_metric_cols if col in insight_df.columns]
        if not available_metrics:
            logger.warning(f"No metrics found for {insight_type} weekly campaign insights. Will only create structural nodes.")
        
        # Copy only needed columns and prepare DataFrame
        selected_cols = ['ad_id', 'campaign_id', 'date_start'] + breakdown_cols + available_metrics
        processed_df = insight_df[selected_cols].copy()
        
        # Ensure ID and breakdown fields are strings, metrics are numeric
        processed_df['ad_id'] = processed_df['ad_id'].astype(str)
        processed_df['campaign_id'] = processed_df['campaign_id'].astype(str)
        for col in breakdown_cols:
            processed_df[col] = processed_df[col].astype(str)
        
        # Convert date_start to datetime for aggregation
        try:
            processed_df['date_start'] = pd.to_datetime(processed_df['date_start'])
        except Exception as e:
            logger.error(f"Error converting date_start to datetime for weekly campaign aggregation: {e}")
            return
        
        # Calculate week start date (Monday-based)
        processed_df['week_day'] = processed_df['date_start'].dt.dayofweek
        processed_df['week_start_date'] = processed_df['date_start'] - pd.to_timedelta(processed_df['week_day'], unit='d')
        processed_df['week_start_date'] = processed_df['week_start_date'].dt.strftime('%Y-%m-%d')
        
        # Convert metrics to numeric for aggregation
        for col in available_metrics:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        
        # Group by campaign_id, week_start_date, and any breakdown columns
        group_cols = ['campaign_id', 'week_start_date'] + breakdown_cols
        logger.info(f"Aggregating {insight_type} insights by {group_cols}...")
        
        # Aggregate data
        try:
            # Sum metrics
            agg_dict = {metric: 'sum' for metric in available_metrics}
            weekly_df = processed_df.groupby(group_cols).agg(agg_dict).reset_index()
            
            # Add derived metrics after aggregation
            if 'clicks' in weekly_df.columns and 'impressions' in weekly_df.columns:
                # Impressions can be 0, safely handle division
                weekly_df['ctr'] = weekly_df.apply(
                    lambda x: (x['clicks'] / x['impressions'] * 100) if x['impressions'] > 0 else 0, 
                    axis=1
                )
            
            if 'spend' in weekly_df.columns and 'clicks' in weekly_df.columns:
                # Clicks can be 0, safely handle division
                weekly_df['cpc'] = weekly_df.apply(
                    lambda x: (x['spend'] / x['clicks']) if x['clicks'] > 0 else 0, 
                    axis=1
                )
            
            if 'spend' in weekly_df.columns and 'impressions' in weekly_df.columns:
                # Calculate CPM (cost per 1000 impressions)
                weekly_df['cpm'] = weekly_df.apply(
                    lambda x: (x['spend'] / x['impressions'] * 1000) if x['impressions'] > 0 else 0, 
                    axis=1
                )
            
            logger.info(f"Successfully aggregated into {len(weekly_df)} weekly campaign records for {insight_type}.")
        except Exception as e:
            logger.error(f"Error during weekly campaign aggregation for {insight_type}: {e}")
            return
        
        # Generate insight_id for each aggregated row
        def generate_weekly_campaign_insight_id(row):
            # Create breakdown dict for the row if breakdown columns are present
            breakdown_data = None
            if breakdown_cols:
                breakdown_data = {col: row[col] for col in breakdown_cols}
            
            return self._generate_insight_id(
                entity_id=row['campaign_id'],
                period_start=row['week_start_date'],
                granularity='weekly',
                insight_type=insight_type,
                entity_type='campaign',
                breakdown_data=breakdown_data
            )
        
        weekly_df['insight_id'] = weekly_df.apply(generate_weekly_campaign_insight_id, axis=1)
        
        # Create node data dictionaries and relationship data
        weekly_insight_nodes = []
        relationships = []
        
        # Convert the DataFrame to records and process each aggregated row
        weekly_records = weekly_df.fillna('').to_dict('records')
        
        for row in weekly_records:
            insight_node = {
                'insight_id': row['insight_id'],
                'campaign_id': row['campaign_id'],
                'insight_type': insight_type,
                'granularity': 'weekly',
                'period_start': row['week_start_date']
            }
            
            # Add breakdown information if applicable
            for col in breakdown_cols:
                if col in row and pd.notna(row[col]) and row[col] != '':
                    insight_node[col] = str(row[col])
            
            # Add aggregated metrics
            for col in weekly_df.columns:
                if col not in group_cols + ['insight_id'] and pd.notna(row[col]):
                    if isinstance(row[col], (int, float)):
                        insight_node[col] = row[col]
                    else:
                        insight_node[col] = str(row[col])
            
            weekly_insight_nodes.append(insight_node)
            
            # Create relationship between Campaign and Weekly Campaign Insight
            relationships.append({
                'start_value': row['campaign_id'],  # Campaign ID
                'end_value': row['insight_id']      # Insight ID
            })
        
        # Create nodes and relationships in batches
        with self.driver.session(database="neo4j") as session:
            # Create Weekly Campaign Insight Nodes
            logger.info(f"Creating {len(weekly_insight_nodes)} {entity_type} nodes for {insight_type}...")
            for i in range(0, len(weekly_insight_nodes), self.BATCH_SIZE):
                batch_nodes = weekly_insight_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed {entity_type} node batch {i // self.BATCH_SIZE + 1} for {insight_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                    logger.error(f"Error writing {entity_type} node batch for {insight_type}: {e}")
            
            # Create Relationships (Campaign -> WeeklyCampaignInsight)
            rel_type = "HAS_WEEKLY_INSIGHT"
            start_node_type = "FbCampaign"
            start_node_key = "id"
            end_node_type = entity_type # "FbWeeklyCampaignInsight"
            end_node_key = id_property # "insight_id"
            
            logger.info(f"Creating {len(relationships)} {rel_type} relationships for {insight_type}...")
            
            # Deduplicate relationships before batching
            unique_rels = {(rel['start_value'], rel['end_value']) for rel in relationships}
            final_relationships = [{'start_value': start, 'end_value': end} for start, end in unique_rels]
            
            for i in range(0, len(final_relationships), self.BATCH_SIZE):
                batch_rels = final_relationships[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_relationships_batch,
                        start_node_type,
                        end_node_type,
                        rel_type,
                        batch_rels,
                        start_key=start_node_key,
                        end_key=end_node_key
                    )
                    logger.info(f"Processed {rel_type} relationship batch {i // self.BATCH_SIZE + 1} for {insight_type} ({len(batch_rels)} rels)")
                except Exception as e:
                    logger.error(f"Error writing {rel_type} relationship batch for {insight_type}: {e}")
        
        logger.info(f"Finished transforming {len(weekly_insight_nodes)} {entity_type} records for {insight_type}.")

    def transform_campaign_monthly_insight(self, insight_df: pd.DataFrame, insight_type: str = "basic"):
        """
        Transform monthly aggregated Campaign Insight data and link to Campaign.
        
        Args:
            insight_df: DataFrame containing insight data
            insight_type: Type of insight data ('basic', 'age_gender', etc.)
        """
        if insight_df.empty:
            logger.info(f"Monthly Campaign Insight dataframe ({insight_type}) is empty. Skipping transformation.")
            return
        
        entity_type = "FbMonthlyCampaignInsight"
        id_property = "insight_id"  # Generated composite key
        logger.info(f"Transforming {len(insight_df)} {insight_type} records into monthly aggregated campaign insights...")
        
        # Define breakdown columns based on insight_type
        breakdown_cols = []
        if insight_type == "age_gender":
            # Include both age and gender for the age_gender breakdown
            breakdown_cols = ['age', 'gender']
        # Add cases for other insight types as needed
        
        # Define metrics to aggregate (sum for most, possibly compute averages for others)
        sum_metric_cols = [
            'spend', 'impressions', 'clicks', 'reach', 'social_spend'
        ]
        
        # Validate required columns exist
        required_cols = ['ad_id', 'campaign_id', 'date_start']
        for col in required_cols:
            if col not in insight_df.columns:
                logger.error(f"Required column '{col}' missing from insight dataframe. Cannot process {insight_type} monthly campaign insights.")
                return
        
        # For breakdowns, ensure the breakdown columns exist
        if breakdown_cols:
            missing_breakdowns = [col for col in breakdown_cols if col not in insight_df.columns]
            if missing_breakdowns:
                logger.error(f"Breakdown columns {missing_breakdowns} missing from {insight_type} dataframe. Cannot process monthly campaign insights.")
                return
        
        # Filter out metrics that are not present in the dataframe
        available_metrics = [col for col in sum_metric_cols if col in insight_df.columns]
        if not available_metrics:
            logger.warning(f"No metrics found for {insight_type} monthly campaign insights. Will only create structural nodes.")
        
        # Copy only needed columns and prepare DataFrame
        selected_cols = ['ad_id', 'campaign_id', 'date_start'] + breakdown_cols + available_metrics
        processed_df = insight_df[selected_cols].copy()
        
        # Ensure ID and breakdown fields are strings, metrics are numeric
        processed_df['ad_id'] = processed_df['ad_id'].astype(str)
        processed_df['campaign_id'] = processed_df['campaign_id'].astype(str)
        for col in breakdown_cols:
            processed_df[col] = processed_df[col].astype(str)
        
        # Convert date_start to datetime for aggregation
        try:
            processed_df['date_start'] = pd.to_datetime(processed_df['date_start'])
        except Exception as e:
            logger.error(f"Error converting date_start to datetime for monthly campaign aggregation: {e}")
            return
        
        # Calculate month start date (first day of the month)
        processed_df['month_start_date'] = processed_df['date_start'].dt.strftime('%Y-%m-01')
        
        # Convert metrics to numeric for aggregation
        for col in available_metrics:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        
        # --- Debugging Check: Log rows with clicks > impressions before aggregation ---
        invalid_ctr_rows = processed_df[processed_df['clicks'] > processed_df['impressions']]
        if not invalid_ctr_rows.empty:
            logger.warning(f"Found {len(invalid_ctr_rows)} rows in input data for monthly campaign insights ({insight_type}) where clicks > impressions BEFORE aggregation. Example rows:")
            # Log a few examples to avoid excessive output
            logger.warning(invalid_ctr_rows.head().to_string())
        # --- End Debugging Check ---

        # Group by campaign_id, month_start_date, and any breakdown columns
        group_cols = ['campaign_id', 'month_start_date'] + breakdown_cols
        logger.info(f"Aggregating {insight_type} insights by {group_cols}...")
        
        # Aggregate data
        try:
            # Sum metrics
            agg_dict = {metric: 'sum' for metric in available_metrics}
            monthly_df = processed_df.groupby(group_cols).agg(agg_dict).reset_index()
            
            # Add derived metrics after aggregation
            if 'clicks' in monthly_df.columns and 'impressions' in monthly_df.columns:
                # Impressions can be 0, safely handle division
                monthly_df['ctr'] = monthly_df.apply(
                    lambda x: (x['clicks'] / x['impressions'] * 100) if x['impressions'] > 0 else 0, 
                    axis=1
                )
            
            if 'spend' in monthly_df.columns and 'clicks' in monthly_df.columns:
                # Clicks can be 0, safely handle division
                monthly_df['cpc'] = monthly_df.apply(
                    lambda x: (x['spend'] / x['clicks']) if x['clicks'] > 0 else 0, 
                    axis=1
                )
            
            if 'spend' in monthly_df.columns and 'impressions' in monthly_df.columns:
                # Calculate CPM (cost per 1000 impressions)
                monthly_df['cpm'] = monthly_df.apply(
                    lambda x: (x['spend'] / x['impressions'] * 1000) if x['impressions'] > 0 else 0, 
                    axis=1
                )
            
            logger.info(f"Successfully aggregated into {len(monthly_df)} monthly campaign records for {insight_type}.")
        except Exception as e:
            logger.error(f"Error during monthly campaign aggregation for {insight_type}: {e}")
            return
        
        # Generate insight_id for each aggregated row
        def generate_monthly_campaign_insight_id(row):
            # Create breakdown dict for the row if breakdown columns are present
            breakdown_data = None
            if breakdown_cols:
                breakdown_data = {col: row[col] for col in breakdown_cols}
            
            return self._generate_insight_id(
                entity_id=row['campaign_id'],
                period_start=row['month_start_date'],
                granularity='monthly',
                insight_type=insight_type,
                entity_type='campaign',
                breakdown_data=breakdown_data
            )
        
        monthly_df['insight_id'] = monthly_df.apply(generate_monthly_campaign_insight_id, axis=1)
        
        # Create node data dictionaries and relationship data
        monthly_insight_nodes = []
        relationships = []
        
        # Convert the DataFrame to records and process each aggregated row
        monthly_records = monthly_df.fillna('').to_dict('records')
        
        for row in monthly_records:
            insight_node = {
                'insight_id': row['insight_id'],
                'campaign_id': row['campaign_id'],
                'insight_type': insight_type,
                'granularity': 'monthly',
                'period_start': row['month_start_date']
            }
            
            # Add breakdown information if applicable
            for col in breakdown_cols:
                if col in row and pd.notna(row[col]) and row[col] != '':
                    insight_node[col] = str(row[col])
            
            # Add aggregated metrics
            for col in monthly_df.columns:
                if col not in group_cols + ['insight_id'] and pd.notna(row[col]):
                    if isinstance(row[col], (int, float)):
                        insight_node[col] = row[col]
                    else:
                        insight_node[col] = str(row[col])
            
            monthly_insight_nodes.append(insight_node)
            
            # Create relationship between Campaign and Monthly Campaign Insight
            relationships.append({
                'start_value': row['campaign_id'],  # Campaign ID
                'end_value': row['insight_id']      # Insight ID
            })
        
        # Create nodes and relationships in batches
        with self.driver.session(database="neo4j") as session:
            # Create Monthly Campaign Insight Nodes
            logger.info(f"Creating {len(monthly_insight_nodes)} {entity_type} nodes for {insight_type}...")
            for i in range(0, len(monthly_insight_nodes), self.BATCH_SIZE):
                batch_nodes = monthly_insight_nodes[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_entity_nodes_batch,
                        entity_type,
                        batch_nodes,
                        id_property
                    )
                    logger.info(f"Processed {entity_type} node batch {i // self.BATCH_SIZE + 1} for {insight_type} ({len(batch_nodes)} nodes)")
                except Exception as e:
                    logger.error(f"Error writing {entity_type} node batch for {insight_type}: {e}")
            
            # Create Relationships (Campaign -> MonthlyCampaignInsight)
            rel_type = "HAS_MONTHLY_INSIGHT"
            start_node_type = "FbCampaign"
            start_node_key = "id"
            end_node_type = entity_type # "FbMonthlyCampaignInsight"
            end_node_key = id_property # "insight_id"
            
            logger.info(f"Creating {len(relationships)} {rel_type} relationships for {insight_type}...")
            
            # Deduplicate relationships before batching
            unique_rels = {(rel['start_value'], rel['end_value']) for rel in relationships}
            final_relationships = [{'start_value': start, 'end_value': end} for start, end in unique_rels]
            
            for i in range(0, len(final_relationships), self.BATCH_SIZE):
                batch_rels = final_relationships[i:i + self.BATCH_SIZE]
                try:
                    session.execute_write(
                        self.create_relationships_batch,
                        start_node_type,
                        end_node_type,
                        rel_type,
                        batch_rels,
                        start_key=start_node_key,
                        end_key=end_node_key
                    )
                    logger.info(f"Processed {rel_type} relationship batch {i // self.BATCH_SIZE + 1} for {insight_type} ({len(batch_rels)} rels)")
                except Exception as e:
                    logger.error(f"Error writing {rel_type} relationship batch for {insight_type}: {e}")
        
        logger.info(f"Finished transforming {len(monthly_insight_nodes)} {entity_type} records for {insight_type}.")

    # --- Pipeline Orchestration ---

    def run_pipeline(self, sql_data: Dict[str, pd.DataFrame]):
        """Run the complete transformation pipeline for Facebook data."""
        try:
            logger.info("Starting Facebook data transformation pipeline")

            # == Step 1: Setup Neo4j ==
            logger.info("Pipeline Step 1: Creating Neo4j constraints and indexes")
            # Ensure driver is valid before proceeding with transactions
            if not self.driver:
                logger.error("Neo4j driver not initialized. Aborting pipeline.")
                return
            try:
                self.driver.verify_connectivity()
                logger.info("Neo4j connection verified.")
            except Exception as e:
                logger.error(f"Neo4j connection failed verification: {e}")
                return # Cannot proceed without connection

            self.create_constraints()
            self.create_indexes()

            # == Step 2: Load Core Entities (Order matters) ==
            # Use .get() for safer access to sql_data dictionary
            adaccount_df = sql_data.get('ad_account', pd.DataFrame())
            if not adaccount_df.empty:
                self.transform_adaccount(adaccount_df)
            else:
                logger.warning("Skipping AdAccount transformation - 'ad_account' data missing or empty.")

            campaign_df = sql_data.get('campaigns', pd.DataFrame())
            if not campaign_df.empty and not adaccount_df.empty:
                self.transform_campaign(campaign_df)
            else:
                logger.warning("Skipping Campaign transformation - 'campaigns' or 'ad_account' data missing or empty.")

            ads_df = sql_data.get('ads', pd.DataFrame())
            ads_insights_df = sql_data.get('ads_insights', pd.DataFrame())
            if not ads_df.empty and not ads_insights_df.empty and not campaign_df.empty:
                self.transform_adset(ads_df, ads_insights_df)
            else:
                logger.warning("Skipping AdSet transformation - 'ads', 'ads_insights', or 'campaigns' data missing or empty.")

            if not ads_df.empty: # Depends on AdSet being created first
                 # Capture the processed ads_df which now includes creative_id
                 processed_ads_df = self.transform_ad(ads_df)
            else:
                 logger.warning("Skipping Ad transformation - 'ads' data missing or empty.")
                 processed_ads_df = pd.DataFrame() # Ensure it's an empty DF if skipped

            adcreative_df = sql_data.get('ad_creatives', pd.DataFrame())
            # Use processed_ads_df here
            if not adcreative_df.empty and not processed_ads_df.empty: 
                 self.transform_adcreative(adcreative_df, processed_ads_df)
            else:
                 logger.warning("Skipping AdCreative transformation - 'ad_creatives' or processed 'ads' data missing or empty.")

            image_df = sql_data.get('images', pd.DataFrame())
            if not image_df.empty and not adcreative_df.empty: # Depends on AdCreative being created first
                 # Pass adcreative_df for potential mapping needs
                 self.transform_image(image_df, adcreative_df)
            else:
                 logger.warning("Skipping Image transformation - 'images' or 'ad_creatives' data missing or empty.")

            # == Step 3: Load Insights (Link to Ads) ==
            # Use processed_ads_df for the check, although insights primarily use ads_insights_df
            if not ads_insights_df.empty and not processed_ads_df.empty:
                 self.transform_insight(ads_insights_df, insight_type="basic")
            else:
                 logger.warning("Skipping basic Insight transformation - 'ads_insights' or processed 'ads' data missing or empty.")

            # Add calls for other insight breakdowns
            insight_breakdowns = [
                 ("ads_insights_age_and_gender", "age_gender"),
                 ("ads_insights_country", "country"),
                 ("ads_insights_delivery_device", "delivery_device"),
                 ("ads_insights_dma", "dma"),
                 ("ads_insights_platform_and_device", "platform_device"),
                 ("ads_insights_region", "region"),
                 # ("ads_insights_action_type", "action_type") # Decide how to model action breakdowns
            ]
            for table_name, type_label in insight_breakdowns:
                insight_df = sql_data.get(table_name, pd.DataFrame())
                # Use processed_ads_df for the check
                if not insight_df.empty and not processed_ads_df.empty:
                    self.transform_insight(insight_df, insight_type=type_label)
                else:
                     logger.warning(f"Skipping {type_label} Insight transformation - '{table_name}' or processed 'ads' data missing or empty.")

            logger.info("Facebook pipeline completed successfully")

        except Exception as e:
            logger.error(f"Error in Facebook pipeline execution: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # Avoid raising here to allow finally block to run
        finally:
            logger.info("Executing finally block for Facebook pipeline.")
            # No need to call self.close() here, it's handled by the main function's finally block


def main():
    logger.info("Starting Facebook data pipeline execution script")

    # --- Configuration ---
    # Neo4j connection details (use environment variables)
    # Use different env var names to avoid conflict with Google pipeline
    NEO4J_URI = os.getenv("FB_NEO4J_URI", "neo4j+s://13bcaa9b.databases.neo4j.io") # <-- PLEASE UPDATE
    NEO4J_USER = os.getenv("FB_NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("h","jtSyN5pLuXxSnlm937XFJkVej8GZ2tIn1NZwEWEH8FI") # <-- PLEASE SET ENV VAR

    # PostgreSQL connection details (use environment variables)
    PG_DB = os.getenv("FB_PG_DB", "defaultdb")
    PG_USER = os.getenv("FB_PG_USER", "avnadmin")
    PG_PASSWORD = os.getenv("FB_PG_PASSWORD") # <-- PLEASE SET ENV VAR
    PG_HOST = os.getenv("FB_PG_HOST","pg-1fd96600-srinixd-b9bc.l.aivencloud.com") # <-- PLEASE SET ENV VAR
    PG_PORT = os.getenv("FB_PG_PORT", "14959") # <-- PLEASE SET ENV VAR (default 5432)

    # Basic validation for essential connection details
    if not NEO4J_PASSWORD:
         logger.error("Neo4j password (FB_NEO4J_PASSWORD) not found in environment variables.")
         return # Cannot proceed
    if not PG_PASSWORD or not PG_HOST or not PG_PORT:
         logger.error("PostgreSQL connection details (FB_PG_PASSWORD, FB_PG_HOST, FB_PG_PORT) incomplete in environment variables.")
         return # Cannot proceed

    pg_conn = None
    pipeline = None

    try:
        # --- Connect to PostgreSQL ---
        logger.info(f"Connecting to PostgreSQL database '{PG_DB}' at {PG_HOST}:{PG_PORT}")
        pg_conn = psycopg2.connect(
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        logger.info("Successfully connected to PostgreSQL database")

        # --- Initialize Pipeline ---
        # Wrap initialization in try-except to ensure close is called if it fails
        try:
            pipeline = FacebookPipeline(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
            logger.info("Successfully initialized FacebookPipeline")
        except Exception as e:
            logger.error(f"Failed to initialize FacebookPipeline: {e}")
            # No need to raise here, finally block will handle cleanup
            return # Stop execution if pipeline init fails

        # --- Load SQL Data ---
        sql_data = {}
        # List of tables based on facebook_schema.json
        tables_to_load = [
            "ad_account",
            "ad_creatives",
            "ads",
            "ads_insights",
            "ads_insights_action_type",
            "ads_insights_age_and_gender",
            "ads_insights_country",
            "ads_insights_delivery_device",
            "ads_insights_dma",
            "ads_insights_platform_and_device",
            "ads_insights_region",
            "campaigns",
            "images",
            # "activitiesad_sets" # This table seems empty in the schema, skip unless confirmed
        ]

        logger.info("Starting to load data from PostgreSQL tables")
        with pg_conn.cursor() as cursor:
            for table in tables_to_load:
                try:
                    # More robust table existence check
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name = %s
                        );
                    """, (table,))
                    table_exists = cursor.fetchone()[0]

                    if table_exists:
                        logger.info(f"Loading data from table: {table}")
                        # Use copy_expert for potentially faster loading? Or stick with pandas.
                        sql_data[table] = pd.read_sql(f'SELECT * FROM "{table}" ', pg_conn) # Quote table name
                        # Data Cleaning (Example - handle potential JSON strings if needed)
                        # for col in sql_data[table].columns:
                        #     # Attempt to parse columns that might be JSON strings stored as text
                        #     if isinstance(sql_data[table][col].iloc[0], str) and '{ ' in sql_data[table][col].iloc[0]: 
                        #         try:
                        #            sql_data[table][col] = sql_data[table][col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                        #         except (json.JSONDecodeError, TypeError):
                        #             pass # Ignore columns that fail parsing
                        logger.info(f"Successfully loaded {len(sql_data[table])} records from {table}")
                    else:
                        logger.warning(f"Table '{table}' does not exist in the 'public' schema. Skipping.")
                        sql_data[table] = pd.DataFrame() # Add empty DataFrame to prevent KeyErrors later
                except psycopg2.Error as db_err:
                    logger.error(f"Database error loading or checking table '{table}': {db_err}")
                    # Optionally rollback transaction if needed: pg_conn.rollback()
                    sql_data[table] = pd.DataFrame() # Ensure key exists
                except Exception as e:
                    logger.error(f"General error loading or checking table '{table}': {str(e)}")
                    sql_data[table] = pd.DataFrame() # Ensure key exists
                    # Decide if you want to continue or raise the error
                    # raise

        # --- Run Pipeline ---
        if pipeline: # Ensure pipeline was initialized successfully
            logger.info("Starting Facebook pipeline run")
            pipeline.run_pipeline(sql_data)
            logger.info("Facebook pipeline execution completed successfully via main")
        else:
            logger.error("Pipeline object not created, skipping run.")

    except psycopg2.Error as db_err:
        logger.error(f"PostgreSQL connection failed: {db_err}")
    except Exception as e:
        logger.error(f"Facebook pipeline failed in main: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # No raise here, finally handles cleanup
    finally:
        # --- Close Connections ---
        logger.info("Executing finally block in main.")
        if pg_conn:
            try:
                pg_conn.close()
                logger.info("Closed PostgreSQL connection")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection: {e}")
        if pipeline:
            try:
                pipeline.close() # Ensure Neo4j connection is closed
                logger.info("Pipeline close requested in main finally block.")
            except Exception as e:
                logger.error(f"Error closing pipeline (Neo4j driver): {e}")

if __name__ == "__main__":
    main()
    logger.info("Facebook pipeline script finished.")
