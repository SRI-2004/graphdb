import pandas as pd
import json
import logging
from typing import Dict, List, Any
from ..graph_base import GraphTransformer

logger = logging.getLogger(__name__)

def transform_adgroup(transformer: GraphTransformer, adgroup_df: pd.DataFrame):
    """Transform ad group data using batch processing, including creating AdGroupBiddingSettings nodes for overrides."""
    logger.info(f"--- Verifying adgroup_df ---")
    logger.info(f"Input adgroup_df shape: {adgroup_df.shape}")
    logger.info(f"First 5 rows of adgroup_df:\n{adgroup_df.head().to_string()}")
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

    with transformer.driver.session() as session:
        for start_idx in range(0, len(adgroup_df), transformer.BATCH_SIZE):
            batch = adgroup_df.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
            
            session.execute_write(transformer.create_entity_nodes_batch, 'AdGroup', adgroup_nodes)
            
            # --- Create Campaign -> AdGroup relationships (as before) ---
            campaign_relationships = [{
                'start_key': 'campaign_id',
                'start_value': row['campaign_id'],
                'end_key': 'ad_group_id',
                'end_value': row['ad_group_id']
            } for _, row in batch.iterrows() if 'campaign_id' in row and pd.notna(row['campaign_id'])] # Added check for campaign_id
            
            if campaign_relationships:
                session.execute_write(
                transformer.create_relationships_batch,
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
                session.execute_write(transformer.create_entity_nodes_batch, 'AdGroupBiddingSettings', bidding_settings_nodes)
            
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