import pandas as pd
import json
import logging
from typing import Dict, List, Any, Optional
from ..graph_base import GraphTransformer

logger = logging.getLogger(__name__)

def transform_label(transformer: GraphTransformer, label_df: pd.DataFrame, customer_label_df: Optional[pd.DataFrame] = None):
    """Transform label data into graph format using batch processing"""
    logger.info(f"Starting Label transformation for {len(label_df)} labels.")
    
    with transformer.driver.session() as session:
        # Remove duplicates based on label_id
        label_df = label_df.drop_duplicates(subset=['label_id'])
        logger.info(f"Processing {len(label_df)} unique labels.")
        
        for start_idx in range(0, len(label_df), transformer.BATCH_SIZE):
            end_idx = min(start_idx + transformer.BATCH_SIZE, len(label_df))
            batch = label_df.iloc[start_idx:end_idx]
            logger.debug(f"Processing batch of {len(batch)} labels ({start_idx+1}-{end_idx}/{len(label_df)}).")
            
            # Create label nodes in batch
            nodes = [{
                'label_id': row['label_id'],
                'name': row['label_name'],
                'status': row.get('label_status', ''),
                'resource_name': row.get('label_resource_name', ''),
                'description': row.get('label_text_label_description', ''),
                'background_color': row.get('label_text_label_background_color', '')
            } for _, row in batch.iterrows()]
            
            session.execute_write(transformer.create_entity_nodes_batch, 'Label', nodes)
        
        # Create relationships to AdAccount if customer_label data is available
        if customer_label_df is not None and not customer_label_df.empty:
            # Remove duplicates
            customer_label_df = customer_label_df.drop_duplicates(subset=['customer_id', 'customer_label_label'])
            logger.info(f"Processing {len(customer_label_df)} customer-label relationships.")
            
            for start_idx in range(0, len(customer_label_df), transformer.BATCH_SIZE):
                end_idx = min(start_idx + transformer.BATCH_SIZE, len(customer_label_df))
                batch = customer_label_df.iloc[start_idx:end_idx]
                logger.debug(f"Processing batch of {len(batch)} customer-label relationships ({start_idx+1}-{end_idx}/{len(customer_label_df)}).")
                
                # Create relationships to AdAccount
                relationships = [{
                    'start_key': 'account_id',
                    'start_value': row['customer_id'],
                    'end_key': 'label_id',
                    'end_value': row['customer_label_label']
                } for _, row in batch.iterrows()]
                
                session.execute_write(
                    transformer.create_relationships_batch,
                    'AdAccount',
                    'Label',
                    'HAS_LABEL',
                    relationships
                )
    
    logger.info("Completed Label transformation.")

def transform_asset(transformer: GraphTransformer, asset_df: pd.DataFrame):
    """Transform asset data into graph format using batch processing"""
    logger.info(f"Starting Asset transformation for {len(asset_df)} assets.")
    
    with transformer.driver.session() as session:
        for start_idx in range(0, len(asset_df), transformer.BATCH_SIZE):
            end_idx = min(start_idx + transformer.BATCH_SIZE, len(asset_df))
            batch = asset_df.iloc[start_idx:end_idx]
            logger.debug(f"Processing batch of {len(batch)} assets ({start_idx+1}-{end_idx}/{len(asset_df)}).")
            
            # Create asset nodes in batch
            nodes = [{
                'asset_id': row['asset_id'],
                'asset_type': row['asset_type'],
                'name': row['asset_name'],
                'file_hash': row.get('asset_file_hash', '')
            } for _, row in batch.iterrows()]
            
            session.execute_write(transformer.create_entity_nodes_batch, 'Asset', nodes)
    
    logger.info("Completed Asset transformation.")
