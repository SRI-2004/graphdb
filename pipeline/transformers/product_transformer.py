import pandas as pd
import logging
from ..graph_base import GraphTransformer

logger = logging.getLogger(__name__)

def transform_product(transformer: GraphTransformer, shopping_perf_df: pd.DataFrame):
    """
    Transform product data from shopping_perf_df into Product nodes and
    links them to Campaigns
    """
    if shopping_perf_df.empty:
        logger.info("No product data to transform.")
        return

    logger.info(f"Starting Product transformation from {len(shopping_perf_df)} shopping performance rows.")

    required_columns = [
        'product_id', 'campaign_id', 'customer_id',
        'product_title', 'product_type_l1', 'product_type_l2', 
        'product_type_l3', 'product_type_l4', 'product_type_l5'
    ]

    # Check if required columns exist
    missing_columns = [col for col in required_columns if col not in shopping_perf_df.columns]
    if missing_columns:
        logger.warning(f"Missing required columns for product transformation: {missing_columns}")
        # Filter to only existing columns
        required_columns = [col for col in required_columns if col in shopping_perf_df.columns]

    # Drop rows with missing product_id and campaign_id
    pre_filter_count = len(shopping_perf_df)
    shopping_perf_df = shopping_perf_df.dropna(subset=['product_id', 'campaign_id'])
    
    if len(shopping_perf_df) < pre_filter_count:
        logger.warning(f"Dropped {pre_filter_count - len(shopping_perf_df)} rows with missing product_id or campaign_id")

    if shopping_perf_df.empty:
        logger.warning("No valid product data after filtering.")
        return

    # Prepare unique product nodes
    unique_products = shopping_perf_df.drop_duplicates(subset=['product_id'])[required_columns].fillna('')
    logger.info(f"Processing {len(unique_products)} unique products.")

    # Prepare campaign-product relationships
    relationships_df = shopping_perf_df[['campaign_id', 'product_id']].drop_duplicates()
    logger.info(f"Will create {len(relationships_df)} Campaign-Product relationships.")

    with transformer.driver.session() as session:
        # Create product nodes in batches
        for start_idx in range(0, len(unique_products), transformer.BATCH_SIZE):
            end_idx = min(start_idx + transformer.BATCH_SIZE, len(unique_products))
            batch = unique_products.iloc[start_idx:end_idx]
            logger.debug(f"Processing batch of {len(batch)} products ({start_idx+1}-{end_idx}/{len(unique_products)}).")

            # Create product nodes
            nodes = [{
                'product_id': row['product_id'],
                'title': row['product_title'],
                'type_l1': row['product_type_l1'],
                'type_l2': row['product_type_l2'],
                'type_l3': row['product_type_l3'],
                'type_l4': row['product_type_l4'],
                'type_l5': row['product_type_l5']
            } for _, row in batch.iterrows()]
            
            session.execute_write(transformer.create_entity_nodes_batch, 'Product', nodes)

        # Create relationships between campaigns and products
        for start_idx in range(0, len(relationships_df), transformer.BATCH_SIZE):
            end_idx = min(start_idx + transformer.BATCH_SIZE, len(relationships_df))
            batch = relationships_df.iloc[start_idx:end_idx]
            logger.debug(f"Processing batch of {len(batch)} campaign-product relationships ({start_idx+1}-{end_idx}/{len(relationships_df)}).")

            # Create relationships
            relationships = [{
                'start_key': 'campaign_id',
                'start_value': row['campaign_id'],
                'end_key': 'product_id',
                'end_value': row['product_id']
            } for _, row in batch.iterrows()]
            
            session.execute_write(
                transformer.create_relationships_batch,
                'Campaign',
                'Product',
                'ADVERTISES_PRODUCT',
                relationships
            )

    logger.info("Completed Product transformation.")
