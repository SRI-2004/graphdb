import pandas as pd
import json
import logging
from typing import Dict, List, Any
from ..graph_base import GraphTransformer

logger = logging.getLogger(__name__)

def transform_campaign(transformer: GraphTransformer, campaign_df: pd.DataFrame, customer_id: str):
    """Transform campaign data using batch processing"""
    with transformer.driver.session() as session:
        for start_idx in range(0, len(campaign_df), transformer.BATCH_SIZE):
            batch = campaign_df.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
            
            session.execute_write(transformer.create_entity_nodes_batch, 'Campaign', nodes)
            
            # Create relationships to AdAccount
            relationships = [{
                'start_key': 'account_id',
                'start_value': customer_id,
                'end_key': 'campaign_id',
                'end_value': row['campaign_id']
            } for _, row in batch.iterrows()]
            
            session.execute_write(
                transformer.create_relationships_batch,
                'AdAccount',
                'Campaign',
                'HAS_CAMPAIGN',
                relationships
            )

def transform_campaign_budget(transformer: GraphTransformer, budget_df: pd.DataFrame):
    """Transform campaign budget data using batch processing"""
    with transformer.driver.session() as session:
        for start_idx in range(0, len(budget_df), transformer.BATCH_SIZE):
            batch = budget_df.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
            
            session.execute_write(transformer.create_entity_nodes_batch, 'CampaignBudget', nodes)
            
            # Create relationships to Campaign
            relationships = [{
                'start_key': 'campaign_id',
                'start_value': row['campaign_id'],
                'end_key': 'budget_id',
                'end_value': row['campaign_budget_id']
            } for _, row in batch.iterrows()]
            
            session.execute_write(
                transformer.create_relationships_batch,
                'Campaign',
                'CampaignBudget',
                'USES_BUDGET',
                relationships
            )

def transform_campaign_criterion(transformer: GraphTransformer, criterion_df: pd.DataFrame):
    """Transform campaign criterion data using batch processing with specific node types."""
    with transformer.driver.session() as session:
        # Prepare lists for batching GeoLocation nodes and relationships
        geo_location_nodes_batch = []
        location_relationships_batch = []

        for start_idx in range(0, len(criterion_df), transformer.BATCH_SIZE):
            batch = criterion_df.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
            session.execute_write(transformer.create_entity_nodes_batch, 'GeoLocation', geo_location_nodes_batch)
        
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
                    transformer.create_relationships_batch,
                    'Campaign',
                    'GeoLocation',
                    rel_type, # Use the specific relationship type (TARGETS or EXCLUDES)
                    relationships
                )
    logger.info("Completed CampaignCriterion transformation (including GeoLocation).")

def transform_campaign_weekly_metrics(transformer: GraphTransformer, campaign_df: pd.DataFrame):
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
    weekly_agg['average_cpm'] = (weekly_agg['metrics_cost_micros'] * 1000 / weekly_agg['metrics_impressions']).fillna(0)
    weekly_agg['cost_per_conversion'] = (weekly_agg['metrics_cost_micros'] / weekly_agg['metrics_conversions']).fillna(0)
    weekly_agg['value_per_conversion'] = (weekly_agg['metrics_conversions_value'] / weekly_agg['metrics_conversions']).fillna(0)
    weekly_agg['interaction_rate'] = (weekly_agg['metrics_interactions'] / weekly_agg['metrics_impressions']).fillna(0)
    weekly_agg['all_conversions_value_per_cost'] = (weekly_agg['metrics_all_conversions_value'] / weekly_agg['metrics_cost_micros']).fillna(0)
    
    # Replace Inf/-Inf with 0
    weekly_agg = weekly_agg.replace([float('inf'), -float('inf')], 0)
    
    # Prepare nodes and relationships
    with transformer.driver.session() as session:
        for start_idx in range(0, len(weekly_agg), transformer.BATCH_SIZE):
            batch = weekly_agg.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
            weekly_metric_nodes = []
            relationships = []
            
            for _, row in batch.iterrows():
                node = {
                    'campaign_id': row['campaign_id'],
                    'week_start_date': row['week_start_date_str'],
                    'entity_type': 'Campaign', # Identify as Campaign metrics 
                    'days_aggregated': int(row['days_aggregated']),
                    # Could add week_number (1-53) or calendar properties as needed
                }
                
                # Add summed metrics (convert micros)
                for col in sum_metrics:
                    prop_name = col.replace('metrics_', '')
                    if 'micros' in prop_name:
                        # Convert to dollars/primary currency unit
                        node[prop_name] = float(row[col]) / 1000000
                    else:
                        node[prop_name] = float(row[col])
                
                # Add averaged metrics (impressions shares, etc.)
                for col in avg_metrics:
                    prop_name = col.replace('metrics_', '')
                    node[prop_name] = float(row[col])
                    
                # Add calculated ratios
                node['ctr'] = float(row['ctr'])
                node['average_cpc'] = float(row['average_cpc']) / 1000000 # Convert micros
                node['average_cpm'] = float(row['average_cpm']) / 1000000 # Convert micros
                node['cost_per_conversion'] = float(row['cost_per_conversion']) / 1000000 # Convert micros
                node['value_per_conversion'] = float(row['value_per_conversion'])
                node['interaction_rate'] = float(row['interaction_rate'])
                node['all_conversions_value_per_cost'] = float(row['all_conversions_value_per_cost']) * 1000000 # Adjusted for micros
                
                # Clean up potential NaN/Inf after calculations
                for key, value in node.items():
                    if isinstance(value, float) and (pd.isna(value) or value == float('inf') or value == -float('inf')):
                        node[key] = 0.0
                
                weekly_metric_nodes.append(node)
                relationships.append({
                    'start_key': 'campaign_id',
                    'start_value': row['campaign_id'],
                    'end_key': 'campaign_id',
                    'end_value': row['campaign_id'],
                    'week_key': 'week_start_date',
                    'week_value': row['week_start_date_str']
                })
            
            if weekly_metric_nodes:
                logger.debug(f"Creating batch of {len(weekly_metric_nodes)} WeeklyMetric nodes for Campaigns.")
                session.execute_write(transformer.create_entity_nodes_batch, 'WeeklyMetric', weekly_metric_nodes)
                
                rel_query = """
                UNWIND $relationships as rel
                MATCH (start:Campaign {campaign_id: rel.start_value})
                MATCH (end:WeeklyMetric {campaign_id: rel.end_value, week_start_date: rel.week_value})
                MERGE (start)-[r:HAS_WEEKLY_METRICS]->(end)
                """
                logger.debug(f"Creating batch of {len(relationships)} HAS_WEEKLY_METRICS relationships.")
                session.run(rel_query, {'relationships': relationships})
    
    logger.info("Completed CampaignWeeklyMetric transformation.")

def transform_campaign_overall_metrics(transformer: GraphTransformer, campaign_df: pd.DataFrame):
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
    with transformer.driver.session() as session:
        for start_idx in range(0, len(overall_agg), transformer.BATCH_SIZE):
            batch = overall_agg.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
                session.execute_write(transformer.create_entity_nodes_batch, 'CampaignOverallMetric', overall_metric_nodes)

                rel_query = """
                UNWIND $relationships as rel
                MATCH (start:Campaign {campaign_id: rel.start_value})
                MATCH (end:CampaignOverallMetric {campaign_id: rel.end_value})
                MERGE (start)-[r:HAS_OVERALL_METRICS]->(end)
                """
                logger.debug(f"Creating batch of {len(relationships)} HAS_OVERALL_METRICS relationships for Campaign.")
                session.run(rel_query, {'relationships': relationships})
                
        logger.info("Completed CampaignOverallMetric transformation.")

def transform_campaign_monthly_metrics(transformer: GraphTransformer, campaign_df: pd.DataFrame):
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
    with transformer.driver.session() as session:
        for start_idx in range(0, len(monthly_agg), transformer.BATCH_SIZE):
            batch = monthly_agg.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
            monthly_metric_nodes = []
            relationships = []
            
            for _, row in batch.iterrows():
                node = {
                    'campaign_id': row['campaign_id'],
                    'month_start_date': row['month_start_date'],
                    'days_aggregated': int(row['days_aggregated'])
                }
                
                # Add summed metrics (convert micros)
                for col in sum_metrics:
                    prop_name = col.replace('metrics_', '')
                    if 'micros' in prop_name:
                        # Convert to dollars/primary currency unit
                        node[prop_name] = float(row[col]) / 1000000
                    else:
                        node[prop_name] = float(row[col])
                
                # Add averaged metrics (impressions shares, etc.)
                for col in avg_metrics:
                    prop_name = col.replace('metrics_', '')
                    node[prop_name] = float(row[col])
                    
                # Add calculated ratios
                node['ctr'] = float(row['ctr'])
                node['average_cpc'] = float(row['average_cpc']) / 1000000 # Convert micros
                node['average_cpm'] = float(row['average_cpm']) / 1000000 # Convert micros
                node['cost_per_conversion'] = float(row['cost_per_conversion']) / 1000000 # Convert micros
                node['value_per_conversion'] = float(row['value_per_conversion'])
                node['interaction_rate'] = float(row['interaction_rate'])
                node['all_conversions_value_per_cost'] = float(row['all_conversions_value_per_cost']) * 1000000 # Adjusted for micros
                
                # Clean up potential residual NaN/Inf
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
                session.execute_write(transformer.create_entity_nodes_batch, 'CampaignMonthlyMetric', monthly_metric_nodes)
                
                rel_query = """
                UNWIND $relationships as rel
                MATCH (start:Campaign {campaign_id: rel.start_value})
                MATCH (end:CampaignMonthlyMetric {campaign_id: rel.end_value, month_start_date: rel.month_value})
                MERGE (start)-[r:HAS_MONTHLY_METRICS]->(end)
                """
                logger.debug(f"Creating batch of {len(relationships)} HAS_MONTHLY_METRICS relationships for Campaign.")
                session.run(rel_query, {'relationships': relationships})
    
    logger.info("Completed CampaignMonthlyMetric transformation.")
