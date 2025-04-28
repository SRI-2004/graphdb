import pandas as pd
import json
import logging
from typing import Dict, List, Any
from ..graph_base import GraphTransformer

logger = logging.getLogger(__name__)

def transform_ad(transformer: GraphTransformer, ad_df: pd.DataFrame, customer_id: str):
    """Transform ads into graph format using batch processing"""
    with transformer.driver.session() as session:
        # Remove duplicates based on ad_id
        ad_df = ad_df.drop_duplicates(subset=['ad_group_ad_ad_id'])
        
        for start_idx in range(0, len(ad_df), transformer.BATCH_SIZE):
            batch = ad_df.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
                session.execute_write(transformer.create_entity_nodes_batch, 'Ad', ads)
                
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
                    transformer.create_relationships_batch,
                    'AdGroup',
                    'Ad',
                    'CONTAINS',
                    adgroup_relationships
                )

def transform_ad_daily_metrics(transformer: GraphTransformer, ad_legacy_df: pd.DataFrame):
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

    with transformer.driver.session() as session:
        for start_idx in range(0, len(ad_legacy_df), transformer.BATCH_SIZE):
            batch = ad_legacy_df.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
                        # Handle specific rates (ctr, interaction_rate) -> float
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
                            has_metrics = True
                
                # If no metric columns were actually filled, skip this node
                if not has_metrics:
                    logger.debug(f"Skipping AdDailyMetric node creation for ad {ad_id}, date {metric_date} - no valid metrics found")
                    continue
                
                # Fill in any missing rates that can be derived from other metrics
                # CTR = clicks / impressions
                if 'clicks' in metric_node and 'impressions' in metric_node and 'ctr' not in metric_node and metric_node['impressions'] > 0:
                    metric_node['ctr'] = metric_node['clicks'] / metric_node['impressions']
                
                # Clean up any NaN values that might have slipped through
                for k, v in list(metric_node.items()):
                    if pd.isna(v):
                        if k in ['impressions', 'clicks', 'conversions', 'all_conversions', 'view_through_conversions', 'interactions']:
                            metric_node[k] = 0
                        elif k in ['ctr', 'average_cpc', 'cost_per_conversion', 'value_per_conversion', 'interaction_rate', 'average_cpm']:
                            metric_node[k] = 0.0
                
                daily_metrics_nodes.append(metric_node)
                relationships.append({
                    'start_key': 'ad_id',
                    'start_value': ad_id,
                    'end_key': 'ad_id',
                    'end_value': ad_id,
                    'date': metric_date
                })
            
            # Batch write metric nodes
            if daily_metrics_nodes:
                session.execute_write(transformer.create_entity_nodes_batch, 'AdDailyMetric', daily_metrics_nodes)
            
            # Batch write relationships
            if relationships:
                rel_query = """
                UNWIND $relationships as rel
                MATCH (start:Ad {ad_id: rel.start_value})
                MATCH (end:AdDailyMetric {ad_id: rel.end_value, date: rel.date})
                MERGE (start)-[r:HAS_DAILY_METRICS]->(end)
                """
                session.run(rel_query, {'relationships': relationships})
    
    logger.info(f"Completed AdDailyMetric transformation - created {len(ad_legacy_df)} nodes.")

def transform_ad_overall_metrics(transformer: GraphTransformer, ad_legacy_df: pd.DataFrame):
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

    # Ensure columns exist, fill missing with 0
    for metric_col in sum_metrics:
        if metric_col not in ad_legacy_df.columns:
            ad_legacy_df[metric_col] = 0
        else:
            ad_legacy_df[metric_col] = pd.to_numeric(ad_legacy_df[metric_col], errors='coerce').fillna(0)

    # Group by Ad ID
    agg_funcs = {col: 'sum' for col in sum_metrics}
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
                    'ad_id': row['ad_id'],
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
                    'start_key': 'ad_id',
                    'start_value': row['ad_id'],
                    'end_key': 'ad_id',
                    'end_value': row['ad_id']
                })

            if overall_metric_nodes:
                logger.debug(f"Creating batch of {len(overall_metric_nodes)} AdOverallMetric nodes.")
                session.execute_write(transformer.create_entity_nodes_batch, 'AdOverallMetric', overall_metric_nodes)

                rel_query = """
                UNWIND $relationships as rel
                MATCH (start:Ad {ad_id: rel.start_value})
                MATCH (end:AdOverallMetric {ad_id: rel.end_value})
                MERGE (start)-[r:HAS_OVERALL_METRICS]->(end)
                """
                logger.debug(f"Creating batch of {len(relationships)} HAS_OVERALL_METRICS relationships for Ad.")
                session.run(rel_query, {'relationships': relationships})
                
        logger.info("Completed AdOverallMetric transformation.")

def transform_ad_monthly_metrics(transformer: GraphTransformer, ad_legacy_df: pd.DataFrame):
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
    with transformer.driver.session() as session:
        for start_idx in range(0, len(monthly_agg), transformer.BATCH_SIZE):
            batch = monthly_agg.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
            monthly_metric_nodes = []
            relationships = []
            
            for _, row in batch.iterrows():
                node = {
                    'ad_id': row['ad_id'],
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
                    'start_key': 'ad_id',
                    'start_value': row['ad_id'],
                    'end_key': 'ad_id',
                    'end_value': row['ad_id'],
                    'month_key': 'month_start_date',
                    'month_value': row['month_start_date']
                })
            
            if monthly_metric_nodes:
                logger.debug(f"Creating batch of {len(monthly_metric_nodes)} AdMonthlyMetric nodes.")
                session.execute_write(transformer.create_entity_nodes_batch, 'AdMonthlyMetric', monthly_metric_nodes)
                
                rel_query = """
                UNWIND $relationships as rel
                MATCH (start:Ad {ad_id: rel.start_value})
                MATCH (end:AdMonthlyMetric {ad_id: rel.end_value, month_start_date: rel.month_value})
                MERGE (start)-[r:HAS_MONTHLY_METRICS]->(end)
                """
                logger.debug(f"Creating batch of {len(relationships)} HAS_MONTHLY_METRICS relationships for Ad.")
                session.run(rel_query, {'relationships': relationships})
    
    logger.info("Completed AdMonthlyMetric transformation.")

def transform_asset(transformer: GraphTransformer, asset_df: pd.DataFrame):
    """Transform asset data into graph format using batch processing"""
    with transformer.driver.session() as session:
        for start_idx in range(0, len(asset_df), transformer.BATCH_SIZE):
            batch = asset_df.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
            # Create asset nodes in batch
            nodes = [{
                'asset_id': row['asset_id'],
                'asset_type': row['asset_type'],
                'name': row['asset_name'],
                'file_hash': row.get('file_hash', '')
            } for _, row in batch.iterrows()]
            
            session.execute_write(transformer.create_entity_nodes_batch, 'Asset', nodes)
