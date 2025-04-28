import pandas as pd
import json
import logging
from typing import Dict, List, Any
from ..graph_base import GraphTransformer

logger = logging.getLogger(__name__)

def transform_adaccount(transformer: GraphTransformer, account_df: pd.DataFrame):
    """Transform ad account data using batch processing"""
    # Remove duplicates from DataFrame if any exist
    account_df = account_df.drop_duplicates(subset=['customer_id'])
    
    logger.info(f"Starting batch AdAccount transformation for {len(account_df)} unique accounts")
    with transformer.driver.session() as session:
        # Process in batches
        for start_idx in range(0, len(account_df), transformer.BATCH_SIZE):
            batch = account_df.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
            session.execute_write(transformer.create_entity_nodes_batch, 'AdAccount', nodes)
            logger.info(f"Processed {len(nodes)} AdAccount nodes")
    
    logger.info("Completed AdAccount transformation")

def transform_account_monthly_metrics(transformer: GraphTransformer, account_perf_df: pd.DataFrame):
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
    with transformer.driver.session() as session:
        for start_idx in range(0, len(monthly_agg), transformer.BATCH_SIZE):
            batch = monthly_agg.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
                session.execute_write(transformer.create_entity_nodes_batch, 'AccountMonthlyMetric', monthly_metric_nodes)

                rel_query = """
                UNWIND $relationships as rel
                MATCH (start:AdAccount {account_id: rel.start_value})
                MATCH (end:AccountMonthlyMetric {account_id: rel.end_value, month_start_date: rel.month_value})
                MERGE (start)-[r:HAS_MONTHLY_METRICS]->(end)
                """
                logger.debug(f"Creating batch of {len(relationships)} HAS_MONTHLY_METRICS relationships for Account.")
                session.run(rel_query, {'relationships': relationships})
                
        logger.info("Completed AccountMonthlyMetric transformation.")

def transform_account_overall_metrics(transformer: GraphTransformer, account_perf_df: pd.DataFrame):
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
    with transformer.driver.session() as session:
        for start_idx in range(0, len(overall_agg), transformer.BATCH_SIZE):
            batch = overall_agg.iloc[start_idx:start_idx + transformer.BATCH_SIZE]
            
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
                session.execute_write(transformer.create_entity_nodes_batch, 'AccountOverallMetric', overall_metric_nodes)

                rel_query = """
                UNWIND $relationships as rel
                MATCH (start:AdAccount {account_id: rel.start_value})
                MATCH (end:AccountOverallMetric {account_id: rel.end_value})
                MERGE (start)-[r:HAS_OVERALL_METRICS]->(end)
                """
                logger.debug(f"Creating batch of {len(relationships)} HAS_OVERALL_METRICS relationships for Account.")
                session.run(rel_query, {'relationships': relationships})
                
        logger.info("Completed AccountOverallMetric transformation.")

def transform_conversion_action(transformer: GraphTransformer, conversion_df: pd.DataFrame):
    """Transform conversion action data into graph format using batch processing"""
    with transformer.driver.session() as session:
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
            session.execute_write(transformer.create_entity_nodes_batch, 'ConversionAction', [node])
            
            # Create relationship to AdAccount
            relationships = [{
                'start_key': 'account_id',
                'start_value': account_id,
                'end_key': 'account_id',
                'end_value': account_id
            }]
            
            # Create relationship in batch
            session.execute_write(
                transformer.create_relationships_batch,
                'AdAccount',
                'ConversionAction',
                'HAS_CONVERSION_ACTIONS',
                relationships
            )
