import pandas as pd
import json
import logging
from typing import Dict, List, Any
from ..graph_base import GraphTransformer

logger = logging.getLogger(__name__)

def transform_audience(transformer: GraphTransformer, audience_df: pd.DataFrame):
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
        
    with transformer.driver.session() as session:
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
                'audience_id': audience_id,
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
                'end_key': 'audience_id',
                'end_value': audience_id
            })

            # 3. Parse dimensions and create component nodes/relationships
            dimensions_value = row.get('audience_dimensions')
            dimensions_to_parse = []

            # Handle list of strings or single string
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

                # Audience Segments
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

        # Execute batch writes for all node types
        if audience_nodes_batch:
            logger.debug(f"Creating {len(audience_nodes_batch)} Audience nodes batch.")
            session.execute_write(transformer.create_entity_nodes_batch, 'Audience', audience_nodes_batch)
        
        # Create relationships between AdAccounts and Audiences
        if adaccount_relationships_batch:
            logger.debug(f"Creating {len(adaccount_relationships_batch)} AdAccount->Audience relationships batch.")
            session.execute_write(
                transformer.create_relationships_batch,
                'AdAccount',
                'Audience',
                'TARGETS',
                adaccount_relationships_batch
            )
            
        # Process AgeRange nodes and relationships
        if age_range_nodes_batch:
            logger.debug(f"Creating {len(age_range_nodes_batch)} AgeRange nodes batch.")
            session.execute_write(transformer.create_entity_nodes_batch, 'AgeRange', age_range_nodes_batch)
            
            # Create relationships between Audiences and AgeRanges
            rel_query = """
            UNWIND $relationships AS rel
            MATCH (a:Audience {audience_id: rel.audience_id})
            MATCH (r:AgeRange {minAge: rel.min_age, maxAge: rel.max_age})
            MERGE (a)-[rel:TARGETS_AGE]->(r)
            """
            logger.debug(f"Creating {len(age_range_rels_batch)} Audience->AgeRange relationships batch.")
            session.run(rel_query, {'relationships': age_range_rels_batch})
            
        # Process Gender nodes and relationships
        if gender_nodes_batch:
            logger.debug(f"Creating {len(gender_nodes_batch)} Gender nodes batch.")
            session.execute_write(transformer.create_entity_nodes_batch, 'Gender', gender_nodes_batch)
            
            # Create relationships between Audiences and Genders
            rel_query = """
            UNWIND $relationships AS rel
            MATCH (a:Audience {audience_id: rel.audience_id})
            MATCH (g:Gender {genderType: rel.gender_type})
            MERGE (a)-[rel:TARGETS_GENDER]->(g)
            """
            logger.debug(f"Creating {len(gender_rels_batch)} Audience->Gender relationships batch.")
            session.run(rel_query, {'relationships': gender_rels_batch})
            
        # Process UserInterest nodes and relationships
        if user_interest_nodes_batch:
            logger.debug(f"Creating {len(user_interest_nodes_batch)} UserInterest nodes batch.")
            session.execute_write(transformer.create_entity_nodes_batch, 'UserInterest', user_interest_nodes_batch)
            
            # Create relationships between Audiences and UserInterests
            rel_query = """
            UNWIND $relationships AS rel
            MATCH (a:Audience {audience_id: rel.audience_id})
            MATCH (i:UserInterest {criterionId: rel.criterion_id})
            MERGE (a)-[rel:TARGETS_INTEREST]->(i)
            """
            logger.debug(f"Creating {len(user_interest_rels_batch)} Audience->UserInterest relationships batch.")
            session.run(rel_query, {'relationships': user_interest_rels_batch})
            
        # Process CustomAudience nodes and relationships
        if custom_audience_nodes_batch:
            logger.debug(f"Creating {len(custom_audience_nodes_batch)} CustomAudience nodes batch.")
            session.execute_write(transformer.create_entity_nodes_batch, 'CustomAudience', custom_audience_nodes_batch)
            
            # Create relationships between Audiences and CustomAudiences
            rel_query = """
            UNWIND $relationships AS rel
            MATCH (a:Audience {audience_id: rel.audience_id})
            MATCH (c:CustomAudience {customAudienceId: rel.custom_audience_id})
            MERGE (a)-[rel:INCLUDES_CUSTOM_AUDIENCE]->(c)
            """
            logger.debug(f"Creating {len(custom_audience_rels_batch)} Audience->CustomAudience relationships batch.")
            session.run(rel_query, {'relationships': custom_audience_rels_batch})
            
    logger.info(f"Completed Audience transformation - created {len(audience_nodes_batch)} audience nodes with their components and relationships.")
