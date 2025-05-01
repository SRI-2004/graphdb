import asyncio
import json
from typing import Dict, Any, AsyncIterator, List, Union
from langchain_core.exceptions import OutputParserException
# Import neo4j time types and standard datetime
from neo4j.time import Date, DateTime, Time 
from datetime import date, datetime, time

from langchain_core.tracers.log_stream import RunLogPatch
from langchain_openai import ChatOpenAI

from ..agents.insight_query_generator import InsightQueryGeneratorAgent
# InsightGeneratorAgent is called externally now
# from ..agents.insight_generator import InsightGeneratorAgent 
from ..utils.neo4j_utils import Neo4jDatabase # Import the utility class
import os

class InsightWorkflow:
    """
    Orchestrates the insight query generation workflow.
    Assumes query has been classified as relevant for insight.
    Delegates execution and final insight generation to the caller.
    """
    # Removed neo4j_db from __init__
    def __init__(self, schema_file: str):
        self.schema_file = schema_file
        # Initialize the agent needed for this workflow.
        try:
            self.insight_query_gen_agent = InsightQueryGeneratorAgent()
        except Exception as e:
             print(f"CRITICAL: Failed to initialize InsightQueryGeneratorAgent in InsightWorkflow: {e}")
             raise RuntimeError(f"Failed to initialize agent: {e}") from e

    # Modified _load_schema to use Neo4jDatabase utility class internally
    def _load_schema(self) -> str:
        """Loads schema content using the Neo4jDatabase utility class."""
        # Use a context manager if Neo4jDatabase supports it, otherwise create/close
        # Assuming Neo4jDatabase handles its own driver connection/closing
        try:
            # Create an instance of the utility class
            db_utils = Neo4jDatabase()
            content = db_utils.get_schema_markdown(self.schema_file)
        except Exception as db_e:
            print(f"Error during schema loading via Neo4jDatabase: {db_e}")
            raise RuntimeError(f"Failed to load schema via Neo4jDatabase: {db_e}") from db_e
        finally:
            # Close the connection if Neo4jDatabase doesn't use context manager
            if 'db_utils' in locals() and hasattr(db_utils, 'close'):
                 try:
                      db_utils.close()
                 except Exception as close_e:
                      print(f"Error closing Neo4jDatabase instance: {close_e}")
                      
        if content is None:
            raise FileNotFoundError(f"Schema file '{self.schema_file}' could not be loaded by Neo4jDatabase.")
        return content

    # Removed _convert_temporal_types as execution/results handled externally

    # Renamed step names slightly for clarity
    async def run(self, user_query: str) -> AsyncIterator[Union[RunLogPatch, Dict[str, Any]]]:
        yield {"type": "status", "step": "insight_query_gen_start", "status": "in_progress"}
        generated_queries = []
        query_gen_final_data = None
        # insight_gen_final_data = None # Removed

        try:
            # --- Step 1: Load Schema (Needed for Agent Input) ---
            yield {"type": "status", "step": "load_schema", "status": "in_progress", "details": "Loading schema for insight query generation..."}
            try:
                schema_content = self._load_schema()
                yield {"type": "status", "step": "load_schema", "status": "completed", "details": "Schema loaded."}
            except Exception as e:
                 yield {"type": "error", "step": "load_schema", "message": f"Failed to load schema: {e}"}
                 return

            # --- Step 2: Generate Queries (Standardized Name) ---
            yield {"type": "status", "step": "generate_queries", "status": "in_progress", "details": "Generating Cypher query(s)..."}
            
            try:
                # Invoke the agent's chain. 
                # Pass the user query AND the loaded schema.
                query_gen_final_data = await self.insight_query_gen_agent.chain.ainvoke({"query": user_query, "schema": schema_content})
            except OutputParserException as ope:
                 # Use standardized step name in error
                 yield {"type": "error", "step": "generate_queries", "status": "failed", "message": f"Failed to parse query generator output: {ope}"}
                 return
            except Exception as qg_err:
                 # Use standardized step name in error
                 yield {"type": "error", "step": "generate_queries", "status": "failed", "message": f"Failed to get query generator result: {qg_err}"}
                 return

            # Validate output structure
            if not isinstance(query_gen_final_data, dict) or "queries" not in query_gen_final_data:
                 yield {"type": "error", "step": "generate_queries", "status": "failed", "message": f"Query generator returned invalid final output format: {query_gen_final_data}"}
                 return

            generated_queries_list = query_gen_final_data.get("queries", [])
            
            # Handle case where no queries are generated
            if not generated_queries_list:
                 yield {"type": "status", "step": "generate_queries", "status": "completed", "details": "Insight generation determined no specific Cypher queries are required for this query.", "generated_queries": []}
                 # Send a final message indicating this. 
                 # yield {"type": "final_insight", "summary": "Based on your query, no specific data retrieval is needed to provide an insight.", "results": [], "requires_execution": False} # Caller handles this now
                 return

            # Map the list of strings to the expected frontend format
            generated_queries_for_frontend = [
                {"objective": f"Insight Query {i+1}", "query": q}
                for i, q in enumerate(generated_queries_list)
            ]
            # Extract query generation reasoning
            query_generation_reasoning = query_gen_final_data.get("reasoning", "N/A")
            
            # Yield generated queries with standardized step name
            yield {
                "type": "status", 
                "step": "generate_queries", # Standardized step name
                "status": "completed", 
                "details": f"Generated {len(generated_queries_for_frontend)} Cypher query(s).", 
                "generated_queries": generated_queries_for_frontend
            }
            
            # Yield reasoning with standardized step name
            if query_generation_reasoning != "N/A":
                 yield {
                    "type": "reasoning_summary", 
                    "step": "generate_queries", # Standardized step name
                    "reasoning": query_generation_reasoning
                 }

            # --- Workflow Ends Here (as per standardization) ---
            print("InsightWorkflow finished: Queries generated.")

        except Exception as e:
            yield {"type": "error", "step": "workflow_exception", "message": f"Insight Workflow Error: {e}"}
            import traceback
            traceback.print_exc()
        finally:
            # Yield a workflow end status
            yield {"type": "status", "step": "insight_query_gen_end", "status": "finished"}

# Example usage requires update if Neo4jDatabase needs env vars
# ... (example usage commented out or removed) ...
