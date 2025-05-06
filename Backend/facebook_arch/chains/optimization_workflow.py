import asyncio
import json
from typing import Dict, Any, AsyncIterator, List, Union, AsyncGenerator

# Import RunLogPatch instead of LogEntry
from langchain_core.tracers.log_stream import RunLogPatch
# Import the missing exception
from langchain_core.exceptions import OutputParserException

from ..agents.optimization_query_generator import OptimizationQueryGeneratorAgent
# Recommendation generator is no longer called here
# from ..agents.optimization_generator import OptimizationRecommendationGeneratorAgent
from ..utils.neo4j_utils import Neo4jDatabase # Still needed for schema loading
# Need LLM and os for the workaround
from langchain_openai import ChatOpenAI
import os

class OptimizationWorkflow:
    """
    Orchestrates the optimization query generation part of the workflow.
    Assumes query has been classified as relevant for optimization.
    Delegates execution and final recommendation generation to the caller.
    """
    # Removed neo4j_db from __init__
    def __init__(self, schema_file: str):
        self.schema_file = schema_file
        # Initialize the agent needed for this workflow.
        try:
            self.query_generator = OptimizationQueryGeneratorAgent()
        except Exception as e:
             print(f"CRITICAL: Failed to initialize OptimizationQueryGeneratorAgent in OptimizationWorkflow: {e}")
             raise RuntimeError(f"Failed to initialize agent: {e}") from e

    # Modified _load_schema to use Neo4jDatabase utility class internally
    def _load_schema(self) -> str:
        """Loads schema content using the Neo4jDatabase utility class."""
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

    # Renamed step names slightly for clarity
    async def run(self, user_query: str) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Runs the optimization query generation workflow: Load Schema -> Generate Queries.
        Yields status updates, reasoning, and finally the generated queries.
        Execution and recommendations are handled externally.
        """
        # Check if agent initialized correctly
        if not hasattr(self, 'query_generator') or self.query_generator is None:
            yield {"type": "error", "step": "workflow_init", "message": "OptimizationWorkflow could not start: Query Generator Agent failed to initialize."}
            return
            
        yield {"type": "status", "step": "opt_query_gen_start", "status": "in_progress"}
        query_gen_final_data = None

        try:
            # --- Step 1: Load Schema (Needed for Agent Input) ---
            yield {"type": "status", "step": "load_schema", "status": "in_progress", "details": "Loading schema for optimization..."}
            try:
                schema_content = self._load_schema()
                yield {"type": "status", "step": "load_schema", "status": "completed", "details": f"Schema loaded."}
            except Exception as e:
                 yield {"type": "error", "step": "load_schema", "message": f"Failed to load schema: {e}"}
                 return

            # --- Step 2: Generate Queries (Standardized Step Name) --- 
            yield {"type": "status", "step": "generate_queries", "status": "in_progress", "details": "Generating optimization queries..."}
            
            try:
                # Invoke query generator agent directly, passing query AND schema
                query_gen_final_data = await self.query_generator.chain.ainvoke({"query": user_query, "schema": schema_content})
            except Exception as qg_err:
                 yield {"type": "error", "step": "generate_queries", "status": "failed", "message": f"Failed to get optimization query generator result: {qg_err}"}
                 return

            # Validate the agent output
            if not isinstance(query_gen_final_data, dict) or "queries" not in query_gen_final_data:
                 yield {"type": "error", "step": "generate_queries", "status": "failed", "message": f"Optimization query generator returned invalid final output: {query_gen_final_data}"}
                 return
                 
            # Ensure the 'queries' field contains a list (could be empty)
            if not isinstance(query_gen_final_data["queries"], list):
                 yield {"type": "error", "step": "generate_queries", "status": "failed", "message": f"Optimization query generator 'queries' field is not a list: {query_gen_final_data['queries']}"}
                 return

            objectives_with_queries = query_gen_final_data["queries"] # This is List[{objective: str, query: str}]
            
            # If no queries generated, yield a specific status and potentially a final message
            if not objectives_with_queries:
                 yield {"type": "status", "step": "generate_queries", "status": "completed", "details": "Optimization analysis determined no specific queries are needed.", "generated_queries": []}
                 # Optionally yield a final message if the frontend needs one
                 # yield {"type": "final_recommendation", "summary": "Based on your query, no specific data retrieval is needed...", "requires_execution": False} # Caller handles this now
                 return
                 
            # Yield the generated queries list (already in the correct format)
            yield {
                "type": "status", 
                "step": "generate_queries", # Standardized step name
                "status": "completed", 
                "details": f"Generated {len(objectives_with_queries)} optimization queries.", 
                "generated_queries": objectives_with_queries # Yield the list of {objective:.., query:..}
            }
            
            # Yield reasoning if available
            if query_gen_final_data.get("reasoning"):
                 yield {
                     "type": "reasoning_summary", 
                     "step": "generate_queries", # Standardized step name
                     "reasoning": query_gen_final_data["reasoning"]
                 }

            # --- Workflow Ends Here ---
            print("OptimizationWorkflow finished: Queries generated.")

        except Exception as e:
            yield {"type": "error", "step": "workflow_exception", "message": f"Optimization Workflow Error: {e}"}
            import traceback
            traceback.print_exc()
        finally:
            # Yield a workflow end status (optional, depends on frontend needs)
            yield {"type": "status", "step": "opt_query_gen_end", "status": "finished"}

# Example usage removed/commented
# ...
