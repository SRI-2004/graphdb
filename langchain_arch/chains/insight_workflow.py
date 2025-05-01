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
from ..agents.insight_generator import InsightGeneratorAgent
from ..utils.neo4j_utils import Neo4jDatabase
import os

class InsightWorkflow:
    """
    Orchestrates the insight generation workflow using astream_log.
    Yields RunLogPatch chunks from agents and custom status/error dicts.
    Gets final agent results via separate ainvoke calls after streaming.
    """
    def __init__(self, neo4j_db: Neo4jDatabase, schema_file: str):
        # Store DB connection and schema file path (may not be strictly needed anymore if agent loads schema)
        self.neo4j_db = neo4j_db
        self.schema_file = schema_file
        # self._schema_content = None # Removed as schema loading is likely internal to agent

        # Initialize the agent needed for this workflow.
        # The agent likely initializes its own LLM and loads schema internally.
        try:
            self.insight_query_gen_agent = InsightQueryGeneratorAgent()
            # TODO: Verify if InsightQueryGeneratorAgent needs any arguments.
        except Exception as e:
             # Handle initialization error appropriately
             print(f"CRITICAL: Failed to initialize InsightQueryGeneratorAgent in InsightWorkflow: {e}")
             raise RuntimeError(f"Failed to initialize agent: {e}") from e

    # Put schema loading back, it's needed for the agent's chain input
    def _load_schema(self) -> str:
        """Loads schema content using the provided Neo4jDatabase instance."""
        # Use the passed neo4j_db instance to load schema
        # No caching needed here as it's called once per run
        content = self.neo4j_db.get_schema_markdown(self.schema_file)
        if content is None:
            # Raise a more specific error if schema loading fails
            raise FileNotFoundError(f"Schema file '{self.schema_file}' could not be loaded by Neo4jDatabase.")
        return content

    def _convert_temporal_types(self, data: List[Dict]) -> List[Dict]:
        """Converts Neo4j temporal types in query results to ISO strings."""
        processed_data = []
        for record in data:
            processed_record = {}
            for key, value in record.items():
                if isinstance(value, (Date, DateTime, Time, date, datetime, time)):
                    processed_record[key] = value.isoformat()
                # Handle nested lists (e.g., from collect())
                elif isinstance(value, list):
                    processed_record[key] = [item.isoformat() if isinstance(item, (Date, DateTime, Time, date, datetime, time)) else item for item in value]
                else:
                    processed_record[key] = value
            processed_data.append(processed_record)
        return processed_data

    async def run(self, user_query: str) -> AsyncIterator[Union[RunLogPatch, Dict[str, Any]]]:
        yield {"type": "status", "step": "insight_workflow_start", "status": "in_progress"}
        generated_queries = []
        query_gen_final_data = None
        insight_gen_final_data = None

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
                 yield {"type": "final_insight", "summary": "Based on your query, no specific data retrieval is needed to provide an insight.", "results": [], "requires_execution": False}
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

            # REMOVED: Step 3: Execute Cypher Queries Concurrently 
            # REMOVED: Step 3.5: Pre-process results for JSON serialization
            # REMOVED: Step 4: Generate Insight using ainvoke

        except Exception as e:
            yield {"type": "error", "step": "workflow_exception", "message": f"Insight Workflow Error: {e}"}
            import traceback
            traceback.print_exc()
        finally:
            # Yield a workflow end status
            yield {"type": "status", "step": "insight_workflow_end", "status": "finished_generation"}

# Example usage (for testing)
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv

    load_dotenv()

    async def main():
        schema_f = os.path.join(os.path.dirname(__file__), '../../neo4j_schema.md')
        print(f"Schema path: {schema_f}")
        test_query = "What are the top 2 ad groups by clicks?"
        print(f"Test Query: {test_query}")

        try:
            with Neo4jDatabase() as db:
                workflow = InsightWorkflow(neo4j_db=db, schema_file=schema_f)
                print("\n--- Running Insight Workflow (RunLogPatch) ---")
                async for result_chunk in workflow.run(user_query=test_query):
                    if isinstance(result_chunk, RunLogPatch):
                         print(f"PATCH: run_id={result_chunk.run_id} ops={result_chunk.ops}")
                    elif isinstance(result_chunk, dict):
                         print(f"DICT: {json.dumps(result_chunk, indent=2)}")
                    else:
                         print(f"OTHER: {result_chunk}")
        except FileNotFoundError as fnf:
            print(f"Error: {fnf}")
        except ValueError as ve:
            print(f"Config Error: {ve}")
        except Exception as e:
            print(f"Workflow failed: {e}")
            import traceback
            traceback.print_exc()

    asyncio.run(main())
