import asyncio
import json
from typing import Dict, Any, AsyncIterator, List, Union
from langchain_core.exceptions import OutputParserException
# Import neo4j time types and standard datetime
from neo4j.time import Date, DateTime, Time 
from datetime import date, datetime, time

from langchain_core.tracers.log_stream import RunLogPatch

from ..agents.insight_query_generator import InsightQueryGeneratorAgent
from ..agents.insight_generator import InsightGeneratorAgent
from ..utils.neo4j_utils import Neo4jDatabase

class InsightWorkflow:
    """
    Orchestrates the insight generation workflow using astream_log.
    Yields RunLogPatch chunks from agents and custom status/error dicts.
    Gets final agent results via separate ainvoke calls after streaming.
    """
    def __init__(self, neo4j_db: Neo4jDatabase, schema_file: str = "neo4j_schema.md"):
        self.query_generator = InsightQueryGeneratorAgent()
        self.insight_generator = InsightGeneratorAgent()
        self.neo4j_db = neo4j_db # Passed from Router
        self.schema_file = schema_file
        self._schema_content = None

    def _load_schema(self) -> str:
        if self._schema_content is None:
            content = self.neo4j_db.get_schema_markdown(self.schema_file)
            if content is None:
                raise FileNotFoundError(f"Schema file '{self.schema_file}' not found.")
            self._schema_content = content
        return self._schema_content

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
            # --- Step 1: Load Schema --- 
            yield {"type": "status", "step": "load_schema", "status": "in_progress", "details": "Loading schema..."}
            try:
                schema = self._load_schema()
                yield {"type": "status", "step": "load_schema", "status": "completed", "details": f"Schema loaded."}
            except Exception as e:
                 yield {"type": "error", "step": "load_schema", "message": f"Failed to load schema: {e}"}
                 return

            # --- Step 2: Generate Cypher using ainvoke --- 
            yield {"type": "status", "step": "generate_cypher", "status": "in_progress", "details": "Generating Cypher query(s)..."}
            
            try:
                # Invoke directly to get final result
                query_gen_final_data = await self.query_generator.chain.ainvoke({"query": user_query, "schema": schema})
            except OutputParserException as ope:
                 yield {"type": "error", "step": "generate_cypher", "status": "failed", "message": f"Failed to parse query generator output: {ope}"}
                 return
            except Exception as qg_err:
                 yield {"type": "error", "step": "generate_cypher", "status": "failed", "message": f"Failed to get query generator result: {qg_err}"}
                 return

            if not isinstance(query_gen_final_data, dict) or "queries" not in query_gen_final_data:
                 yield {"type": "error", "step": "generate_cypher", "status": "failed", "message": f"Query generator returned invalid final output format: {query_gen_final_data}"}
                 return

            generated_queries = query_gen_final_data["queries"]
            # Extract query generation reasoning
            query_generation_reasoning = query_gen_final_data.get("reasoning", "N/A") # Get reasoning, provide default
            
            yield {"type": "status", "step": "generate_cypher", "status": "completed", "details": f"Generated {len(generated_queries)} Cypher query(s).", "generated_queries": generated_queries}
            # Yield reasoning directly from the ainvoke result
            if query_generation_reasoning != "N/A": # Yield only if reasoning exists
                 yield {"type": "reasoning_summary", "step": "generate_cypher", "reasoning": query_generation_reasoning}

            # --- Step 3: Execute Cypher Queries Concurrently --- 
            yield {"type": "status", "step": "execute_cypher", "status": "in_progress", "details": f"Preparing to execute {len(generated_queries)} Cypher query(s) concurrently..."}
            all_results_combined = []
            has_error = False
            error_message = ""

            async def execute_single_query(query: str, index: int) -> Union[List[Dict], Exception]:
                """Helper coroutine to run a single query and return result or exception."""
                # This function no longer yields status. Status is handled after gather.
                loop = asyncio.get_running_loop()
                try:
                    # Run Neo4j query in a thread pool executor
                    results = await loop.run_in_executor(None, self.neo4j_db.query, query, None)
                    return results
                except Exception as e:
                    # Return the exception object itself to be handled by gather
                    print(f"Error in execute_single_query {index}: {e}") # Add logging
                    return e

            # Yield status *before* creating tasks
            yield {"type": "status", "step": "execute_cypher", "status": "in_progress", "details": f"Executing {len(generated_queries)} queries concurrently..."}

            # Create tasks for all queries
            tasks = [execute_single_query(query, i) for i, query in enumerate(generated_queries)]
            
            # Use an inner async generator to yield status updates AFTER gathering results
            async def gather_and_yield(tasks_to_run):
                nonlocal all_results_combined, has_error, error_message
                try:
                    # Gather results or exceptions
                    results_list = await asyncio.gather(*tasks_to_run)
                    
                    # Process results and yield status/errors
                    for i, result in enumerate(results_list):
                        if isinstance(result, Exception):
                            has_error = True
                            error_message = f"Error executing Cypher query {i+1}: {result}"
                            yield {"type": "error", "step": "execute_cypher", "message": error_message, "query": generated_queries[i], "query_index": i}
                            # Decide whether to break or continue gathering results from other queries
                            # break # Uncomment to stop on first error
                        elif isinstance(result, list):
                            # Successfully got results list
                            all_results_combined.extend(result)
                            yield {"type": "status", "step": "execute_cypher", "status": "partial_complete", "details": f"Query {i+1} finished, {len(result)} results.", "query_index": i}
                        else:
                            # Handle unexpected return type
                            has_error = True
                            error_message = f"Unexpected result type for query {i+1}: {type(result)}"
                            yield {"type": "error", "step": "execute_cypher", "message": error_message, "query": generated_queries[i], "query_index": i}

                except Exception as gather_err:
                    # Catch potential errors during gather itself 
                    has_error = True
                    error_message = f"Error during concurrent query execution: {gather_err}"
                    yield {"type": "error", "step": "execute_cypher", "message": error_message}

            # Run the gather_and_yield generator
            async for status_update in gather_and_yield(tasks):
                 yield status_update # Propagate status/error updates from within gather

            # Check if any error occurred during execution
            if has_error:
                 yield {"type": "status", "step": "execute_cypher", "status": "failed", "details": f"Concurrent execution failed. {error_message}"}
                 return
                 
            yield {"type": "status", "step": "execute_cypher", "status": "completed", "details": f"All {len(generated_queries)} queries executed concurrently.", "result_count": len(all_results_combined)}

            # --- Step 3.5: Pre-process results for JSON serialization --- 
            yield {"type": "status", "step": "process_results", "status": "in_progress", "details": "Converting temporal types in results..."}
            try:
                processed_data = self._convert_temporal_types(all_results_combined)
                yield {"type": "status", "step": "process_results", "status": "completed", "details": "Temporal types converted."}
            except Exception as proc_err:
                yield {"type": "error", "step": "process_results", "message": f"Failed to process query results: {proc_err}"}
                return

            # --- Step 4: Generate Insight using ainvoke --- 
            yield {"type": "status", "step": "generate_insight", "status": "in_progress", "details": "Generating insight..."}
            insight_gen_final_data = None # Initialize
            raw_llm_output = None # To store the AIMessage
            
            try:
            
                # Include query generation reasoning in the input
                insight_input = {
                    "query": user_query, 
                    "data": processed_data, 
                    "query_generation_reasoning": query_generation_reasoning
                } 
                
                # Invoke the chain, this returns the AIMessage object
                raw_llm_output = await self.insight_generator.chain.ainvoke(insight_input)
                
                # Explicitly parse the content of the message using the agent's parser
                yield {"type": "status", "step": "generate_insight", "status": "in_progress", "details": "Parsing insight generator output..."}
                # Ensure the agent has the 'output_parser' attribute defined in its __init__
                if hasattr(self.insight_generator, 'output_parser') and callable(getattr(self.insight_generator.output_parser, 'parse', None)):
                    insight_gen_final_data = self.insight_generator.output_parser.parse(raw_llm_output.content)
                else:
                    # Fallback or raise error if parser is missing
                    yield {"type": "error", "step": "generate_insight", "status": "failed", "message": "InsightGeneratorAgent is missing the output_parser attribute."}
                    return 

            except OutputParserException as ope:
                # Handle parsing errors
                yield {"type": "error", "step": "generate_insight", "status": "failed", "message": f"Failed to parse insight generator output: {ope}"}
                return
            except Exception as ig_err:
                # Catch other errors during ainvoke or parsing
                yield {"type": "error", "step": "generate_insight", "status": "failed", "message": f"Failed during insight generation or parsing: {ig_err}"}
                return

            # Check the parsed data
            if not isinstance(insight_gen_final_data, dict) or "insight" not in insight_gen_final_data:
                 yield {"type": "error", "step": "generate_insight", "status": "failed", "message": f"Insight generator returned invalid final output format after parsing: {insight_gen_final_data}"}
                 return

            # Yield the final parsed dictionary
            yield {"type": "final_insight", "step": "generate_insight", "status": "completed", **insight_gen_final_data}

        except Exception as e:
            yield {"type": "error", "step": "workflow_exception", "message": f"Insight Workflow Error: {e}"}
            import traceback
            traceback.print_exc()
        finally:
            # Yield a workflow end status
            yield {"type": "status", "step": "insight_workflow_end", "status": "finished"}

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
