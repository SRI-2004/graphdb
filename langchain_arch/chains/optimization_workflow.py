import asyncio
import json
from typing import Dict, Any, AsyncIterator, List, Union

# Import RunLogPatch instead of LogEntry
from langchain_core.tracers.log_stream import RunLogPatch
# Import the missing exception
from langchain_core.exceptions import OutputParserException

from ..agents.optimization_query_generator import OptimizationQueryGeneratorAgent
from ..agents.optimization_generator import OptimizationRecommendationGeneratorAgent
from ..utils.neo4j_utils import Neo4jDatabase

class OptimizationWorkflow:
    """
    Orchestrates the optimization recommendation workflow using astream_log.
    Yields RunLogPatch chunks from agents and custom status/error dicts.
    Gets final agent results via separate ainvoke calls after streaming.
    """
    def __init__(self, neo4j_db: Neo4jDatabase, schema_file: str = "neo4j_schema.md"):
        self.query_generator = OptimizationQueryGeneratorAgent()
        self.recommendation_generator = OptimizationRecommendationGeneratorAgent()
        self.neo4j_db = neo4j_db
        self.schema_file = schema_file
        self._schema_content = None

    def _load_schema(self) -> str:
        if self._schema_content is None:
            content = self.neo4j_db.get_schema_markdown(self.schema_file)
            if content is None:
                raise FileNotFoundError(f"Schema file '{self.schema_file}' not found.")
            self._schema_content = content
        return self._schema_content

    async def _execute_query_async(self, objective: str, cypher_query: str) -> Dict[str, Any]:
        try:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, self.neo4j_db.query, cypher_query, None)
            return {"objective": objective, "query": cypher_query, "results": results, "status": "success"}
        except Exception as e:
            print(f"Error executing query for objective '{objective}': {e}\nQuery: {cypher_query}")
            return {"objective": objective, "query": cypher_query, "error": str(e), "status": "error"}

    async def run(self, user_query: str) -> AsyncIterator[Union[RunLogPatch, Dict[str, Any]]]:
        yield {"type": "status", "step": "opt_workflow_start", "status": "in_progress"}
        objectives_with_queries = []
        query_gen_final_data = None
        reco_gen_final_data = None

        try:
            # --- Step 1: Load Schema --- 
            yield {"type": "status", "step": "load_schema", "status": "in_progress", "details": "Loading schema..."}
            try:
                schema = self._load_schema()
                yield {"type": "status", "step": "load_schema", "status": "completed", "details": f"Schema loaded."}
            except Exception as e:
                 yield {"type": "error", "step": "load_schema", "message": f"Failed to load schema: {e}"}; return

            # --- Step 2: Generate Opt Queries using ainvoke --- 
            yield {"type": "status", "step": "generate_opt_queries", "status": "in_progress", "details": "Generating optimization queries..."}
            
            try:
                # Invoke directly to get final result
                query_gen_final_data = await self.query_generator.chain.ainvoke({"query": user_query, "schema": schema})
            except Exception as qg_err:
                 yield {"type": "error", "step": "generate_opt_queries", "status": "failed", "message": f"Failed to get opt query generator result: {qg_err}"}; return

            if not isinstance(query_gen_final_data, dict) or "queries" not in query_gen_final_data:
                 yield {"type": "error", "step": "generate_opt_queries", "status": "failed", "message": f"Opt query generator returned invalid final output: {query_gen_final_data}"}; return

            objectives_with_queries = query_gen_final_data["queries"]
            yield {"type": "status", "step": "generate_opt_queries", "status": "completed", "details": f"Generated {len(objectives_with_queries)} optimization queries.", "generated_queries": objectives_with_queries}
            if query_gen_final_data.get("reasoning"):
                 yield {"type": "reasoning_summary", "step": "generate_opt_queries", "reasoning": query_gen_final_data["reasoning"]}

            # --- Step 3: Execute Optimization Queries Concurrently using asyncio.gather --- 
            num_queries = len(objectives_with_queries)
            yield {"type": "status", "step": "execute_opt_queries", "status": "in_progress", "details": f"Preparing to execute {num_queries} optimization queries concurrently..."}
            
            combined_query_results = {} # Store results keyed by objective
            has_error = False
            error_message = ""

            async def execute_single_opt_query(query_item: dict, index: int) -> Union[Dict, Exception]:
                """Helper coroutine to run a single opt query and return result dict or exception."""
                objective = query_item.get("objective", f"Unknown Objective {index+1}")
                query = query_item.get("query", "")
                if not query:
                    # Return an error-like dict immediately for missing query
                    return {"objective": objective, "error": "Missing query text", "status": "error"} 
                
                # No yield here
                loop = asyncio.get_running_loop()
                try:
                    # Run Neo4j query in a thread pool executor
                    results = await loop.run_in_executor(None, self.neo4j_db.query, query, None)
                    # Return success dict
                    return {"objective": objective, "results": results, "status": "success"}
                except Exception as e:
                    # Return the exception object itself
                    print(f"Error in execute_single_opt_query {index} ('{objective}'): {e}") # Add logging
                    return e

            # Yield status *before* creating tasks
            yield {"type": "status", "step": "execute_opt_queries", "status": "in_progress", "details": f"Executing {num_queries} optimization queries concurrently..."}

            # Create tasks, ensuring items are valid dicts with 'query'
            tasks = [
                execute_single_opt_query(item, i) 
                for i, item in enumerate(objectives_with_queries) 
                if isinstance(item, dict) and "query" in item
            ]
            
            if len(tasks) != num_queries:
                 yield {"type": "status", "step": "execute_opt_queries", "status": "warning", "details": f"Filtered out {num_queries - len(tasks)} invalid items from generated queries list."}
                 num_queries = len(tasks)
                 if num_queries == 0:
                      yield {"type": "error", "step": "execute_opt_queries", "message": "No valid queries found to execute after filtering."}; return

            # Use an inner async generator to yield status updates AFTER gathering results
            async def gather_and_yield(tasks_to_run):
                nonlocal combined_query_results, has_error, error_message
                try:
                    results_list = await asyncio.gather(*tasks_to_run)
                    
                    for i, result_or_exc in enumerate(results_list):
                        # Determine original query item index if filtering occurred (tricky, assume order for now)
                        original_item_index = i # Placeholder - refine if filtering is complex
                        query_item = objectives_with_queries[original_item_index] # Get corresponding query info
                        objective = query_item.get("objective", f"Unknown Objective {original_item_index+1}")
                        query_text = query_item.get("query", "N/A")

                        if isinstance(result_or_exc, Exception):
                            # Exception from execute_single_opt_query
                            has_error = True
                            error_message = f"Query '{objective}' FAILED: {result_or_exc}"
                            yield {"type": "error", "step": "execute_opt_queries", "objective": objective, "message": error_message, "query": query_text, "query_index": original_item_index}
                            combined_query_results[objective] = [] # Store empty for failed objective
                        elif isinstance(result_or_exc, dict):
                            # Dict returned by execute_single_opt_query
                            status = result_or_exc.get("status")
                            if status == "error":
                                has_error = True
                                err_detail = result_or_exc.get("error", "Unknown execution error")
                                error_message = f"Query '{objective}' failed: {err_detail}" 
                                yield {"type": "error", "step": "execute_opt_queries", "objective": objective, "message": error_message, "query": query_text, "query_index": original_item_index}
                                combined_query_results[objective] = [] # Store empty for failed objective
                            elif status == "success":
                                results = result_or_exc.get("results", [])
                                combined_query_results[objective] = results
                                yield {"type": "status", "step": "execute_opt_queries", "status": "partial_complete", "objective": objective, "details": f"Query '{objective}' finished, {len(results)} results.", "query_index": original_item_index}
                            else:
                                # Handle unexpected dict status
                                has_error = True;
                                error_message = f"Unexpected status '{status}' for query '{objective}'"
                                yield {"type": "error", "step": "execute_opt_queries", "objective": objective, "message": error_message, "query": query_text, "query_index": original_item_index}
                                combined_query_results[objective] = []
                        else:
                            # Handle unexpected return type from gather
                            has_error = True;
                            error_message = f"Unexpected result type for query task {original_item_index+1}: {type(result_or_exc)}"
                            yield {"type": "error", "step": "execute_opt_queries", "objective": objective, "message": error_message, "query": query_text, "query_index": original_item_index}
                            combined_query_results[objective] = []

                except Exception as gather_err:
                    has_error = True
                    error_message = f"Error during concurrent query execution: {gather_err}"
                    yield {"type": "error", "step": "execute_opt_queries", "message": error_message}

            # Run the gather_and_yield generator
            async for status_update in gather_and_yield(tasks):
                 yield status_update # Propagate status/error updates

            # Check if any error occurred during execution
            if has_error:
                 final_detail = f"Concurrent execution finished. {error_message}"
                 yield {"type": "status", "step": "execute_opt_queries", "status": "failed", "details": final_detail}
                 # Depending on requirements, you might want to return here or continue to recommendations with partial data
                 # return # Uncomment to stop workflow on query error
            else:     
                final_detail = f"All {num_queries} optimization queries executed concurrently."
                yield {"type": "status", "step": "execute_opt_queries", "status": "completed", "details": final_detail, "result_summary": {k: len(v) for k, v in combined_query_results.items()}}

            # --- Step 4: Generate Recommendations using ainvoke --- 
            yield {"type": "status", "step": "generate_recommendations", "status": "in_progress", "details": "Generating recommendations..."}
            reco_gen_final_data = None # Initialize
            
            try:
                reco_input = {"query": user_query, "data": combined_query_results}
                # Invoke the chain directly. Since JsonOutputParser is the last step
                # in the agent's chain, this returns the already parsed dictionary.
                reco_gen_final_data = await self.recommendation_generator.chain.ainvoke(reco_input)

            except Exception as rg_err:
                # Catch errors during the ainvoke call (LLM call or parsing within the chain)
                yield {"type": "error", "step": "generate_recommendations", "status": "failed", "message": f"Failed during recommendation generation: {rg_err}"}
                return

            # Check for the correct key based on the prompt: "optimization_report"
            if not isinstance(reco_gen_final_data, dict) or "optimization_report" not in reco_gen_final_data:
                 yield {"type": "error", "step": "generate_recommendations", "status": "failed", "message": f"Recommendation generator returned invalid final output format (expected 'optimization_report'): {reco_gen_final_data}"}; return

            # Yield final result, passing the report content and reasoning
            yield {
                "type": "final_recommendations", 
                "step": "generate_recommendations", 
                "status": "completed", 
                "report": reco_gen_final_data.get("optimization_report", ""), # Pass report content
                "reasoning": reco_gen_final_data.get("reasoning", "") # Pass reasoning
            }

        except Exception as e:
            yield {"type": "error", "step": "workflow_exception", "message": f"Optimization Workflow Error: {e}"}
            import traceback
            traceback.print_exc()
        finally:
            yield {"type": "status", "step": "opt_workflow_end", "status": "finished"}

# ... (Example usage needs update) ...
