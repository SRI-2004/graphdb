import asyncio
import json
from typing import Dict, Any, AsyncIterator, Union, Optional
from collections.abc import Mapping

from langchain_core.tracers.log_stream import RunLogPatch, LogEntry

from .insight_workflow import InsightWorkflow
from .optimization_workflow import OptimizationWorkflow
from ..agents.classifier import ClassifierAgent
from ..utils.neo4j_utils import Neo4jDatabase

class Router:
    """
    Top-level router using astream_log.
    Classifies query and routes to the appropriate workflow,
    streaming RunLogPatch objects and custom status dicts.
    Gets final agent results via separate ainvoke calls after streaming.
    """
    def __init__(self, schema_file: str = "neo4j_schema.md"):
        # Initialize DB connection per Router instance.
        # Ensures connection is managed if Router is long-lived.
        self._db_connection = None
        self.schema_file = schema_file
        self.classifier = ClassifierAgent()
        # Workflow instantiation moved to run() to ensure they get the active DB connection

    def _get_db(self):
        """Creates or returns the active DB connection for this router instance."""
        if self._db_connection is None:
            # Add error handling for DB connection failure if needed
            self._db_connection = Neo4jDatabase()
        return self._db_connection

    def _close_db(self):
        """Closes the DB connection if it exists."""
        if self._db_connection:
            try:
                self._db_connection.close()
                # print("Router: DB connection closed.")
            except Exception as e:
                print(f"Router: Error closing DB: {e}")
            finally:
                self._db_connection = None

    async def run(self, user_query: str, user_id: str) -> AsyncIterator[Union[RunLogPatch, Dict[str, Any]]]:
        """
        Runs classification, gets the result, then routes to the selected workflow 
        and streams its RunLogPatch and status dicts.
        Manages Neo4j connection lifecycle for the run.
        """
        yield {"type": "status", "step": "start_router", "status": "in_progress", "details": "Initializing..."}
        print(f"DEBUG Router received user_query: {user_query!r}")
        classification_output: Optional[Dict[str, Any]] = None
        db = self._get_db()

        try:
            # --- Step 1: Call Classifier Agent --- 
            yield {"type": "status", "step": "classify_query", "status": "in_progress", "details": "Classifying query..."}

            try:
                # Call the classifier agent's run method directly and await the result
                classification_output = await self.classifier.run(query=user_query, user_id=user_id)
                # No stream processing loop needed here anymore
                                        
            except Exception as class_err:
                 # Capture error from the classifier agent's execution
                 print(f"Router: Error during classifier agent execution: {class_err}")
                 yield {"type": "error", "step": "classify_query", "status": "failed", "message": f"Failed during classifier agent execution: {class_err}"}
                 self._close_db()
                 return # Stop processing if classification fails

            # --- Step 1b: Validate Classifier Output --- 
            if classification_output is None:
                 # This means classifier.run() returned None, likely due to an internal error (e.g., JSON parsing)
                 yield {"type": "error", "step": "classify_query", "status": "failed", "message": "Classifier agent did not produce a final parsable output."}
                 self._close_db()
                 return
                 
            if not isinstance(classification_output, dict) or "workflow" not in classification_output:
                 # Classifier returned something, but not the expected format
                 yield {"type": "error", "step": "classify_query", "status": "failed", "message": f"Classifier returned invalid final output structure: {classification_output}"}
                 self._close_db()
                 return

            # --- Step 1c: Yield Classification Result --- 
            workflow_type = classification_output.get("workflow")
            yield {"type": "status", "step": "classify_query", "status": "completed", "details": f"Query classified for '{workflow_type}' workflow.", "classification_details": classification_output}
            yield {"type": "routing_decision", "workflow_type": workflow_type}

            # --- Step 2: Route to Workflow --- 
            yield {"type": "status", "step": "route_workflow", "status": "in_progress", "details": f"Routing to '{workflow_type}' workflow."}

            # Workflow execution remains the same - they yield their own streams
            if workflow_type == "insight":
                insight_workflow = InsightWorkflow(db, self.schema_file)
                async for workflow_chunk in insight_workflow.run(user_query):
                    yield workflow_chunk
            elif workflow_type == "optimization":
                optimization_workflow = OptimizationWorkflow(db, self.schema_file)
                async for workflow_chunk in optimization_workflow.run(user_query):
                    yield workflow_chunk
            else:
                yield {"type": "error", "step": "route_workflow", "message": f"Unknown workflow type: {workflow_type}"}
                return

        except Exception as e:
             yield {"type": "error", "step": "router_exception", "message": f"Router Error: {e}"}
             import traceback
             traceback.print_exc()
        finally:
            self._close_db()

# --- Testing Code Update --- 
# (Make sure the test code handles the fact that the router now yields slightly differently
# - it won't yield the internal classifier LogEntry/RunLogPatch chunks anymore,
# but will yield the final classification status/details dict directly after step 1)
# Example usage (for testing)
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv

    load_dotenv()

    async def main_test():
        # ... (schema_f and test_user setup remains the same) ...
        schema_f = os.path.join(os.path.dirname(__file__), '../../neo4j_schema.md')
        print(f"Schema path: {schema_f}")
        test_user = "test-router-user"

        router = Router(schema_file=schema_f)

        test_query_insight = "Which ad groups have the highest cost per click?"
        print(f"\n--- Running Router with Insight Query: '{test_query_insight}' ---")
        try:
            async for result_chunk in router.run(user_query=test_query_insight, user_id=test_user):
                # No longer expect RunLogPatch from classifier, only from workflows
                if isinstance(result_chunk, RunLogPatch):
                    print(f"WORKFLOW PATCH: run_id={result_chunk.run_id} ops={len(result_chunk.ops)} ops")
                elif isinstance(result_chunk, dict):
                    print(f"DICT: {json.dumps(result_chunk, indent=2)}")
                else:
                    print(f"OTHER: {result_chunk}")
        except Exception as e:
            print(f"Insight test failed: {e}")
            import traceback
            traceback.print_exc()

        # Create a new router instance for the second test
        router_opt = Router(schema_file=schema_f)
        test_query_optimization = "Suggest ways to lower my overall advertising spend."
        print(f"\n--- Running Router with Optimization Query: '{test_query_optimization}' ---")
        try:
            async for result_chunk in router_opt.run(user_query=test_query_optimization, user_id=test_user):
                 if isinstance(result_chunk, RunLogPatch):
                     print(f"WORKFLOW PATCH: run_id={result_chunk.run_id} ops={len(result_chunk.ops)} ops")
                 elif isinstance(result_chunk, dict):
                     print(f"DICT: {json.dumps(result_chunk, indent=2)}")
                 else:
                     print(f"OTHER: {result_chunk}")
        except Exception as e:
            print(f"Optimization test failed: {e}")
            import traceback
            traceback.print_exc()

    asyncio.run(main_test())

