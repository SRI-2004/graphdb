import warnings
from pydantic.warnings import PydanticDeprecatedSince211

# 1) Silence the Pydantic-v2.11 deprecation warnings:
warnings.filterwarnings(
    "ignore",
    category=PydanticDeprecatedSince211,
    message="Accessing the 'model_fields' attribute.*"
)

import os
import sys
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase
import pandas as pd # Import pandas for potential DataFrame conversion
from typing import Dict, List, Any # Add typing imports
import uuid # Add UUID import
from langchain_core.tracers.log_stream import LogEntry
from langchain_core.messages import BaseMessage
import json

import logging
# 2) Turn down Uvicorn's logger to WARNING (so it won't print INFO messages)
logging.getLogger("uvicorn").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


# --- Setup: Paths and Environment --- 

# Define project root (one level up from this file) primarily for sys.path and schema location
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Add project root to sys.path to allow importing langchain_arch
if project_root not in sys.path:
    sys.path.insert(0, project_root)

dotenv_path_local = os.path.join(os.path.dirname(__file__), '.env') 
load_dotenv()


required_env_vars = ["OPENAI_API_KEY", "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}. Ensure they are set in the execution environment (e.g., Render service config) or a local .env file.")
    sys.exit(1) # Exit if critical env vars are missing

# These will now correctly use Render's env vars first
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Schema File Path (relative to project root - graphdb/)
# This uses the correctly calculated project_root path
SCHEMA_FILE_DEFAULT = "neo4j_schema.md"
schema_path_abs = os.path.abspath(os.path.join(project_root, SCHEMA_FILE_DEFAULT))
if not os.path.exists(schema_path_abs):
    print(f"ERROR: Schema file not found at '{schema_path_abs}'")
    sys.exit(1)

# --- Application State (Initialization on Startup) --- 

# Store shared resources like the driver, router, and LLM instance
app_state = {
    "neo4j_driver": None,
    "router": None,
    "llm": None, # Add LLM instance to app state
    "classifier_agent": None # Add classifier instance to app state
}

# --- FastAPI App --- 

app = FastAPI(title="Insight Assistant Backend")

# Configure CORS (Cross-Origin Resource Sharing)
# Allows the Next.js frontend (running on a different port) to communicate with the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow your Next.js frontend origin
    allow_credentials=True,
    allow_methods=["*"], # Allow all methods (GET, POST, etc.)
    allow_headers=["*"], # Allow all headers
)

# --- Pydantic Models (for request/response validation) ---

class ChatRequest(BaseModel):
    query: str

# --- Initialization Logic (Router, Driver, LLM, Agents) --- 

# Import Router and Agents here, after setting sys.path
try:
    from langchain_openai import ChatOpenAI # Import LLM
    from langchain_arch.chains.router import Router
    # Import final agents
    from langchain_arch.agents.insight_generator import InsightGeneratorAgent
    from langchain_arch.agents.optimization_generator import OptimizationRecommendationGeneratorAgent
    from langchain_arch.agents.graph_generator import GraphGeneratorAgent # Import the new agent
    from langchain_arch.agents.classifier import ClassifierAgent # Import the ClassifierAgent
    # Import workflow classes
    from langchain_arch.chains.insight_workflow import InsightWorkflow
    from langchain_arch.chains.optimization_workflow import OptimizationWorkflow
except ImportError as e:
    print(f"ERROR: Failed to import Router, Agents, or Workflows: {e}. Ensure langchain_arch is in the Python path ({project_root}) and dependencies are installed.")
    sys.exit(1)

@app.on_event("startup")
async def startup_event():
    """Initialize Neo4j Driver, LLM, and LangChain Router on application startup."""
    warnings.filterwarnings(
        "ignore",
        category=PydanticDeprecatedSince211,
        message="Accessing the 'model_fields' attribute.*"
    )
    print("Initializing Neo4j driver...")
    neo4j_uri = os.getenv("NEO4J_URI") # Get URI again for logging
    print(f"Attempting to connect to Neo4j at: {neo4j_uri}") # Log the URI being used
    try:
        app_state["neo4j_driver"] = AsyncGraphDatabase.driver(
            NEO4J_URI, # Use the variable loaded earlier
            auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
        )
        # Add a specific timeout to verify_connectivity if desired (e.g., 10 seconds)
        # await asyncio.wait_for(app_state["neo4j_driver"].verify_connectivity(), timeout=10.0)
        await app_state["neo4j_driver"].verify_connectivity() # Verify connection
        print("Neo4j driver initialized and connection verified successfully.")
    except Exception as e:
        # Log the specific exception type and message
        print(f"FATAL: Failed to initialize or verify Neo4j Driver connection to {neo4j_uri}.")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Details: {e}")
        # Optionally log traceback for more detail if needed during debugging
        # import traceback
        # traceback.print_exc()
        app_state["neo4j_driver"] = None # Ensure driver state is None if failed
        # Consider if the app should exit or continue degraded
        # sys.exit(1)

    print("Initializing LLM...")
    try:
        # Initialize the LLM instance (adjust model name and temp as needed)
        app_state["llm"] = ChatOpenAI(model="gpt-4-turbo", temperature=0, api_key=OPENAI_API_KEY)
        print("LLM initialized successfully.")
    except Exception as e:
        print(f"ERROR: Failed to initialize LLM: {e}")
        # App might still run if LLM is not critical for all endpoints, but chat will fail.

    print("Initializing LangChain Router...")
    # Ensure driver is available (LLM not needed directly by Router init)
    if app_state["neo4j_driver"]:
        try:
            # Router initializes its own classifier and manages DB connection
            # It likely doesn't need LLM or connection details passed directly here
            app_state["router"] = Router(schema_file=schema_path_abs)
            print("LangChain Router initialized successfully.")
        except Exception as e:
            print(f"ERROR: Failed to initialize Router: {e}")
            # App can likely run, but chat functionality will fail
    else:
        print("WARNING: Skipping Router initialization because Neo4j driver failed.")

    print("Initializing Classifier Agent...")
    try:
        # Initialize the actual ClassifierAgent
        app_state["classifier_agent"] = ClassifierAgent() # Corrected Agent Initialization
        print("Classifier agent initialized successfully.")
    except Exception as e:
        print(f"ERROR: Failed to initialize classifier agent: {e}")
        # App might still run if classifier is not critical for all endpoints, but chat will fail.

@app.on_event("shutdown")
async def shutdown_event():
    """Close the Neo4j Driver on application shutdown."""
    if app_state["neo4j_driver"]:
        print("Closing Neo4j driver...")
        await app_state["neo4j_driver"].close()
        print("Neo4j driver closed.")

# --- Helper Function for Neo4j Query Execution --- 
async def execute_neo4j_query(driver, query: str, params: dict = None):
    """Executes a Cypher query and returns results as list of dicts or error string."""
    if not driver:
        return None, "Neo4j driver not available."
    try:
        async with driver.session() as session:
            response = await session.run(query, parameters=params)
            # Convert Neo4j Records to list of dictionaries
            data = [record.data() async for record in response]
            # Convert temporal types to ISO strings for JSON serialization
            processed_data = []
            for record in data:
                processed_record = {}
                for key, value in record.items():
                    # Add check for common temporal types from Neo4j driver and standard datetime
                    if hasattr(value, 'isoformat'):
                         processed_record[key] = value.isoformat()
                    elif isinstance(value, list):
                        # Handle lists potentially containing temporal types
                        processed_record[key] = [
                            item.isoformat() if hasattr(item, 'isoformat') else item
                            for item in value
                        ]
                    else:
                         processed_record[key] = value
                processed_data.append(processed_record)
            return processed_data, None # Return processed list of dicts and no error
    except Exception as e:
        error_message = f"Failed to execute query: {e}\nQuery: {query}\nParams: {params}"
        print(f"ERROR: {error_message}")
        return None, str(e) # Return None for data and the error message string

# --- API Endpoints --- 

@app.get("/")
async def read_root():
    return {"message": "Insight Assistant Backend is running."}

# --- WebSocket Endpoint for Chat --- 

@app.websocket("/api/v1/chat/stream")
async def websocket_chat(websocket: WebSocket):
    await websocket.accept()
    print("WebSocket connection accepted.")

    # Generate a unique ID for this session/user connection
    # TODO: Replace with proper user ID from authentication if available
    session_user_id = str(uuid.uuid4())
    print(f"Generated Session User ID: {session_user_id}")
    await websocket.send_json({"type": "session_info", "user_id": session_user_id})

    # Get shared resources
    router = app_state.get("router") # Keep router for now, might remove later if fully replaced
    neo4j_driver = app_state.get("neo4j_driver")
    llm = app_state.get("llm") # Get LLM instance
    classifier_agent = app_state.get("classifier_agent") # Get classifier instance

    if not neo4j_driver or not llm or not classifier_agent:
        # Removed router check for now, focus on classifier
        error_msg = "Backend components (DB Driver, LLM, Classifier) not initialized."
        print(f"ERROR: {error_msg}")
        await websocket.send_json({"type": "error", "message": error_msg})
        await websocket.close()
        return

    try:
        while True:
            # Wait for a message (user query) from the client
            user_query = await websocket.receive_text()
            print(f"Received message: {user_query}")

            # --- NEW Step 1: Conversational Classification --- 
            classification_output = None
            try:
                print(f"---> Calling ClassifierAgent for user: {session_user_id}")
                classification_output = await classifier_agent.run(query=user_query, user_id=session_user_id)
                print(f"<--- ClassifierAgent Output: {classification_output}")

                if not classification_output or not isinstance(classification_output, dict) or "action" not in classification_output or "response" not in classification_output:
                    # Handle unexpected/invalid output format from classifier
                    print(f"ERROR: ClassifierAgent returned invalid or missing output: {classification_output}")
                    await websocket.send_json({"type": "error", "step": "Classifier", "message": "Failed to understand the request due to an internal classification error."})
                    continue # Wait for next user message

                # Check the action determined by the classifier
                action = classification_output.get("action")
                classifier_response = classification_output.get("response")

                if action == "answer":
                    # Classifier answered directly based on memory
                    print("Classifier action: answer - Sending direct response.")
                    await websocket.send_json({"type": "classifier_answer", "content": classifier_response})
                    # End processing for this message, wait for the next one
                    continue 
                
                elif action == "trigger_workflow":
                    # Classifier determined a workflow is needed
                    print("Classifier action: trigger_workflow - Proceeding to workflow.")
                    # Send the intermediary message to the user
                    await websocket.send_json({"type": "classifier_info", "content": classifier_response})
                    
                    # Extract details needed for the workflow
                    workflow_type = classification_output.get("workflow_type")
                    entities = classification_output.get("entities") # May be null or []
                    
                    # Validate extracted details
                    if workflow_type not in ["insight", "optimization"]:
                         print(f"ERROR: Classifier requested workflow trigger but provided invalid type: {workflow_type}")
                         await websocket.send_json({"type": "error", "step": "Classifier", "message": f"Internal error: Invalid workflow type '{workflow_type}' identified."})
                         continue # Wait for next user message
                         
                    # Entities can be null or empty list, downstream steps should handle this
                    print(f"Workflow Type: {workflow_type}, Entities: {entities}")
                    # --- PROCEED TO WORKFLOW STEPS BELOW --- 
                    
                else:
                    # Unknown action from classifier
                    print(f"ERROR: ClassifierAgent returned unknown action: {action}")
                    await websocket.send_json({"type": "error", "step": "Classifier", "message": f"Internal error: Unknown classification action '{action}'."})
                    continue # Wait for next user message

            except Exception as e:
                print(f"ERROR during ClassifierAgent execution: {e}")
                import traceback
                tb_str = traceback.format_exc()
                await websocket.send_json({"type": "error", "step": "Classifier", "message": f"Error processing request: {e}", "details": tb_str })
                continue # Skip to next message on classifier error

            # --- Step 2: Query Planning & Generation (via Workflow) --- 
            generated_queries_list = []
            captured_reasoning = None
            query_gen_has_errors = False
            requires_execution = False # Default to not executing unless queries generated successfully

            # Instantiate and run the correct workflow based on classifier output
            try:
                print(f"---> Starting Query Generation via {workflow_type} workflow...")
                workflow_instance = None
                if workflow_type == "insight":
                    # Ensure schema_path_abs is correctly defined earlier or globally
                    workflow_instance = InsightWorkflow(schema_file=schema_path_abs) 
                elif workflow_type == "optimization":
                    workflow_instance = OptimizationWorkflow(schema_file=schema_path_abs)
                
                if workflow_instance:
                    async for chunk in workflow_instance.run(user_query): # Pass only user_query
                        # Forward chunk to frontend (contains status, reasoning, errors, generated queries)
                        await websocket.send_json(chunk)
                        
                        # Capture the generated queries list when the step completes
                        if chunk.get("type") == "status" and chunk.get("step") == "generate_queries" and chunk.get("status") == "completed":
                            generated_queries_list = chunk.get("generated_queries", [])
                            print(f"Captured {len(generated_queries_list)} generated queries from {workflow_type} workflow.")
                            
                        # Capture reasoning summary
                        if chunk.get("type") == "reasoning_summary" and chunk.get("step") == "generate_queries":
                            captured_reasoning = chunk.get("reasoning")
                            print("Captured query generation reasoning.")
                            
                        # Check for errors yielded by the workflow
                        if chunk.get("type") == "error":
                             query_gen_has_errors = True
                             print(f"Error during {workflow_type} workflow execution (query gen): {chunk.get('message')}")
                             # No need to continue if query gen fails catastrophically
                             # The error is sent to the frontend, loop will break naturally or finish
                             
                else:
                    # This case should not be reached due to validation after classifier
                    raise ValueError(f"Workflow instance could not be created for type: {workflow_type}")

                # Determine if execution is required AFTER iterating through the whole workflow stream
                if not query_gen_has_errors and generated_queries_list: 
                     requires_execution = True # Proceed to execution if queries were generated without error
                     print("Query generation successful. Proceeding to execution.")
                elif not query_gen_has_errors and not generated_queries_list:
                     print(f"{workflow_type} workflow completed without errors but generated no queries. Skipping execution.")
                     requires_execution = False 
                else: # query_gen_has_errors is True
                     print("Query generation failed. Skipping execution.")
                     requires_execution = False 
                     
            except Exception as wf_e:
                print(f"ERROR during {workflow_type} workflow invocation or processing: {wf_e}")
                query_gen_has_errors = True # Mark error occurred
                requires_execution = False # Definitely skip execution
                import traceback
                tb_str_wf = traceback.format_exc()
                await websocket.send_json({"type": "error", "step": "QueryGeneration", "message": f"Error running {workflow_type} workflow: {wf_e}", "details": tb_str_wf })

            # --- Step 3: Execute Generated Queries (if any and required) --- 
            all_query_results = []
            execution_completed_successfully = False # Reset before execution

            if generated_queries_list and requires_execution:
                await websocket.send_json({"type": "status", "step": "QueryExecution", "status": "in_progress", "details": f"Executing {len(generated_queries_list)} captured queries..."})
                
                results_processed_count = 0
                execution_has_errors = False
                for i, query_item in enumerate(generated_queries_list):
                    if isinstance(query_item, dict) and "query" in query_item:
                        objective = query_item.get("objective", f"Query {i+1}") # Use objective from item
                        query_text = query_item.get("query")
                        
                        await websocket.send_json({"type": "status", "step": "QueryExecution", "status": "running_query", "details": f"Running: {objective}", "index": i})
                        
                        # Execute query and get data/error
                        data, error = await execute_neo4j_query(neo4j_driver, query_text)
                        
                        # Store result details
                        result_detail = {
                            "objective": objective,
                            "query": query_text,
                            "data": data, # Will be None if error occurred
                            "error": error # Will be None if success
                        }
                        all_query_results.append(result_detail)
                        
                        # Send result back to client (individual query result)
                        result_message = {
                            "type": "query_result",
                            "objective": objective,
                            "query": query_text, # Include query for context
                            "index": i,
                        }
                        if error:
                            result_message["error"] = error
                            execution_has_errors = True # Mark that at least one error occurred
                        else:
                            # Send data only if no error
                            result_message["data"] = data 
                        
                        await websocket.send_json(result_message)
                        results_processed_count += 1
                    else:
                         print(f"Skipping invalid query item at index {i}: {query_item}")
                         await websocket.send_json({"type": "warning", "step": "QueryExecution", "message": f"Skipping invalid query item at index {i}.", "details": str(query_item)})
                # Final status for Query Execution phase
                status_detail = f"Finished executing {results_processed_count} queries."
                final_status = "completed"
                if execution_has_errors:
                    status_detail += " Some queries failed."
                    final_status = "completed_with_errors"
                else:
                     execution_completed_successfully = True # Mark successful completion
                     
                await websocket.send_json({"type": "status", "step": "QueryExecution", "status": final_status, "details": status_detail})
            elif not requires_execution:
                 # If execution was skipped due to query gen failure or no queries needed
                 print("Skipping Query Execution step.")
                 await websocket.send_json({"type": "status", "step": "QueryExecution", "status": "skipped", "details": "Query execution skipped."})
                 execution_completed_successfully = False # Ensure final analysis knows execution didn't happen

            # --- Step 4: Final Analysis / Recommendation Generation --- 
            if execution_completed_successfully and workflow_type:
                # Proceed only if execution finished without critical errors AND we know the workflow type
                await websocket.send_json({"type": "status", "step": "FinalAnalysis", "status": "in_progress", "details": f"Generating final {workflow_type} and graph suggestion..."})
                
                # Instantiate agents
                final_text_agent = None
                graph_agent = None
                final_message_type = "final_insight" # Default
                
                try:
                    # Instantiate the graph agent (always needed)
                    graph_agent = GraphGeneratorAgent()

                    # Instantiate the correct text-based agent based on workflow_type
                    if workflow_type == "insight":
                        final_text_agent = InsightGeneratorAgent()
                        final_message_type = "final_insight"
                    elif workflow_type == "optimization":
                        final_text_agent = OptimizationRecommendationGeneratorAgent()
                        final_message_type = "final_recommendation"
                    else:
                        raise ValueError(f"Unknown workflow type for final analysis: {workflow_type}")
                    
                    # Prepare inputs for both agents
                    # Text agent input varies
                    text_agent_query = user_query
                    text_agent_data = None
                    insight_reasoning = None # Variable to hold reasoning specifically for insight agent
                    if workflow_type == "insight":
                        # InsightGeneratorAgent.run expects query, data (list), user_id, query_generation_reasoning
                        text_agent_data = all_query_results # Pass the list of results
                        insight_reasoning = captured_reasoning # Assign captured reasoning
                    elif workflow_type == "optimization":
                        # OptimizationRecommendationGeneratorAgent.run expects query, data (dict), user_id
                        grouped_results = {}
                        for result_item in all_query_results:
                            objective = result_item.get("objective", "Unknown Objective")
                            if objective not in grouped_results:
                                grouped_results[objective] = []
                             # Store the full result item if needed by the agent, or just the data part
                            # Assuming agent needs the data part: result_item.get('data', []) or similar
                            if result_item.get('data') is not None:
                                grouped_results[objective].extend(result_item.get('data', [])) # Append data list
                            # If agent needs full result dict: grouped_results[objective].append(result_item)
                        text_agent_data = grouped_results

                    # Graph agent input takes the flat list of all query result objects (original)
                    graph_agent_input = {"query": user_query, "data": all_query_results}

                    # Invoke agents concurrently using asyncio.gather
                    print(f"---> Invoking final text agent ({workflow_type}) and graph agent concurrently...")
                    
                    # Call the run method for the text agent, passing user_id
                    # Updated call for insight agent to include reasoning
                    if workflow_type == "insight":
                        text_agent_task = final_text_agent.run(
                            query=text_agent_query, 
                            data=text_agent_data, 
                            user_id=session_user_id,
                            query_generation_reasoning=insight_reasoning or "" # Pass reasoning, default to empty string if None
                        )
                    elif workflow_type == "optimization":
                        text_agent_task = final_text_agent.run(
                            query=text_agent_query, 
                            data=text_agent_data, 
                            user_id=session_user_id
                            # Optimization agent does not need reasoning
                        )
                    
                    # Graph agent still uses chain.ainvoke as it was reverted
                    graph_agent_task = graph_agent.chain.ainvoke(graph_agent_input)
                    
                    # Gather results, return_exceptions=True handles errors in either task
                    results = await asyncio.gather(text_agent_task, graph_agent_task, return_exceptions=True)
                    
                    text_agent_output = results[0] # Result from agent.run()
                    graph_agent_output = results[1] # Result from graph_agent.chain.ainvoke()
                    print(f"<--- Concurrent agent invocation finished.")
                    print(f"---> Raw graph_agent_output: {graph_agent_output}")

                    # Process Text Agent Result
                    final_text_payload = None
                    if isinstance(text_agent_output, Exception):
                         print(f"!!! ERROR in final text agent ({workflow_type}): {text_agent_output}")
                         # Send error or default message
                         await websocket.send_json({
                             "type": final_message_type, # Use determined type
                             "error": f"Error generating final {workflow_type}: {text_agent_output}",
                             "content": f"An error occurred while generating the {workflow_type}."
                         })
                    elif text_agent_output is not None: 
                        final_text_payload = text_agent_output
                        # --- EDIT: Log the raw insight content --- 
                        if final_message_type == "final_insight" and "insight" in final_text_payload:
                            print("DEBUG: Raw Insight Content from Agent:\n---")
                            print(repr(final_text_payload["insight"])) # Use repr() to show newlines
                            print("---")
                        # --- End Log ---
                    else:
                         # Agent run resulted in None, likely an internal error handled by the agent
                         print(f"WARNING: Final text agent ({workflow_type}) returned None.")
                         await websocket.send_json({
                            "type": final_message_type,
                            "content": f"Could not generate the final {workflow_type}."
                         })

                    # Process Graph Agent Result (Original Logic - needs careful check)
                    graph_suggestions_list = None
                    if isinstance(graph_agent_output, Exception):
                        print(f"!!! ERROR in graph agent: {graph_agent_output}")
                        # Optionally send a graph error message or just log it
                        # await websocket.send_json({"type": "graph_suggestion_error", "error": str(graph_agent_output)})
                        graph_suggestions_list = [] # Send empty list on error
                    elif isinstance(graph_agent_output, dict) and 'graph_suggestions' in graph_agent_output and isinstance(graph_agent_output['graph_suggestions'], list):
                        graph_suggestions_list = graph_agent_output['graph_suggestions'] # Extract the list
                    else:
                        print(f"WARNING: Graph agent returned unexpected structure: {graph_agent_output}")
                        graph_suggestions_list = [] # Send empty list on format error
                        
                    # Combine and Send Final Message Only If Text Agent Succeeded
                    if final_text_payload is not None: 
                        final_response = {
                        "type": final_message_type, 
                            **final_text_payload,
                            "graph_suggestions": graph_suggestions_list if graph_suggestions_list is not None else [],
                            "query_generation_reasoning": captured_reasoning or "N/A" 
                        }
                        # --- EDIT: Log the final_response being sent ---
                        print(f"DEBUG: Sending final_response: {json.dumps(final_response, indent=2)}")
                        # --- End Log ---
                        await websocket.send_json(final_response)
                        print(f"Sent final {final_message_type} message.")

                except Exception as e:
                    print(f"ERROR during final analysis step: {e}")
                    import traceback
                    tb_str = traceback.format_exc()
                    print(tb_str)
                    await websocket.send_json({"type": "error", "step": "FinalAnalysis", "message": f"Failed during final analysis: {e}", "details": tb_str})

            elif not execution_completed_successfully and action == "trigger_workflow":
                 # Handle case where workflow was triggered but execution failed/skipped
                 print(f"Skipping final analysis for {workflow_type} as query execution was not successful.")
                 await websocket.send_json({"type": "warning", "step": "FinalAnalysis", "message": f"Could not generate final {workflow_type} because query execution failed or was skipped."})
            # Implicit else: If action was "answer", we already continued, so no final analysis happens.
                 
            print(f"--- Processing finished for user query: {user_query} --- ")

    except WebSocketDisconnect:
        print("WebSocket disconnected.")
    except Exception as e:
        print(f"ERROR in WebSocket handler: {e}")
        import traceback
        tb_str = traceback.format_exc()
        try:
            # Try sending error before closing if connection is still open
            await websocket.send_json({"type": "error", "message": f"Unexpected WebSocket error: {e}", "details": tb_str})
        except Exception:
            pass # Ignore error if sending fails (connection likely closed)
        finally:
             # Ensure connection is closed on error
             try:
                 await websocket.close()
             except Exception:
                 pass # Ignore if already closed

# --- Run the app (for local development) --- 
if __name__ == "__main__":
    print(f"Starting Uvicorn server...")
    print(f"Project Root: {project_root}")
    print(f"Looking for .env at: {dotenv_path_local}")
    print(f"Looking for schema at: {schema_path_abs}")
    uvicorn.run("main:app", host="0.0.0.0", port=8050) 