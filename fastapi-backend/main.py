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

# --- Setup: Paths and Environment --- 

# Assume backend is run from the workspace root OR fastapi-backend dir
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load .env file from the project root (../)
# Ensure your .env file is in the graphdb directory
dotenv_path = os.path.join(project_root, '.env')
found_dotenv = load_dotenv(dotenv_path=dotenv_path)
if not found_dotenv:
    print(f"WARNING: .env file not found at {dotenv_path}. Ensure environment variables are set.")

# --- Configuration & Validation --- 

# Validate environment variables
required_env_vars = ["OPENAI_API_KEY", "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    print(f"ERROR: Missing environment variables: {', '.join(missing_vars)}. Set them in {dotenv_path} or system environment.")
    sys.exit(1) # Exit if critical env vars are missing

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") # Get API key

# Schema File Path (relative to project root)
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
    "llm": None # Add LLM instance to app state
}

# --- FastAPI App --- 

app = FastAPI(title="Insight Assistant Backend")

# Configure CORS (Cross-Origin Resource Sharing)
# Allows the Next.js frontend (running on a different port) to communicate with the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], # Allow your Next.js frontend origin
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
except ImportError as e:
    print(f"ERROR: Failed to import Router or Agents: {e}. Ensure langchain_arch is in the Python path ({project_root}) and dependencies are installed.")
    sys.exit(1)

@app.on_event("startup")
async def startup_event():
    """Initialize Neo4j Driver, LLM, and LangChain Router on application startup."""
    print("Initializing Neo4j driver...")
    try:
        app_state["neo4j_driver"] = AsyncGraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
        )
        await app_state["neo4j_driver"].verify_connectivity()
        print("Neo4j driver initialized successfully.")
    except Exception as e:
        print(f"FATAL: Failed to initialize Neo4j Driver: {e}")
        # Decide if the app should proceed without the driver
        # sys.exit(1) # Or just log the error and let endpoints handle the missing driver

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
    print("WebSocket connection established.")

    router = app_state.get("router")
    neo4j_driver = app_state.get("neo4j_driver")
    llm = app_state.get("llm") # Get LLM instance

    if not router or not neo4j_driver or not llm:
        await websocket.send_json({"type": "error", "message": "Backend components (Router, DB, LLM) not initialized."})
        await websocket.close()
        return

    try:
        while True:
            # Wait for a message (user query) from the client
            user_query = await websocket.receive_text()
            print(f"Received message: {user_query}")

            generated_queries_list = [] # To store queries for execution later
            workflow_type = None # To store 'insight' or 'optimization'
            requires_execution = True # Assume execution needed unless told otherwise
            final_workflow_message = None # Store messages like final_insight/final_recommendation if execution not needed
            captured_reasoning = None # Variable to store query generation reasoning

            # --- Start LangChain Router Processing --- 
            try:
                async for chunk in router.run(user_query=user_query):
                    # Send each chunk (status, reasoning, data, etc.) to the client
                    await websocket.send_json(chunk)

                    # Capture routing decision (Assuming router yields this first)
                    if chunk.get("type") == "routing_decision":
                        workflow_type = chunk.get("workflow_type")
                        print(f"Router decided on workflow: {workflow_type}")
                    
                    # Capture generated queries (standardized step name)
                    if chunk.get("type") == "status" and chunk.get("step") == "generate_queries" and chunk.get("status") == "completed":
                        if isinstance(chunk.get("generated_queries"), list):
                            generated_queries_list = chunk["generated_queries"]
                            print(f"Captured {len(generated_queries_list)} generated queries from step: {chunk.get('step')}.")
                        else:
                             print("WARNING: generate_queries completed but 'generated_queries' key missing or not a list.")
                             
                    # Capture reasoning summary associated with query generation
                    if chunk.get("type") == "reasoning_summary" and chunk.get("step") == "generate_queries":
                        captured_reasoning = chunk.get("reasoning", "N/A") # Store reasoning, default to N/A
                        print(f"Captured query generation reasoning.")

                    # Capture final message if workflow indicates no execution needed
                    if chunk.get("type") in ["final_insight", "final_recommendation"] and not chunk.get("requires_execution", True):
                         requires_execution = False
                         final_workflow_message = chunk # Store the message to send later
                         print(f"Workflow indicated no query execution needed. Capturing final message.")
                
                # Indicate main router processing finished
                await websocket.send_json({"type": "status", "step": "Processing", "status": "router_completed", "details": "Workflow generation finished. Proceeding to execution/analysis..."})

            except Exception as e:
                print(f"ERROR during router execution: {e}")
                import traceback
                tb_str = traceback.format_exc()
                await websocket.send_json({"type": "error", "message": f"Error processing request: {e}", "details": tb_str })
                continue # Skip steps below if router failed

            # --- Step 2: Execute Generated Queries (if any and required) --- 
            all_query_results = [] # Store results: List[Dict(objective, query, data, error)]
            execution_completed_successfully = False

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
            
            elif not generated_queries_list and final_workflow_message:
                 # Workflow generated no queries but provided a final message directly
                 await websocket.send_json(final_workflow_message)
                 print("Sent final message provided directly by workflow (no execution needed).")
                 # Skip final analysis section as workflow handled it
                 execution_completed_successfully = False # Prevent final analysis step
            
            else:
                 # No queries generated and no specific message -> Skipped execution
                 await websocket.send_json({"type": "status", "step": "QueryExecution", "status": "skipped", "details": "No generated queries captured or required to execute."})
                 execution_completed_successfully = False # Prevent final analysis step

            # --- Step 3: Final Analysis / Recommendation Generation --- 
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

                    # Instantiate the correct text-based agent
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
                    text_agent_input = {}
                    if workflow_type == "insight":
                        text_agent_input = {"query": user_query, "data": all_query_results, "query_generation_reasoning": captured_reasoning or "N/A"}
                    elif workflow_type == "optimization":
                        grouped_results = {}
                        for result_item in all_query_results:
                            objective = result_item.get("objective", "Unknown Objective")
                            if objective not in grouped_results:
                                grouped_results[objective] = []
                            grouped_results[objective].append(result_item)
                        text_agent_input = {"query": user_query, "data": grouped_results}
                    
                    # Graph agent input now takes the flat list of all query result objects
                    graph_agent_input = {"query": user_query, "data": all_query_results}

                    # Invoke agents concurrently using asyncio.gather
                    print(f"---> Invoking final text agent ({workflow_type}) and graph agent concurrently...")
                    text_agent_task = final_text_agent.chain.ainvoke(text_agent_input)
                    graph_agent_task = graph_agent.chain.ainvoke(graph_agent_input)
                    
                    # Gather results, return_exceptions=True handles errors in either task
                    results = await asyncio.gather(text_agent_task, graph_agent_task, return_exceptions=True)
                    
                    text_agent_output = results[0]
                    graph_agent_output = results[1]
                    print(f"<--- Concurrent agent invocation finished.")
                    # Add specific log for the graph agent's raw output or exception
                    print(f"---> Raw graph_agent_output: {graph_agent_output}")

                    # Send Graph Suggestion Message Separately (if successful)
                    graph_suggestions_list = None # Expect a list now
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
                        
                    # Send the list of suggestions (even if empty)
                    if graph_suggestions_list is not None: # Check if processing happened
                        try:
                            # Send message with type 'graph_suggestions' (plural) and the list
                            await websocket.send_json({"type": "graph_suggestions", "suggestions": graph_suggestions_list})
                            print(f"Graph suggestions message sent to client ({len(graph_suggestions_list)} suggestions).")
                        except Exception as send_err:
                             print(f"ERROR sending graph suggestion message: {send_err}")

                    # Process Text Agent Result and Send Final Message
                    final_message_payload = {} # Start fresh for the text message
                    text_agent_failed = False
                    if isinstance(text_agent_output, Exception):
                        print(f"!!! ERROR in final text agent ({workflow_type}): {text_agent_output}")
                        tb_str = "".join(traceback.format_exception(type(text_agent_output), text_agent_output, text_agent_output.__traceback__))
                        final_message_payload['error_details'] = f"Text Generation Error: {text_agent_output}\n{tb_str}"
                        if workflow_type == "insight": final_message_payload['insight'] = "Error generating insight."
                        else: final_message_payload['optimization_report'] = "Error generating report."
                        text_agent_failed = True
                    elif isinstance(text_agent_output, dict):
                        # Expecting keys like report_sections (list) and reasoning (str)
                        # We'll pass the whole dict payload for now, frontend can extract sections
                        final_message_payload.update(text_agent_output)
                    else:
                        print(f"WARNING: Final text agent ({workflow_type}) returned unexpected type: {type(text_agent_output)}")
                        # Set default structure for error case
                        if workflow_type == "insight": 
                            final_message_payload['insight'] = "Unexpected output format."
                        else: 
                            final_message_payload['report_sections'] = [{'title': 'Error', 'content': 'Unexpected output format from agent.'}]
                        text_agent_failed = True
                        
                    # Send the final text-based message (insight/recommendation)
                    final_status = "completed_with_errors" if text_agent_failed else "completed"
                    message_to_send = {
                        "type": final_message_type, 
                        "step": "FinalAnalysis", # Step name might need adjustment if we consider graph separate
                        "status": final_status, 
                        **final_message_payload # Excludes graph suggestion
                    }
                    print(f"Sending final {final_message_type} message: (Keys: {list(message_to_send.keys())})") 
                    await websocket.send_json(message_to_send)
                    print(f"Final {workflow_type} message sent to client.")

                except Exception as final_agent_setup_err:
                    print(f"!!! ERROR during final agent setup/invocation: {final_agent_setup_err}")
                    import traceback
                    tb_str = traceback.format_exc()
                    print(tb_str)
                    await websocket.send_json({"type": "error", "step": "FinalAnalysis", "message": f"Failed during final analysis setup/invocation: {final_agent_setup_err}", "details": tb_str})

            elif execution_completed_successfully and not workflow_type:
                 print("WARNING: Query execution completed but workflow type unknown. Skipping final analysis.")
                 await websocket.send_json({"type": "warning", "step": "FinalAnalysis", "message": "Could not determine original workflow type to generate final analysis."}) 
            else:
                 # Execution was skipped or failed with errors deemed critical earlier
                 print("Skipping final analysis step due to skipped/failed execution or missing workflow type.")
                 
            print(f"--- Full processing finished for user query: {user_query} --- ")

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
    print(f"Looking for .env at: {dotenv_path}")
    print(f"Looking for schema at: {schema_path_abs}")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 