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
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase
from typing import Dict, List, Any, Optional, Union
import uuid # Add UUID import
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

# Check for both Google and Facebook required variables
google_env_vars = ["GOOGLE_NEO4J_URI", "GOOGLE_NEO4J_USERNAME", "GOOGLE_NEO4J_PASSWORD"]
facebook_env_vars = ["FACEBOOK_NEO4J_URI", "FACEBOOK_NEO4J_USERNAME", "FACEBOOK_NEO4J_PASSWORD"]
common_env_vars = ["OPENAI_API_KEY"]

# Load Google Neo4j environment variables
missing_google_vars = [var for var in google_env_vars if not os.getenv(var)]
if missing_google_vars:
    print(f"WARNING: Missing Google Neo4j environment variables: {', '.join(missing_google_vars)}.")
    # Fall back to non-prefixed variables if available
    if not os.getenv("NEO4J_URI") or not os.getenv("NEO4J_USERNAME") or not os.getenv("NEO4J_PASSWORD"):
        print(f"ERROR: No fallback Neo4j credentials found. Both Google-specific and default credentials are missing.")
        sys.exit(1)
    print(f"Using default Neo4j credentials for Google workflows.")
    GOOGLE_NEO4J_URI = os.getenv("NEO4J_URI")
    GOOGLE_NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
    GOOGLE_NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
else:
    GOOGLE_NEO4J_URI = os.getenv("GOOGLE_NEO4J_URI")
    GOOGLE_NEO4J_USERNAME = os.getenv("GOOGLE_NEO4J_USERNAME")
    GOOGLE_NEO4J_PASSWORD = os.getenv("GOOGLE_NEO4J_PASSWORD")

# Load Facebook Neo4j environment variables
missing_facebook_vars = [var for var in facebook_env_vars if not os.getenv(var)]
if missing_facebook_vars:
    print(f"WARNING: Missing Facebook Neo4j environment variables: {', '.join(missing_facebook_vars)}.")
    # Fall back to Google variables if available, then to non-prefixed variables
    if all(os.getenv(var) for var in google_env_vars):
        print(f"Using Google Neo4j credentials for Facebook workflows.")
        FACEBOOK_NEO4J_URI = GOOGLE_NEO4J_URI
        FACEBOOK_NEO4J_USERNAME = GOOGLE_NEO4J_USERNAME 
        FACEBOOK_NEO4J_PASSWORD = GOOGLE_NEO4J_PASSWORD
    elif os.getenv("NEO4J_URI") and os.getenv("NEO4J_USERNAME") and os.getenv("NEO4J_PASSWORD"):
        print(f"Using default Neo4j credentials for Facebook workflows.")
        FACEBOOK_NEO4J_URI = os.getenv("NEO4J_URI")
        FACEBOOK_NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
        FACEBOOK_NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
    else:
        print(f"ERROR: No fallback Neo4j credentials found for Facebook workflows.")
        sys.exit(1)
else:
    FACEBOOK_NEO4J_URI = os.getenv("FACEBOOK_NEO4J_URI")
    FACEBOOK_NEO4J_USERNAME = os.getenv("FACEBOOK_NEO4J_USERNAME")
    FACEBOOK_NEO4J_PASSWORD = os.getenv("FACEBOOK_NEO4J_PASSWORD")

# Check for common required variables
missing_common_vars = [var for var in common_env_vars if not os.getenv(var)]
if missing_common_vars:
    print(f"ERROR: Missing required environment variables: {', '.join(missing_common_vars)}.")
    sys.exit(1)

# Load common variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Schema File Paths (relative to project root - graphdb/)
# Default schema file - for backward compatibility
SCHEMA_FILE_DEFAULT = "neo4j_schema.md"
schema_path_abs = os.path.abspath(os.path.join(project_root, SCHEMA_FILE_DEFAULT))

# Google-specific schema
GOOGLE_SCHEMA_FILE = os.getenv("GOOGLE_SCHEMA_FILE", SCHEMA_FILE_DEFAULT)
google_schema_path_abs = os.path.abspath(os.path.join(project_root, GOOGLE_SCHEMA_FILE))

# Facebook-specific schema
FACEBOOK_SCHEMA_FILE = os.getenv("FACEBOOK_SCHEMA_FILE", SCHEMA_FILE_DEFAULT)
facebook_schema_path_abs = os.path.abspath(os.path.join(project_root, FACEBOOK_SCHEMA_FILE))

# Verify schema files exist
if not os.path.exists(google_schema_path_abs):
    print(f"WARNING: Google schema file not found at '{google_schema_path_abs}'. Using default schema.")
    google_schema_path_abs = schema_path_abs

if not os.path.exists(facebook_schema_path_abs):
    print(f"WARNING: Facebook schema file not found at '{facebook_schema_path_abs}'. Using default schema.")
    facebook_schema_path_abs = schema_path_abs

if not os.path.exists(schema_path_abs):
    print(f"ERROR: Default schema file not found at '{schema_path_abs}'")
    sys.exit(1)

# --- Application State (Initialization on Startup) --- 

# Store shared resources like the drivers, router, and LLM instance
app_state = {
    "google_neo4j_driver": None,    # Google-specific Neo4j driver
    "facebook_neo4j_driver": None,  # Facebook-specific Neo4j driver
    "router": None,
    "llm": None,                    # Add LLM instance to app state
    "classifier_agent": None        # Add classifier instance to app state
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

# Add a model for workflow trigger requests
class WorkflowTriggerRequest(BaseModel):
    query: str  # The user's query
    user_id: str  # The user ID for connecting to the right WebSocket
    workflow_type: Optional[str] = None  # Optional override for classification

class TriggerResponse(BaseModel):
    status: str
    workflow_id: str  # ID for tracking this workflow instance
    message: str

# --- Initialization Logic (Router, Driver, LLM, Agents) --- 

# Import Router and Agents here, after setting sys.path
try:
    # Import from langchain_arch
    from langchain_openai import ChatOpenAI # Import LLM
    from langchain_arch.chains.router import Router
    # Import final agents from langchain_arch
    from langchain_arch.agents.insight_generator import InsightGeneratorAgent
    from langchain_arch.agents.optimization_generator import OptimizationRecommendationGeneratorAgent
    from langchain_arch.agents.graph_generator import GraphGeneratorAgent # Import the new agent
    from langchain_arch.agents.classifier import ClassifierAgent # Import the ClassifierAgent
    # Import workflow classes from langchain_arch
    from langchain_arch.chains.insight_workflow import InsightWorkflow
    from langchain_arch.chains.optimization_workflow import OptimizationWorkflow
    
    # For google components status tracking
    google_components_available = True
except ImportError as e:
    print(f"ERROR: Failed to import Google components from langchain_arch: {e}. Ensure langchain_arch is in the Python path ({project_root}) and dependencies are installed.")
    google_components_available = False
    # Don't exit here, try to load facebook components first

# Also try to import Facebook components (don't exit on failure)
try:
    # Import final agents from facebook_arch
    from facebook_arch.agents.insight_generator import InsightGeneratorAgent as FacebookInsightAgent
    from facebook_arch.agents.optimization_generator import OptimizationRecommendationGeneratorAgent as FacebookOptimizationAgent
    # Import workflow classes from facebook_arch
    from facebook_arch.chains.insight_workflow import InsightWorkflow as FacebookInsightWorkflow
    from facebook_arch.chains.optimization_workflow import OptimizationWorkflow as FacebookOptimizationWorkflow
    
    # For facebook components status tracking
    facebook_components_available = True
    print("Successfully imported Facebook components from facebook_arch.")
except ImportError as e:
    print(f"WARNING: Failed to import Facebook components from facebook_arch: {e}. Facebook-specific optimizations will not be available.")
    facebook_components_available = False

# Exit if neither Google nor Facebook components are available
if not google_components_available and not facebook_components_available:
    print(f"FATAL: Neither Google nor Facebook components could be loaded. Ensure at least one architecture is properly installed.")
    sys.exit(1)

@app.on_event("startup")
async def startup_event():
    """Initialize Neo4j Drivers, LLM, and LangChain Router on application startup."""
    warnings.filterwarnings(
        "ignore",
        category=PydanticDeprecatedSince211,
        message="Accessing the 'model_fields' attribute.*"
    )
    
    # Initialize Google Neo4j driver
    print("Initializing Google Neo4j driver...")
    try:
        app_state["google_neo4j_driver"] = AsyncGraphDatabase.driver(
            GOOGLE_NEO4J_URI,
            auth=(GOOGLE_NEO4J_USERNAME, GOOGLE_NEO4J_PASSWORD)
        )
        await app_state["google_neo4j_driver"].verify_connectivity()
        print(f"Google Neo4j driver initialized and connection verified successfully at: {GOOGLE_NEO4J_URI}")
    except Exception as e:
        print(f"WARNING: Failed to initialize Google Neo4j Driver connection.")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Details: {e}")
        app_state["google_neo4j_driver"] = None
    
    # Initialize Facebook Neo4j driver
    print("Initializing Facebook Neo4j driver...")
    try:
        app_state["facebook_neo4j_driver"] = AsyncGraphDatabase.driver(
            FACEBOOK_NEO4J_URI,
            auth=(FACEBOOK_NEO4J_USERNAME, FACEBOOK_NEO4J_PASSWORD)
        )
        await app_state["facebook_neo4j_driver"].verify_connectivity()
        print(f"Facebook Neo4j driver initialized and connection verified successfully at: {FACEBOOK_NEO4J_URI}")
    except Exception as e:
        print(f"WARNING: Failed to initialize Facebook Neo4j Driver connection.")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Details: {e}")
        app_state["facebook_neo4j_driver"] = None
    
    # Check if at least one driver is available
    if not app_state["google_neo4j_driver"] and not app_state["facebook_neo4j_driver"]:
        print("FATAL: No Neo4j drivers could be initialized. Exiting.")
        sys.exit(1)

    print("Initializing LLM...")
    try:
        # Initialize the LLM instance (adjust model name and temp as needed)
        app_state["llm"] = ChatOpenAI(model="gpt-4o", temperature=0, api_key=OPENAI_API_KEY)
        print("LLM initialized successfully.")
    except Exception as e:
        print(f"ERROR: Failed to initialize LLM: {e}")
        # App might still run if LLM is not critical for all endpoints, but chat will fail.

    print("Initializing LangChain Router...")
    # Initialize Router with the default schema - it will be used primarily for classification
    # Later workflow-specific schemas will be used for the actual workflows
    if app_state["google_neo4j_driver"] or app_state["facebook_neo4j_driver"]:
        try:
            app_state["router"] = Router(schema_file=schema_path_abs)
            print("LangChain Router initialized successfully.")
        except Exception as e:
            print(f"ERROR: Failed to initialize Router: {e}")
    else:
        print("WARNING: Skipping Router initialization because no Neo4j drivers are available.")

    print("Initializing Classifier Agent...")
    try:
        # Initialize the actual ClassifierAgent
        app_state["classifier_agent"] = ClassifierAgent() # Corrected Agent Initialization
        print("Classifier agent initialized successfully.")
    except Exception as e:
        print(f"ERROR: Failed to initialize classifier agent: {e}")
    
    # Add status tracking to app_state
    app_state["google_components_available"] = google_components_available
    app_state["facebook_components_available"] = facebook_components_available
    
    # Log component availability
    print(f"Components availability: Google={google_components_available}, Facebook={facebook_components_available}")

@app.on_event("shutdown")
async def shutdown_event():
    """Close the Neo4j Drivers on application shutdown."""
    # Close Google Neo4j driver
    if app_state["google_neo4j_driver"]:
        print("Closing Google Neo4j driver...")
        await app_state["google_neo4j_driver"].close()
        print("Google Neo4j driver closed.")
    
    # Close Facebook Neo4j driver
    if app_state["facebook_neo4j_driver"]:
        print("Closing Facebook Neo4j driver...")
        await app_state["facebook_neo4j_driver"].close()
        print("Facebook Neo4j driver closed.")

# --- API Endpoints --- 

@app.get("/")
async def read_root():
    return {"message": "Insight Assistant Backend is running."}

# --- WebSocket Connection Manager ---

class ConnectionManager:
    def __init__(self):
        # Map of user_id to active WebSocket connection
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections[user_id] = websocket
        print(f"WebSocket connection established for user_id: {user_id}")
    
    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]
            print(f"WebSocket connection removed for user_id: {user_id}")
    
    async def send_json(self, user_id: str, message: dict):
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_json(message)
                return True
            except Exception as e:
                print(f"Error sending message to user {user_id}: {e}")
                return False
        else:
            print(f"No active connection for user_id: {user_id}")
            return False

# Initialize connection manager
connection_manager = ConnectionManager()

# --- WebSocket Endpoint for Chat --- 

@app.websocket("/api/v1/chat/stream")
async def websocket_chat(websocket: WebSocket):
    """
    WebSocket endpoint for maintaining a connection with the frontend.
    Simplified to focus only on establishing the connection and handling disconnects.
    User queries and workflow triggering are now handled by the chatbot agent, not this endpoint.
    """
    # For tracking a unique websocket connection for the session
    session_user_id = f"user-{uuid.uuid4()}"
    
    try:
        # Connect via connection manager
        await connection_manager.connect(websocket, session_user_id)
        
        # Send welcome message with user_id (frontend will use this to interact with the agent)
        await websocket.send_json({
            "type": "connection_established",
            "user_id": session_user_id,
            "message": "WebSocket connection established. This connection will receive workflow updates."
        })
        
        # Simple message handling loop - only for connection maintenance
        while True:
            try:
                # Wait for any message
                message = await websocket.receive_text()
                
                try:
                    # Try to parse as JSON
                    message_data = json.loads(message)
                    message_type = message_data.get("type", "unknown")
                    
                    # Only handle heartbeat messages here
                    if message_type == "heartbeat":
                        await websocket.send_json({"type": "heartbeat_response"})
                    else:
                        # Log but don't process other message types
                        print(f"WebSocket received message type '{message_type}' from user {session_user_id}. Ignoring (use agent endpoints instead).")
                except json.JSONDecodeError:
                    # Not JSON, just log
                    print(f"WebSocket received non-JSON message from user {session_user_id}. Ignoring.")
            
            except WebSocketDisconnect:
                # Client disconnected gracefully
                print(f"WebSocket client {session_user_id} disconnected gracefully.")
                break # Exit the loop
            except Exception as e:
                # Handle other potential errors (e.g., processing message) without breaking loop
                print(f"Error in WebSocket message loop for user {session_user_id}: {e}")
                # Depending on the error, you might want to break here too, 
                # but for now, we continue to allow recovery from minor issues.
    
    except WebSocketDisconnect:
        # This catches disconnects that happen *before* or *during* the initial connect/accept
        print(f"WebSocket disconnected for user {session_user_id} during or before connection setup.")
    except Exception as e:
        # Catch any other unexpected errors during initial connection setup
        print(f"Unexpected error during WebSocket connection setup for {session_user_id}: {e}")
    finally:
        # Ensure user is removed from connection manager
        connection_manager.disconnect(session_user_id)
        print(f"Connection closed and removed from manager for user {session_user_id}")

# --- Background Task for Running Workflows ---

async def run_workflow_background(
    workflow_type: str,
    user_query: str, 
    user_id: str,
    workflow_id: str,
    skip_classification: bool = False
):
    """
    Executes the full workflow in the background:
    1. Classification (if not skipped)
    2. Query Generation
    3. Query Execution
    4. Final Analysis
    
    Sends all updates via WebSocket using the connection_manager.
    """
    print(f"Starting background workflow execution: {workflow_id} for user {user_id}")
    
    # Determine platform (Google or Facebook) from workflow_id
    platform = "google"  # Default to google
    if workflow_id.startswith("facebook-"):
        platform = "facebook"
    
    print(f"Detected platform: {platform} from workflow_id: {workflow_id}")
    
    # Select the appropriate schema and driver based on platform
    if platform == "google":
        selected_schema_path = google_schema_path_abs
        selected_driver = app_state.get("google_neo4j_driver")
    else:  # facebook
        selected_schema_path = facebook_schema_path_abs
        selected_driver = app_state.get("facebook_neo4j_driver")
    
    # Verify that we have a driver for this platform
    if not selected_driver:
        print(f"ERROR: No Neo4j driver available for platform: {platform}")
        await connection_manager.send_json(user_id, {
            "type": "error",
            "workflow_id": workflow_id,
            "step": "initialization",
            "message": f"No database connection available for {platform} platform."
        })
        return  # Exit early
    
    # Send initial status
    await connection_manager.send_json(user_id, {
        "type": "status", 
        "workflow_id": workflow_id,
        "step": "workflow_start", 
        "status": "in_progress",
        "details": f"Starting {platform} {workflow_type} workflow..."
    })
    
    # Note: Much of this logic is copied/adapted from the websocket_chat function
    # with modifications to use connection_manager instead of direct websocket
    final_workflow_type = workflow_type # Initialize here for scope if classification fails/is skipped
    
    try:
        # --- Step 1: Classification (optional, may be skipped) ---
        # final_workflow_type = workflow_type  # Default to what was passed in - moved up
        
        if not skip_classification and app_state.get("classifier_agent"):
            # Send classification start notification
            await connection_manager.send_json(user_id, {
                "type": "status", 
                "workflow_id": workflow_id,
                "step": "classification", 
                "status": "in_progress",
                "details": "Classifying your request..."
            })
            
            # Call classifier agent
            try:
                classification_output = await app_state["classifier_agent"].run(
                    query=user_query, user_id=user_id
                )
                
                if classification_output and isinstance(classification_output, dict):
                    action = classification_output.get("action", "unknown")
                    classifier_response = classification_output.get("response", "")
                    
                    if action == "answer":
                        # Classifier answered directly, send response and end workflow
                        await connection_manager.send_json(user_id, {
                            "type": "classifier_answer", 
                            "workflow_id": workflow_id,
                            "content": classifier_response
                        })
                        # End processing for this workflow
                        return # Exit the function early
                    
                    elif action == "trigger_workflow":
                        # Extract which workflow the classifier determined
                        detected_workflow_type = classification_output.get("workflow_type")
                        
                        if detected_workflow_type and detected_workflow_type in ["insight", "optimization"]:
                            # Use the detected workflow type if valid
                            final_workflow_type = detected_workflow_type
                            
                            # Notify about classifier determination
                            await connection_manager.send_json(user_id, {
                                "type": "classifier_info", 
                                "workflow_id": workflow_id,
                                "content": classifier_response
                            })
                        else:
                            # Invalid workflow type detected, use original
                            print(f"WARNING: Classifier returned invalid workflow type: {detected_workflow_type}. Using original: {workflow_type}")
                            final_workflow_type = workflow_type # Ensure it's set back if invalid
                    else:
                        # Unknown action, continue with original workflow type
                        print(f"WARNING: ClassifierAgent returned unknown action: {action}. Continuing with specified workflow: {workflow_type}")
                        final_workflow_type = workflow_type # Ensure it's set
                else:
                    # Invalid or no output from classifier, continue with original workflow
                    print(f"WARNING: Invalid output from classifier: {classification_output}")
                    final_workflow_type = workflow_type # Ensure it's set
            except Exception as e:
                # Classification error, continue with the original workflow type
                print(f"ERROR during ClassifierAgent execution: {e}")
                import traceback # Import traceback here
                tb_str = traceback.format_exc()
                await connection_manager.send_json(user_id, {
                    "type": "error", 
                    "workflow_id": workflow_id,
                    "step": "classification", 
                    "message": f"Error during classification: {e}",
                    "details": tb_str
                })
                final_workflow_type = workflow_type # Ensure it's set on error
        else:
             # Classification skipped or agent not available
            final_workflow_type = workflow_type # Ensure it's set


            # --- Step 2: Query Planning & Generation (via Workflow) --- 
            generated_queries_list = []
            captured_reasoning = None
            query_gen_has_errors = False

            try:
                print(f"---> Starting Query Generation via {final_workflow_type} workflow for {platform} platform...")
                workflow_instance = None
        
                if platform == "google":
                    # For Google, use workflows from langchain_arch
                    if not app_state.get("google_components_available", False):
                        raise ImportError("Google components were not successfully imported during startup")
                        
                    from langchain_arch.chains.insight_workflow import InsightWorkflow as GoogleInsightWorkflow
                    from langchain_arch.chains.optimization_workflow import OptimizationWorkflow as GoogleOptimizationWorkflow
                    
                    if final_workflow_type == "insight":
                        workflow_instance = GoogleInsightWorkflow(schema_file=selected_schema_path) 
                        print(f"Using GoogleInsightWorkflow from langchain_arch")
                    elif final_workflow_type == "optimization":
                        workflow_instance = GoogleOptimizationWorkflow(schema_file=selected_schema_path)
                        print(f"Using GoogleOptimizationWorkflow from langchain_arch")
                    else:
                        raise ValueError(f"Unsupported workflow type: {final_workflow_type}")
                elif platform == "facebook":
                    # For Facebook, use workflows from facebook_arch
                    if not app_state.get("facebook_components_available", False):
                        raise ImportError("Facebook components were not successfully imported during startup")
                        
                    try:
                        # Try to import from facebook_arch
                        from facebook_arch.chains.insight_workflow import InsightWorkflow as FacebookInsightWorkflow
                        from facebook_arch.chains.optimization_workflow import OptimizationWorkflow as FacebookOptimizationWorkflow
                        
                        if final_workflow_type == "insight":
                            workflow_instance = FacebookInsightWorkflow(schema_file=selected_schema_path) 
                            print(f"Using FacebookInsightWorkflow from facebook_arch")
                        elif final_workflow_type == "optimization":
                            workflow_instance = FacebookOptimizationWorkflow(schema_file=selected_schema_path)
                            print(f"Using FacebookOptimizationWorkflow from facebook_arch")
                        else:
                            raise ValueError(f"Unsupported workflow type: {final_workflow_type}")
                    except ImportError as ie:
                        print(f"ERROR: Could not import Facebook workflows from facebook_arch: {ie}")
                        await connection_manager.send_json(user_id, {
                            "type": "error", 
                            "workflow_id": workflow_id,
                            "step": "generate_queries", 
                            "message": f"Facebook workflow components not found: {ie}"
                        })
                        return  # Exit early
                else:
                    raise ValueError(f"Unsupported platform: {platform}")
                
                if workflow_instance:
                    async for chunk in workflow_instance.run(user_query):
                        # Forward chunk to frontend via connection manager
                        chunk["workflow_id"] = workflow_id  # Add workflow_id to all chunks
                        await connection_manager.send_json(user_id, chunk)
                        
                        # Capture generated queries when that step completes
                        if chunk.get("type") == "status" and chunk.get("step") == "generate_queries" and chunk.get("status") == "completed":
                            generated_queries_list = chunk.get("generated_queries", [])
                            print(f"Captured {len(generated_queries_list)} generated queries from {final_workflow_type} workflow.")
                            
                        # Capture reasoning summary
                        if chunk.get("type") == "reasoning_summary" and chunk.get("step") == "generate_queries":
                            captured_reasoning = chunk.get("reasoning")
                            print("Captured query generation reasoning.")
                            
                        # Check for errors
                        if chunk.get("type") == "error":
                            query_gen_has_errors = True
                            print(f"Query generation error: {chunk.get('message')}")
            
            except Exception as e:
                query_gen_has_errors = True
                print(f"Exception in query generation: {e}")
                import traceback # Import traceback here
                tb_str = traceback.format_exc()
                await connection_manager.send_json(user_id, {
                    "type": "error", 
                    "workflow_id": workflow_id,
                    "step": "generate_queries", 
                    "message": f"Error during query generation: {e}",
                    "details": tb_str
                })
            
            # --- Continue with execution only if we have queries ---
            if not generated_queries_list or query_gen_has_errors:
                if not generated_queries_list and not query_gen_has_errors:
                    # This was an intentional "no queries needed" scenario
                    await connection_manager.send_json(user_id, {
                        "type": "status", 
                        "workflow_id": workflow_id,
                        "step": "execution", 
                        "status": "completed",
                        "details": "No database queries required for this request."
                    })
                else:
                    # This was due to an error
                    await connection_manager.send_json(user_id, {
                        "type": "error", 
                        "workflow_id": workflow_id,
                        "step": "execution", 
                        "message": "Cannot execute queries due to errors in the generation step."
                    })
                return # Exit the function early
            
            # --- Step 3: Execute Queries ---
            # Logic copied from websocket_chat with appropriate adaptations
            all_query_results = []
            execution_completed_successfully = False
            
            await connection_manager.send_json(user_id, {
                "type": "status", 
                "workflow_id": workflow_id,
                "step": "execution", 
                "status": "in_progress",
                "details": f"Executing {len(generated_queries_list)} database queries on {platform} database..."
            })
            
            try:
                # Use the platform-specific Neo4j driver
                driver = selected_driver
                if not driver:
                    raise RuntimeError(f"Neo4j driver for {platform} not initialized")
                
                # Execute each query
                for i, query_obj in enumerate(generated_queries_list):
                    objective = query_obj.get("objective", f"{platform} Query {i+1}")
                    query = query_obj.get("query", "").strip()
                    
                    if not query:
                        print(f"WARNING: Empty query for {platform} objective: {objective}")
                        continue
                    
                    # Update progress
                    await connection_manager.send_json(user_id, {
                        "type": "status", 
                        "workflow_id": workflow_id,
                        "step": "execute_query", 
                        "status": "in_progress",
                        "details": f"Executing {platform} query for: {objective}",
                        "current": i + 1,
                        "total": len(generated_queries_list)
                    })
                    
                    # Execute the query
                    try:
                        # Need imports for temporal types if not already present globally
                        from neo4j.time import Date, DateTime, Time
                        from datetime import date, datetime, time
                        
                        async with driver.session() as session:
                            result = await session.run(query)
                            data = await result.data() # Get results as a list of dictionaries
                            
                            # Convert any Neo4j temporal types or Python datetime types to strings
                            # Important: Modify the list of dicts 'data' directly
                            for record_dict in data: # Iterate through each dictionary in the list
                                for key, value in record_dict.items():
                                    if isinstance(value, (Date, DateTime, Time, date, datetime, time)):
                                        record_dict[key] = str(value) # Update the value in the dictionary
                            
                            # Store the modified result with its objective
                            query_result = {
                            "platform": platform,
                            "objective": objective,
                                "query": query,
                                "data": data # Now contains stringified dates/times
                        }
                            all_query_results.append(query_result)
                        
                            # Send individual query result (with stringified dates) to frontend
                            await connection_manager.send_json(user_id, query_result)
                    
                    except Exception as query_err:
                        print(f"ERROR executing {platform} query for {objective}: {query_err}")
                        await connection_manager.send_json(user_id, {
                            "type": "error", 
                            "workflow_id": workflow_id,
                            "step": "execute_query",
                            "platform": platform,
                            "objective": objective,
                            "message": f"Error executing {platform} query: {query_err}"
                        })
                
                # If we got here, mark execution as successful even if some queries failed
                execution_completed_successfully = True
                
                # Send execution complete status
                await connection_manager.send_json(user_id, {
                    "type": "status", 
                    "workflow_id": workflow_id,
                    "step": "execution", 
                    "status": "completed",
                    "details": f"Executed {len(all_query_results)} of {len(generated_queries_list)} queries successfully on {platform} database."
                })
            
            except Exception as e:
                print(f"ERROR during query execution: {e}")
                import traceback # Import traceback here
                tb_str = traceback.format_exc()
                await connection_manager.send_json(user_id, {
                    "type": "error", 
                    "workflow_id": workflow_id,
                    "step": "execution", 
                    "message": f"Error during query execution: {e}",
                    "details": tb_str
                })
                return # Exit the function early

            # --- Step 4: Final Analysis / Recommendation Generation --- 
            if execution_completed_successfully and final_workflow_type:
                await connection_manager.send_json(user_id, {
                    "type": "status", 
                    "workflow_id": workflow_id,
                    "step": "FinalAnalysis", 
                    "status": "in_progress",
                    "details": f"Generating final {final_workflow_type} and graph suggestion..."
                })
                    
                # Instantiate agents (correct indentation)
                final_text_agent = None
                graph_agent = None
                final_message_type = "final_insight"  # Default
                
                try:
                    # Instantiate platform-specific agents
                    if platform == "google":
                        # For Google, use agents from langchain_arch
                        if not app_state.get("google_components_available", False):
                            raise ImportError("Google components were not successfully imported during startup")
                            
                        from langchain_arch.agents.graph_generator import GraphGeneratorAgent
                        from langchain_arch.agents.insight_generator import InsightGeneratorAgent as GoogleInsightAgent
                        from langchain_arch.agents.optimization_generator import OptimizationRecommendationGeneratorAgent as GoogleOptimizationAgent
                        
                        # Graph agent is shared across platforms
                        graph_agent = GraphGeneratorAgent()
                        
                        # Text agent depends on workflow type
                        if final_workflow_type == "insight":
                            final_text_agent = GoogleInsightAgent()
                            final_message_type = "final_insight"
                            print(f"Using GoogleInsightAgent from langchain_arch")
                        elif final_workflow_type == "optimization":
                            final_text_agent = GoogleOptimizationAgent()
                            final_message_type = "final_recommendation"
                            print(f"Using GoogleOptimizationAgent from langchain_arch")
                        else:
                            raise ValueError(f"Unknown workflow type for final analysis: {final_workflow_type}")
                    
                    elif platform == "facebook":
                        # For Facebook, use agents from facebook_arch
                        if not app_state.get("facebook_components_available", False):
                            raise ImportError("Facebook components were not successfully imported during startup")
                            
                        try:
                            # GraphGeneratorAgent is shared across platforms
                            from langchain_arch.agents.graph_generator import GraphGeneratorAgent
                            graph_agent = GraphGeneratorAgent()
                            
                            # Try to import platform-specific text agents
                            from facebook_arch.agents.insight_generator import InsightGeneratorAgent as FacebookInsightAgent
                            from facebook_arch.agents.optimization_generator import OptimizationRecommendationGeneratorAgent as FacebookOptimizationAgent
                            
                            if final_workflow_type == "insight":
                                final_text_agent = FacebookInsightAgent()
                                final_message_type = "final_insight"
                                print(f"Using FacebookInsightAgent from facebook_arch")
                            elif final_workflow_type == "optimization":
                                final_text_agent = FacebookOptimizationAgent()
                                final_message_type = "final_recommendation"
                                print(f"Using FacebookOptimizationAgent from facebook_arch")
                            else:
                                raise ValueError(f"Unknown workflow type for final analysis: {final_workflow_type}")
                                
                        except ImportError as ie:
                            print(f"ERROR: Could not import Facebook agents from facebook_arch: {ie}")
                            await connection_manager.send_json(user_id, {
                                "type": "error", 
                                "workflow_id": workflow_id,
                                "step": "FinalAnalysis", 
                                "message": f"Facebook analysis components not found: {ie}"
                            })
                            return  # Exit early
                    else:
                        raise ValueError(f"Unsupported platform: {platform}")
                    
                    # Prepare inputs for both agents
                    text_agent_query = user_query
                    text_agent_data = None
                    insight_reasoning = None 
                
                    if final_workflow_type == "insight":
                        text_agent_data = all_query_results
                        insight_reasoning = captured_reasoning
                    elif final_workflow_type == "optimization":
                        grouped_results = {}
                        for result_item in all_query_results:
                            objective = result_item.get("objective", "Unknown Objective")
                            if objective not in grouped_results:
                                grouped_results[objective] = []
                            if result_item.get('data') is not None and isinstance(result_item['data'], list):
                                 grouped_results[objective].extend(result_item['data'])
                        text_agent_data = grouped_results

                    # Graph agent input 
                    graph_agent_input = {"query": user_query, "data": all_query_results}

                    # Invoke agents concurrently
                    print(f"---> Invoking final text agent ({final_workflow_type}) and graph agent concurrently...")
                    
                    text_agent_task = None
                    if final_workflow_type == "insight":
                        text_agent_task = final_text_agent.run(
                            query=text_agent_query, 
                            data=text_agent_data, 
                            user_id=user_id,
                            query_generation_reasoning=insight_reasoning or ""
                        )
                    elif final_workflow_type == "optimization":
                        text_agent_task = final_text_agent.run(
                            query=text_agent_query, 
                            data=text_agent_data, 
                            user_id=user_id
                        )
                    
                    graph_agent_task = graph_agent.chain.ainvoke(graph_agent_input)
                    
                    if text_agent_task:
                        results = await asyncio.gather(text_agent_task, graph_agent_task, return_exceptions=True)
                        text_agent_output = results[0]
                        graph_agent_output = results[1]
                    else:
                        # Directly await the graph task if the text task wasn't initiated
                        graph_agent_output_list = await asyncio.gather(graph_agent_task, return_exceptions=True)
                        graph_agent_output = graph_agent_output_list[0]
                        text_agent_output = ValueError("Text agent task was not initiated.")
                    
                    print(f"<--- Concurrent agent invocation finished.")

                    # Process Text Agent Result
                    final_text_payload = None
                    if isinstance(text_agent_output, Exception):
                        print(f"!!! ERROR in final text agent ({final_workflow_type}): {text_agent_output}")
                        await connection_manager.send_json(user_id, {
                            "type": final_message_type, 
                            "workflow_id": workflow_id,
                            "error": f"Error generating final {final_workflow_type}: {text_agent_output}",
                            "content": f"An error occurred while generating the {final_workflow_type}."
                         })
                    elif text_agent_output is not None: 
                        final_text_payload = text_agent_output
                    else:
                        print(f"WARNING: Final text agent ({final_workflow_type}) returned None.")
                        await connection_manager.send_json(user_id, {
                            "type": final_message_type,
                            "workflow_id": workflow_id,
                            "content": f"Could not generate the final {final_workflow_type}."
                         })

                    # Process Graph Agent Result
                    graph_suggestions_list = []
                    if isinstance(graph_agent_output, Exception):
                        print(f"!!! ERROR in graph agent: {graph_agent_output}")
                    elif isinstance(graph_agent_output, dict) and 'graph_suggestions' in graph_agent_output and isinstance(graph_agent_output['graph_suggestions'], list):
                        graph_suggestions_list = graph_agent_output['graph_suggestions']
                    else:
                        print(f"WARNING: Graph agent returned unexpected structure or no suggestions: {graph_agent_output}")
                        
                    # Combine and Send Final Message ONLY if the text agent succeeded
                    if final_text_payload is not None: 
                        final_response = {
                        "type": final_message_type, 
                            "workflow_id": workflow_id,
                            **final_text_payload,
                            "graph_suggestions": graph_suggestions_list,
                            "query_generation_reasoning": captured_reasoning or "N/A" 
                        }
                        await connection_manager.send_json(user_id, final_response)
                        print(f"Sent final {final_message_type} message with {len(graph_suggestions_list)} graph suggestions.")
                    else:
                        print("Skipping final combined message as text agent did not produce a payload.")

                except Exception as e:
                    # Catch errors specific to the final analysis/agent invocation block
                    print(f"ERROR during final analysis stage: {e}")
                    import traceback
                    tb_str = traceback.format_exc()
                    await connection_manager.send_json(user_id, {
                        "type": "error",
                        "workflow_id": workflow_id,
                        "step": "FinalAnalysis",
                        "message": f"Error during final analysis: {e}",
                        "details": tb_str
                    })
        
    except Exception as e:
        # Catch any broader errors in the main try block of the function
        print(f"CRITICAL: Unhandled error in run_workflow_background: {e}")
        import traceback # Import traceback here
        tb_str = traceback.format_exc()
        await connection_manager.send_json(user_id, {
            "type": "error",
            "workflow_id": workflow_id,
            "step": "workflow_execution", # General step
            "message": f"Critical error during workflow execution: {e}",
            "details": tb_str
        })
    
    finally:
        # This block executes regardless of errors in the main try block
        # Send workflow completion status
        await connection_manager.send_json(user_id, {
            "type": "status",
            "workflow_id": workflow_id,
            "step": "workflow_end",
            "status": "completed", # Mark as completed even if errors occurred
            "details": f"{final_workflow_type.capitalize() if final_workflow_type else 'Workflow'} processing completed."
        })
        print(f"Background workflow {workflow_id} for user {user_id} completed.")


@app.post("/api/v1/workflows/trigger/google_insight", response_model=TriggerResponse)
async def trigger_google_insight(
    request: WorkflowTriggerRequest, 
    background_tasks: BackgroundTasks
):
    """Trigger a Google Insight workflow execution."""
    workflow_id = f"google-insight-{uuid.uuid4()}"
    # Add task to background
    background_tasks.add_task(
        run_workflow_background,
        workflow_type="insight",
        user_query=request.query,
        user_id=request.user_id,
        workflow_id=workflow_id,
        skip_classification=request.workflow_type is not None  # Skip if explicit type provided
    )
    
    return {
        "status": "initiated",
        "workflow_id": workflow_id,
        "message": "Google Insight workflow initiated. Results will be streamed via WebSocket."
    }

@app.post("/api/v1/workflows/trigger/google_optimization", response_model=TriggerResponse)
async def trigger_google_optimization(
    request: WorkflowTriggerRequest, 
    background_tasks: BackgroundTasks
):
    """Trigger a Google Optimization workflow execution."""
    print(f"[Backend Endpoint Debug] Received Request Body: {request}")
    workflow_id = f"google-optimization-{uuid.uuid4()}"
    # Add task to background
    background_tasks.add_task(
        run_workflow_background,
        workflow_type="optimization",
        user_query=request.query,
        user_id=request.user_id,
        workflow_id=workflow_id,
        skip_classification=request.workflow_type is not None  # Skip if explicit type provided
    )
    
    return {
        "status": "initiated",
        "workflow_id": workflow_id,
        "message": "Google Optimization workflow initiated. Results will be streamed via WebSocket."
    }

@app.post("/api/v1/workflows/trigger/facebook_insight", response_model=TriggerResponse)
async def trigger_facebook_insight(
    request: WorkflowTriggerRequest, 
    background_tasks: BackgroundTasks
):
    """Trigger a Facebook Insight workflow execution."""
    workflow_id = f"facebook-insight-{uuid.uuid4()}"
    # Add task to background
    background_tasks.add_task(
        run_workflow_background,
        workflow_type="insight",
        user_query=request.query,
        user_id=request.user_id,
        workflow_id=workflow_id,
        skip_classification=request.workflow_type is not None  # Skip if explicit type provided
    )
    
    return {
        "status": "initiated",
        "workflow_id": workflow_id,
        "message": "Facebook Insight workflow initiated. Results will be streamed via WebSocket."
    }

@app.post("/api/v1/workflows/trigger/facebook_optimization", response_model=TriggerResponse)
async def trigger_facebook_optimization(
    request: WorkflowTriggerRequest, 
    background_tasks: BackgroundTasks
):
    """Trigger a Facebook Optimization workflow execution."""
    workflow_id = f"facebook-optimization-{uuid.uuid4()}"
    # Add task to background
    background_tasks.add_task(
        run_workflow_background,
        workflow_type="optimization",
        user_query=request.query,
        user_id=request.user_id,
        workflow_id=workflow_id,
        skip_classification=request.workflow_type is not None  # Skip if explicit type provided
    )
    
    return {
        "status": "initiated",
        "workflow_id": workflow_id,
        "message": "Facebook Optimization workflow initiated. Results will be streamed via WebSocket."
    }

async def _execute_queries_on_driver(
    queries_list: List[Dict],
    driver: AsyncGraphDatabase.driver,
    user_id: str,
    workflow_id: str,
    platform_name: str,
    connection_manager: ConnectionManager
) -> List[Dict]:
    """
    Helper function to execute a list of Cypher queries against a given Neo4j driver.
    Sends progress updates via WebSocket.
    Returns a list of query results, each including the platform name.
    """
    all_query_results_for_platform = [] # Renamed variable for clarity
    if not queries_list:
        await connection_manager.send_json(user_id, {
            "type": "status",
            "workflow_id": workflow_id,
            "step": f"execute_{platform_name}_queries",
            "status": "skipped",
            "details": f"No queries to execute for {platform_name}."
        })
        return all_query_results_for_platform

    await connection_manager.send_json(user_id, {
        "type": "status",
        "workflow_id": workflow_id,
        "step": f"execute_{platform_name}_queries",
        "status": "in_progress",
        "details": f"Executing {len(queries_list)} {platform_name} database queries..."
    })

    from neo4j.time import Date, DateTime, Time
    from datetime import date, datetime, time

    for i, query_obj in enumerate(queries_list):
        objective = query_obj.get("objective", f"{platform_name} Query {i+1}")
        query_str = query_obj.get("query", "").strip() # Renamed to query_str to avoid conflict with query var name

        if not query_str:
            print(f"WARNING: Empty query for {platform_name} objective: {objective}")
            continue

        await connection_manager.send_json(user_id, {
            "type": "status",
            "workflow_id": workflow_id,
            "step": f"execute_{platform_name}_query_detail",
            "status": "in_progress",
            "details": f"Executing {platform_name} query for: {objective}",
            "current": i + 1,
            "total": len(queries_list)
        })

        try:
            async with driver.session() as session:
                result = await session.run(query_str) # Use query_str
                data = await result.data()

                for record_dict in data:
                    for key, value in record_dict.items():
                        if isinstance(value, (Date, DateTime, Time, date, datetime, time)):
                            record_dict[key] = str(value)
                
                # This is the dictionary that gets returned by the function in a list
                collected_result = {
                    "platform": platform_name, 
                    "objective": objective, 
                    "query": query_str, # Store the executed query string
                    "data": data
                }
                all_query_results_for_platform.append(collected_result)

                # This is the WebSocket message for individual query result (can stay as is)
                await connection_manager.send_json(user_id, {
                    "type": "query_result",
                    "workflow_id": workflow_id,
                    "platform": platform_name,
                    "objective": objective,
                    "data": data 
                })
        except Exception as query_err:
            print(f"ERROR executing {platform_name} query for {objective}: {query_err}")
            await connection_manager.send_json(user_id, {
                "type": "error",
                "workflow_id": workflow_id,
                "step": f"execute_{platform_name}_query_detail",
                "objective": objective,
                "message": f"Error executing {platform_name} query: {query_err}"
            })

    await connection_manager.send_json(user_id, {
        "type": "status",
        "workflow_id": workflow_id,
        "step": f"execute_{platform_name}_queries",
        "status": "completed",
        "details": f"Executed {len(all_query_results_for_platform)} of {len(queries_list)} {platform_name} queries."
    })
    return all_query_results_for_platform


async def run_general_insight_workflow_background(
    user_query: str,
    user_id: str,
    workflow_id: str
):
    """
    Executes a combined insight workflow:
    1. Gathers insights from Google.
    2. Gathers insights from Facebook.
    3. Combines results and uses an LLM for a final summary.
    Sends all updates via WebSocket.
    """
    print(f"Starting general insight workflow: {workflow_id} for user {user_id}")
    await connection_manager.send_json(user_id, {
        "type": "status", "workflow_id": workflow_id, "step": "workflow_start",
        "status": "in_progress", "details": "Starting general insight workflow..."
    })

    # Ensure necessary components are available
    google_driver = app_state.get("google_neo4j_driver")
    facebook_driver = app_state.get("facebook_neo4j_driver")
    llm = app_state.get("llm")
    google_components_available = app_state.get("google_components_available", False)
    facebook_components_available = app_state.get("facebook_components_available", False)

    if not (google_driver and facebook_driver and llm and google_components_available and facebook_components_available):
        error_message = "General insight workflow cannot proceed due to missing components: "
        if not google_driver: error_message += "Google Driver missing. "
        if not facebook_driver: error_message += "Facebook Driver missing. "
        if not llm: error_message += "LLM missing. "
        if not google_components_available: error_message += "Google components not loaded. "
        if not facebook_components_available: error_message += "Facebook components not loaded. "
        
        print(f"ERROR: {error_message}")
        await connection_manager.send_json(user_id, {
            "type": "error", "workflow_id": workflow_id, "step": "initialization",
            "message": error_message
        })
        return

    all_google_query_results = []
    google_reasoning = "N/A"
    all_facebook_query_results = []
    facebook_reasoning = "N/A"
    
    final_insight_text = "Could not generate combined insight."
    final_graph_suggestions = []

    try:
        # --- Step 1: Google Insight Data Gathering ---
        await connection_manager.send_json(user_id, {
            "type": "status", "workflow_id": workflow_id, "step": "google_data_gathering",
            "status": "in_progress", "details": "Gathering data from Google..."
        })
        try:
            from langchain_arch.chains.insight_workflow import InsightWorkflow as GoogleInsightWorkflow
            google_workflow = GoogleInsightWorkflow(schema_file=google_schema_path_abs)
            
            google_generated_queries_raw = []
            async for chunk in google_workflow.run(user_query):
                chunk["workflow_id"] = workflow_id # Add workflow_id for context
                if chunk.get("type") == "status" and chunk.get("step") == "generate_queries":
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Google: {chunk.get('details', '')}"})
                    if chunk.get("status") == "completed":
                        google_generated_queries_raw = chunk.get("generated_queries", [])
                elif chunk.get("type") == "reasoning_summary" and chunk.get("step") == "generate_queries":
                    google_reasoning = chunk.get("reasoning", "N/A")
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Google: {chunk.get('details', '')}", "reasoning": google_reasoning})
                elif chunk.get("type") == "error":
                     await connection_manager.send_json(user_id, {**chunk, "details": f"Google: {chunk.get('details', '')}"})
                     raise Exception(chunk.get("message","Error in Google query generation"))
            
            # Prefix objectives for uniqueness
            google_generated_queries_prefixed = [
                {**q, 'objective': f"google: {q.get('objective', f'Query {i+1}')}"} 
                for i, q in enumerate(google_generated_queries_raw)
            ]

            if google_generated_queries_prefixed:
                all_google_query_results = await _execute_queries_on_driver(
                    google_generated_queries_prefixed, google_driver, user_id, workflow_id, "google", connection_manager
                )
            await connection_manager.send_json(user_id, {
                "type": "status", "workflow_id": workflow_id, "step": "google_data_gathering",
                "status": "completed", "details": "Google data gathering complete."
            })
        except Exception as e:
            print(f"ERROR during Google data gathering for general insight: {e}")
            await connection_manager.send_json(user_id, {
                "type": "error", "workflow_id": workflow_id, "step": "google_data_gathering",
                "message": f"Error during Google data gathering: {e}"
            })
            # Decide if we should proceed or stop; for now, we'll try Facebook

        # --- Step 2: Facebook Insight Data Gathering ---
        await connection_manager.send_json(user_id, {
            "type": "status", "workflow_id": workflow_id, "step": "facebook_data_gathering",
            "status": "in_progress", "details": "Gathering data from Facebook..."
        })
        try:
            from facebook_arch.chains.insight_workflow import InsightWorkflow as FacebookInsightWorkflow
            facebook_workflow = FacebookInsightWorkflow(schema_file=facebook_schema_path_abs)

            facebook_generated_queries_raw = []
            async for chunk in facebook_workflow.run(user_query):
                chunk["workflow_id"] = workflow_id
                if chunk.get("type") == "status" and chunk.get("step") == "generate_queries":
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Facebook: {chunk.get('details', '')}"})
                    if chunk.get("status") == "completed":
                        facebook_generated_queries_raw = chunk.get("generated_queries", [])
                elif chunk.get("type") == "reasoning_summary" and chunk.get("step") == "generate_queries":
                    facebook_reasoning = chunk.get("reasoning", "N/A")
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Facebook: {chunk.get('details', '')}", "reasoning": facebook_reasoning})
                elif chunk.get("type") == "error":
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Facebook: {chunk.get('details', '')}"})
                    raise Exception(chunk.get("message","Error in Facebook query generation"))
            
            # Prefix objectives for uniqueness
            facebook_generated_queries_prefixed = [
                {**q, 'objective': f"facebook: {q.get('objective', f'Query {i+1}')}"} 
                for i, q in enumerate(facebook_generated_queries_raw)
            ]

            if facebook_generated_queries_prefixed:
                all_facebook_query_results = await _execute_queries_on_driver(
                    facebook_generated_queries_prefixed, facebook_driver, user_id, workflow_id, "facebook", connection_manager
                )
            await connection_manager.send_json(user_id, {
                "type": "status", "workflow_id": workflow_id, "step": "facebook_data_gathering",
                "status": "completed", "details": "Facebook data gathering complete."
            })
        except Exception as e:
            print(f"ERROR during Facebook data gathering for general insight: {e}")
            await connection_manager.send_json(user_id, {
                "type": "error", "workflow_id": workflow_id, "step": "facebook_data_gathering",
                "message": f"Error during Facebook data gathering: {e}"
            })

        # --- Step 3: Combine Data & Generate Final Insight ---
        if not all_google_query_results and not all_facebook_query_results:
            final_insight_text = "No data was found from Google or Facebook for your query."
            await connection_manager.send_json(user_id, {
                "type": "status", "workflow_id": workflow_id, "step": "combined_analysis",
                "status": "skipped", "details": final_insight_text
            })
        else:
            await connection_manager.send_json(user_id, {
                "type": "status", "workflow_id": workflow_id, "step": "combined_analysis",
                "status": "in_progress", "details": "Combining data and generating final insight..."
            })
            
            combined_query_results = all_google_query_results + all_facebook_query_results # These now have prefixed objectives
            combined_reasoning = f"""Google Reasoning:
{google_reasoning}

Facebook Reasoning:
{facebook_reasoning}"""

            try:
                # Use InsightGeneratorAgent (e.g., from langchain_arch)
                from langchain_arch.agents.insight_generator import InsightGeneratorAgent
                insight_agent = InsightGeneratorAgent() # Assuming default init is fine
                
                insight_agent_output = await insight_agent.run(
                    query=user_query,
                    data=combined_query_results,
                    user_id=user_id, # Pass user_id if agent uses it
                    query_generation_reasoning=combined_reasoning
                )
                
                if isinstance(insight_agent_output, dict) and "insight" in insight_agent_output:
                    final_insight_text = insight_agent_output["insight"]
                elif isinstance(insight_agent_output, str): # Basic fallback
                     final_insight_text = insight_agent_output
                else:
                    print(f"Warning: Insight agent returned unexpected output: {insight_agent_output}")
                    final_insight_text = "Failed to generate a combined insight summary."

                # Graph Suggestions
                from langchain_arch.agents.graph_generator import GraphGeneratorAgent
                graph_agent = GraphGeneratorAgent()
                # Pass combined results (with prefixed objectives) to graph agent
                graph_agent_input = {"query": user_query, "data": combined_query_results} 
                graph_agent_output = await graph_agent.chain.ainvoke(graph_agent_input)
                if isinstance(graph_agent_output, dict) and 'graph_suggestions' in graph_agent_output:
                    # Ensure graph agent associates suggestions with the prefixed objectives
                    # (This assumes the agent's prompt guides it or it picks up the objective field correctly)
                    final_graph_suggestions = graph_agent_output['graph_suggestions']

            except Exception as e:
                print(f"ERROR during combined analysis: {e}")
                import traceback
                tb_str = traceback.format_exc()
                await connection_manager.send_json(user_id, {
                    "type": "error", "workflow_id": workflow_id, "step": "combined_analysis",
                    "message": f"Error during combined analysis: {e}", "details": tb_str
                })

        # --- Step 4: Send Final Result ---
        await connection_manager.send_json(user_id, {
            "type": "final_insight", # Consistent with other insight messages
            "workflow_id": workflow_id,
            "insight": final_insight_text,
            "graph_suggestions": final_graph_suggestions,
            "query_generation_reasoning": combined_reasoning, # Send combined reasoning
            "executed_queries": combined_query_results, # Add all executed query data
            "raw_google_results_count": len(all_google_query_results),
            "raw_facebook_results_count": len(all_facebook_query_results)
        })

    except Exception as e:
        # Broad exception for the whole workflow
        print(f"CRITICAL: Unhandled error in run_general_insight_workflow_background: {e}")
        import traceback
        tb_str = traceback.format_exc()
        await connection_manager.send_json(user_id, {
            "type": "error", "workflow_id": workflow_id, "step": "workflow_execution",
            "message": f"Critical error during general insight workflow: {e}", "details": tb_str
        })
    finally:
        await connection_manager.send_json(user_id, {
            "type": "status", "workflow_id": workflow_id, "step": "workflow_end",
            "status": "completed", "details": "General insight workflow processing completed."
        })
        print(f"General insight workflow {workflow_id} for user {user_id} completed.")


@app.post("/api/v1/workflows/trigger/general_insight", response_model=TriggerResponse)
async def trigger_general_insight(
    request: WorkflowTriggerRequest,
    background_tasks: BackgroundTasks
):
    """Trigger a General Insight workflow combining Google and Facebook data."""
    workflow_id = f"general-insight-{uuid.uuid4()}"
    background_tasks.add_task(
        run_general_insight_workflow_background,
        user_query=request.query,
        user_id=request.user_id,
        workflow_id=workflow_id
    )
    return {
        "status": "initiated",
        "workflow_id": workflow_id,
        "message": "General Insight workflow initiated. Results will be streamed via WebSocket."
    }

async def run_general_optimization_workflow_background(
    user_query: str,
    user_id: str,
    workflow_id: str
):
    """
    Executes a combined optimization workflow:
    1. Gathers optimization recommendations from Google.
    2. Gathers optimization recommendations from Facebook.
    3. Combines results and uses an LLM for a final recommendation.
    Sends all updates via WebSocket.
    """
    print(f"Starting general optimization workflow: {workflow_id} for user {user_id}")
    await connection_manager.send_json(user_id, {
        "type": "status", "workflow_id": workflow_id, "step": "workflow_start",
        "status": "in_progress", "details": "Starting general optimization workflow..."
    })

    # Ensure necessary components are available
    google_driver = app_state.get("google_neo4j_driver")
    facebook_driver = app_state.get("facebook_neo4j_driver")
    llm = app_state.get("llm")
    google_components_available = app_state.get("google_components_available", False)
    facebook_components_available = app_state.get("facebook_components_available", False)

    if not (google_driver and facebook_driver and llm and google_components_available and facebook_components_available):
        error_message = "General optimization workflow cannot proceed due to missing components: "
        if not google_driver: error_message += "Google Driver missing. "
        if not facebook_driver: error_message += "Facebook Driver missing. "
        if not llm: error_message += "LLM missing. "
        if not google_components_available: error_message += "Google components not loaded. "
        if not facebook_components_available: error_message += "Facebook components not loaded. "
        
        print(f"ERROR: {error_message}")
        await connection_manager.send_json(user_id, {
            "type": "error", "workflow_id": workflow_id, "step": "initialization",
            "message": error_message
        })
        return

    all_google_query_results = []
    google_reasoning = "N/A"
    all_facebook_query_results = []
    facebook_reasoning = "N/A"
    
    final_report_sections = []
    final_graph_suggestions = []

    try:
        # --- Step 1: Google Optimization Data Gathering ---
        await connection_manager.send_json(user_id, {
            "type": "status", "workflow_id": workflow_id, "step": "google_data_gathering",
            "status": "in_progress", "details": "Gathering data from Google for optimization..."
        })
        try:
            from langchain_arch.chains.optimization_workflow import OptimizationWorkflow as GoogleOptimizationWorkflow
            google_workflow = GoogleOptimizationWorkflow(schema_file=google_schema_path_abs)
            
            google_generated_queries_raw = []
            async for chunk in google_workflow.run(user_query):
                chunk["workflow_id"] = workflow_id # Add workflow_id for context
                if chunk.get("type") == "status" and chunk.get("step") == "generate_queries":
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Google: {chunk.get('details', '')}"})
                    if chunk.get("status") == "completed":
                        google_generated_queries_raw = chunk.get("generated_queries", [])
                elif chunk.get("type") == "reasoning_summary" and chunk.get("step") == "generate_queries":
                    google_reasoning = chunk.get("reasoning", "N/A")
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Google: {chunk.get('details', '')}", "reasoning": google_reasoning})
                elif chunk.get("type") == "error":
                     await connection_manager.send_json(user_id, {**chunk, "details": f"Google: {chunk.get('details', '')}"})
                     raise Exception(chunk.get("message","Error in Google query generation"))
            
            # Prefix objectives for uniqueness
            google_generated_queries_prefixed = [
                {**q, 'objective': f"google: {q.get('objective', f'Query {i+1}')}"} 
                for i, q in enumerate(google_generated_queries_raw)
            ]

            if google_generated_queries_prefixed:
                all_google_query_results = await _execute_queries_on_driver(
                    google_generated_queries_prefixed, google_driver, user_id, workflow_id, "google", connection_manager
                )
            await connection_manager.send_json(user_id, {
                "type": "status", "workflow_id": workflow_id, "step": "google_data_gathering",
                "status": "completed", "details": "Google data gathering for optimization complete."
            })
        except Exception as e:
            print(f"ERROR during Google data gathering for general optimization: {e}")
            await connection_manager.send_json(user_id, {
                "type": "error", "workflow_id": workflow_id, "step": "google_data_gathering",
                "message": f"Error during Google data gathering: {e}"
            })
            # Decide if we should proceed or stop; for now, we'll try Facebook

        # --- Step 2: Facebook Optimization Data Gathering ---
        await connection_manager.send_json(user_id, {
            "type": "status", "workflow_id": workflow_id, "step": "facebook_data_gathering",
            "status": "in_progress", "details": "Gathering data from Facebook for optimization..."
        })
        try:
            from facebook_arch.chains.optimization_workflow import OptimizationWorkflow as FacebookOptimizationWorkflow
            facebook_workflow = FacebookOptimizationWorkflow(schema_file=facebook_schema_path_abs)

            facebook_generated_queries_raw = []
            async for chunk in facebook_workflow.run(user_query):
                chunk["workflow_id"] = workflow_id
                if chunk.get("type") == "status" and chunk.get("step") == "generate_queries":
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Facebook: {chunk.get('details', '')}"})
                    if chunk.get("status") == "completed":
                        facebook_generated_queries_raw = chunk.get("generated_queries", [])
                elif chunk.get("type") == "reasoning_summary" and chunk.get("step") == "generate_queries":
                    facebook_reasoning = chunk.get("reasoning", "N/A")
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Facebook: {chunk.get('details', '')}", "reasoning": facebook_reasoning})
                elif chunk.get("type") == "error":
                    await connection_manager.send_json(user_id, {**chunk, "details": f"Facebook: {chunk.get('details', '')}"})
                    raise Exception(chunk.get("message","Error in Facebook query generation"))
            
            # Prefix objectives for uniqueness
            facebook_generated_queries_prefixed = [
                {**q, 'objective': f"facebook: {q.get('objective', f'Query {i+1}')}"} 
                for i, q in enumerate(facebook_generated_queries_raw)
            ]

            if facebook_generated_queries_prefixed:
                all_facebook_query_results = await _execute_queries_on_driver(
                    facebook_generated_queries_prefixed, facebook_driver, user_id, workflow_id, "facebook", connection_manager
                )
            await connection_manager.send_json(user_id, {
                "type": "status", "workflow_id": workflow_id, "step": "facebook_data_gathering",
                "status": "completed", "details": "Facebook data gathering for optimization complete."
            })
        except Exception as e:
            print(f"ERROR during Facebook data gathering for general optimization: {e}")
            await connection_manager.send_json(user_id, {
                "type": "error", "workflow_id": workflow_id, "step": "facebook_data_gathering",
                "message": f"Error during Facebook data gathering: {e}"
            })

        # --- Step 3: Combine Data & Generate Final Optimization Recommendations ---
        if not all_google_query_results and not all_facebook_query_results:
            final_report_text = "No data was found from Google or Facebook for your optimization query."
            await connection_manager.send_json(user_id, {
                "type": "status", "workflow_id": workflow_id, "step": "combined_analysis",
                "status": "skipped", "details": final_report_text
            })
        else:
            await connection_manager.send_json(user_id, {
                "type": "status", "workflow_id": workflow_id, "step": "combined_analysis",
                "status": "in_progress", "details": "Combining data and generating final optimization recommendations..."
            })
            
            combined_query_results = all_google_query_results + all_facebook_query_results # These now have prefixed objectives
            combined_reasoning = f"""Google Reasoning:
{google_reasoning}

Facebook Reasoning:
{facebook_reasoning}"""

            try:
                # Use OptimizationRecommendationGeneratorAgent (e.g., from langchain_arch)
                # For now, we'll use the Google one as there isn't a specific combined one
                from langchain_arch.agents.optimization_generator import OptimizationRecommendationGeneratorAgent as GoogleOptimizationAgent
                optimization_agent = GoogleOptimizationAgent() # Assuming default init is fine
                
                # Prepare data in the expected format
                # The optimization agent expects a data parameter with grouped results by objective
                grouped_results = {}
                for result_item in combined_query_results:
                    objective = result_item.get("objective", "Unknown Objective")
                    if objective not in grouped_results:
                        grouped_results[objective] = []
                    if result_item.get('data') is not None and isinstance(result_item['data'], list):
                        grouped_results[objective].extend(result_item['data'])
                
                # Call the optimization agent with the correct parameters
                optimization_agent_output = await optimization_agent.run(
                    query=user_query,
                    data=grouped_results,
                    user_id=user_id
                )
                
                if isinstance(optimization_agent_output, dict) and "report_sections" in optimization_agent_output:
                    final_report_sections = optimization_agent_output["report_sections"]
                else:
                    print(f"Warning: Optimization agent returned unexpected output: {optimization_agent_output}")
                    # Fallback to a simple structure
                    final_report_sections = [{
                        "title": "Combined Optimization Recommendations",
                        "content": "Unable to generate structured recommendations from the combined data."
                    }]

                # Graph Suggestions
                from langchain_arch.agents.graph_generator import GraphGeneratorAgent
                graph_agent = GraphGeneratorAgent()
                # Pass combined results (with prefixed objectives) to graph agent
                graph_agent_input = {"query": user_query, "data": combined_query_results} 
                graph_agent_output = await graph_agent.chain.ainvoke(graph_agent_input)
                if isinstance(graph_agent_output, dict) and 'graph_suggestions' in graph_agent_output:
                    # Ensure graph agent associates suggestions with the prefixed objectives
                    final_graph_suggestions = graph_agent_output['graph_suggestions']

            except Exception as e:
                print(f"ERROR during combined optimization analysis: {e}")
                import traceback
                tb_str = traceback.format_exc()
                await connection_manager.send_json(user_id, {
                    "type": "error", "workflow_id": workflow_id, "step": "combined_analysis",
                    "message": f"Error during combined optimization analysis: {e}", "details": tb_str
                })

        # --- Step 4: Send Final Result ---
        await connection_manager.send_json(user_id, {
            "type": "final_recommendation", # Consistent with single-platform optimization messages
            "workflow_id": workflow_id,
            "report_sections": final_report_sections,
            "graph_suggestions": final_graph_suggestions,
            "query_generation_reasoning": combined_reasoning, # Send combined reasoning
            "executed_queries": combined_query_results, # Add all executed query data
            "raw_google_results_count": len(all_google_query_results),
            "raw_facebook_results_count": len(all_facebook_query_results)
        })

    except Exception as e:
        # Broad exception for the whole workflow
        print(f"CRITICAL: Unhandled error in run_general_optimization_workflow_background: {e}")
        import traceback
        tb_str = traceback.format_exc()
        await connection_manager.send_json(user_id, {
            "type": "error", "workflow_id": workflow_id, "step": "workflow_execution",
            "message": f"Critical error during general optimization workflow: {e}", "details": tb_str
        })
    finally:
        await connection_manager.send_json(user_id, {
            "type": "status", "workflow_id": workflow_id, "step": "workflow_end",
            "status": "completed", "details": "General optimization workflow processing completed."
        })
        print(f"General optimization workflow {workflow_id} for user {user_id} completed.")


@app.post("/api/v1/workflows/trigger/general_optimization", response_model=TriggerResponse)
async def trigger_general_optimization(
    request: WorkflowTriggerRequest,
    background_tasks: BackgroundTasks
):
    """Trigger a General Optimization workflow combining Google and Facebook data."""
    workflow_id = f"general-optimization-{uuid.uuid4()}"
    background_tasks.add_task(
        run_general_optimization_workflow_background,
        user_query=request.query,
        user_id=request.user_id,
        workflow_id=workflow_id
    )
    return {
        "status": "initiated",
        "workflow_id": workflow_id,
        "message": "General Optimization workflow initiated. Results will be streamed via WebSocket."
    }

# --- Run the app (for local development) --- 
if __name__ == "__main__":
    print(f"Starting Uvicorn server...")
    print(f"Project Root: {project_root}")
    print(f"Looking for .env at: {dotenv_path_local}")
    print(f"Default Schema: {schema_path_abs}")
    print(f"Google Schema: {google_schema_path_abs}")
    print(f"Facebook Schema: {facebook_schema_path_abs}")
    uvicorn.run("main:app", host="0.0.0.0", port=8050) 