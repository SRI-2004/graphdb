import streamlit as st
import pandas as pd
import asyncio
import os
import sys
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase, RoutingControl

# --- Setup (Paths, Env Vars, Router Import) ---
# Use __file__ if available, otherwise fallback
if "__file__" in globals():
    dirname = os.path.dirname(__file__)
    project_root = dirname
else:
    project_root = os.getcwd()
    # st.warning(f"'__file__' not found. Using CWD as project root: {project_root}") # Optional warning

# Add project root to sys.path BEFORE importing local modules
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Try importing the Router AFTER setting the path
try:
    from langchain_arch.chains.router import Router
except ImportError:
    st.error(f"Failed to import Router from langchain_arch.chains.router. Ensure the path '{project_root}' is correct and the module exists.")
    st.stop() # Stop execution if Router can't be imported

# Load .env file using default search path - REMOVED FOR DEPLOYMENT
# Environment variables should be set directly in the deployment environment (e.g., Render dashboard)
# found_dotenv = load_dotenv() # Call without path, checks current/parent dirs
# if not found_dotenv:
#     st.warning(".env file not found in standard locations. Ensure it exists or env vars are set externally.")
    # Decide if this should be fatal or just a warning
    # st.stop() # Option: Make it fatal if .env is absolutely required

# Schema File Path (relative to project root)
SCHEMA_FILE_DEFAULT = "neo4j_schema.md"
schema_path_abs = os.path.abspath(os.path.join(project_root, SCHEMA_FILE_DEFAULT))

# Validate schema file existence
if not os.path.exists(schema_path_abs):
    st.error(f"Schema file not found at '{schema_path_abs}'")
    st.stop()

# Validate environment variables
required_env_vars = ["OPENAI_API_KEY", "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    st.error(f"Missing environment variables: {', '.join(missing_vars)}. Please check your .env file.")
    st.stop()
# --- End Setup ---

# --- Neo4j Driver Setup (Async) ---
# Reuse Neo4j connection details from environment
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Modify execute_neo4j_query to accept the driver
async def execute_neo4j_query(driver, query: str, database: str = "neo4j"):
    """Executes a Cypher query using the provided driver and returns results."""
    if not driver:
        return None, "Neo4j driver not available."

    try:
        async with driver.session(database=database) as session:
            response = await session.run(query)
            data = await response.data() # Gets list of dictionaries
            if data:
                df = pd.DataFrame(data)
                return df, None # Return DataFrame and no error
            else:
                return pd.DataFrame(), None # Return empty DataFrame, no error
    except Exception as e:
        error_message = f"Failed to execute query: {e}"
        print(f"ERROR executing Neo4j query:\nQuery: {query}\nError: {e}") # Log error
        return None, error_message # Return None for DataFrame and the error message

# --- Streamlit App ---

# Set page config FIRST
st.set_page_config(page_title="Insight Assistant", layout="wide")

# Then set the title
st.title("💡 Insight Assistant")

# --- Initialize Session State ---
if "messages" not in st.session_state:
    st.session_state.messages = [] # Stores chat history: {role: "user"/"assistant", content: "..."}
# NEW: Store list of query results from the last turn
if "query_results" not in st.session_state:
    st.session_state.query_results = [] # List of dicts: {objective, query, dataframe?, error?}
# NEW: Index for viewing query results
if "current_result_index" not in st.session_state:
    st.session_state.current_result_index = 0

# Initialize Neo4j Driver in Session State ONCE
if "neo4j_driver" not in st.session_state:
    try:
        st.session_state.neo4j_driver = AsyncGraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
        )
        print("Neo4j driver initialized and stored in session state.")
    except Exception as e:
        st.error(f"Failed to initialize Neo4j Driver: {e}")
        st.session_state.neo4j_driver = None # Indicate failure
        print(f"FATAL: Neo4j Driver initialization failed: {e}")

# Initialize Router in Session State ONCE
if "router" not in st.session_state:
    if st.session_state.get("neo4j_driver"): # Only init router if driver succeeded
        try:
            st.session_state.router = Router(schema_file=schema_path_abs)
            print("Router initialized.")
        except Exception as e:
            st.error(f"Error initializing the Router: {e}")
            st.session_state.router = None # Indicate failure
    else:
        st.warning("Router initialization skipped because Neo4j driver failed.")
        st.session_state.router = None

# --- Layout (Split Screen) ---
col_chat, col_data = st.columns([2, 3]) # Adjust ratio as needed (e.g., [1, 1] for equal)

# --- Left Pane: Chat Interface ---
with col_chat:
    st.header("Chat")

    # Display chat messages from history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            # Display reasoning/queries associated with assistant messages if stored
            if message["role"] == "assistant":
                # Check if reasoning content exists and is not empty/whitespace
                if "reasoning" in message and message["reasoning"] and str(message["reasoning"]).strip():
                    with st.expander("Reasoning & Details"):
                        st.markdown(str(message["reasoning"])) # Ensure it's string
                # Check if queries content exists and is not empty/whitespace
                if "queries" in message and message["queries"] and str(message["queries"]).strip():
                     with st.expander("Generated Queries"):
                        # Use st.markdown to render formatted queries correctly
                        st.markdown(str(message["queries"])) # Ensure it's string

    # --- Chat Input and Processing ---
    if prompt := st.chat_input("Ask about insights or optimizations..."):
        # Check if essential components are ready
        if not st.session_state.get("router") or not st.session_state.get("neo4j_driver"):
            st.error("Critical components (Router or Neo4j Driver) failed to initialize. Cannot process request.")
        else:
            # Add user message to chat history and display
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)

            # Define an async function to encapsulate the assistant's processing
            async def process_request():
                # --- Assistant Processing ---
                with st.chat_message("assistant"):
                    status_placeholder = st.empty() # For status updates
                    final_response_placeholder = st.empty() # For final text response
                    reasoning_placeholder = st.empty() # For reasoning expander
                    queries_placeholder = st.empty() # For queries expander

                    collected_final_text = ""
                    collected_queries_list = []
                    collected_reasoning_list = []
                    workflow_failed = False
                    step_where_failed = ""
                    error_details = ""
                    # NEW: Temporary list to hold results for this turn
                    current_turn_results = []

                    try:
                        status_placeholder.info("Processing request...")
                        # Retrieve driver from session state
                        neo4j_driver_instance = st.session_state.neo4j_driver
                        
                        print("--- Starting Router Processing ---") # DEBUG
                        async for chunk in st.session_state.router.run(user_query=prompt):
                            print(f"DEBUG: Received chunk: {chunk}") # DEBUG: Print every chunk
                            if isinstance(chunk, dict):
                                msg_type = chunk.get("type")
                                step = chunk.get("step", "Unknown Step")
                                status = chunk.get("status", "...")

                                # Correctly indented checks for msg_type
                                if msg_type == "status":
                                    details = chunk.get("details", "")
                                    status_text = f"**{step.replace('_', ' ').title()}**: {status.replace('_', ' ').title()}"
                                    if details: status_text += f" - {details}"
                                    status_placeholder.info(status_text)

                                    # Capture queries on completion
                                    if status == "completed":
                                        queries_key = "generated_queries" # Common key
                                        if queries_key in chunk:
                                            queries_data = chunk[queries_key]
                                            if isinstance(queries_data, list):
                                                # --- MODIFIED QUERY CAPTURE --- 
                                                for idx, q_item in enumerate(queries_data):
                                                    if isinstance(q_item, dict) and "query" in q_item:
                                                        # If it's a dict with a query key, add it (ensure objective exists or default)
                                                        collected_queries_list.append({
                                                            "objective": q_item.get("objective", f"Generated Query {len(collected_queries_list) + 1}"),
                                                            "query": q_item["query"]
                                                        })
                                                    elif isinstance(q_item, str):
                                                        # If it's just a string, assume it's the query and create a default dict
                                                        collected_queries_list.append({
                                                            "objective": f"Generated Query {len(collected_queries_list) + 1}",
                                                            "query": q_item
                                                        })
                                                    # else: ignore items that are neither dicts with queries nor strings
                                                # --- END MODIFIED QUERY CAPTURE --- 

                                elif msg_type == "reasoning_summary": # Indented correctly inside the `if isinstance` block
                                    reasoning_text = chunk.get("reasoning")
                                    print(f"DEBUG: Captured reasoning_summary ({step}): {reasoning_text}") # DEBUG
                                    print(f"DEBUG: Checking condition for adding reasoning: exists={bool(reasoning_text)}, not already added={not any(r[0] == step for r in collected_reasoning_list)}") # DEBUG
                                    if reasoning_text and not any(r[0] == step for r in collected_reasoning_list):
                                        collected_reasoning_list.append((step, reasoning_text))

                                elif msg_type == "final_insight": # Indented correctly inside the `if isinstance` block
                                    insight = chunk.get("insight", "No insight generated.")
                                    reasoning = chunk.get("reasoning")
                                    print(f"DEBUG: Captured final_insight ({step}): Insight exists={bool(insight)}, Reasoning exists={bool(reasoning)}") # DEBUG
                                    collected_final_text = insight # Assign insight here
                                    print(f"DEBUG: Assigned to collected_final_text: '{collected_final_text[:100]}...'") # DEBUG - Show partial assignment
                                    print(f"DEBUG: Checking condition for adding reasoning: exists={bool(reasoning)}, not already added={not any(r[0] == step for r in collected_reasoning_list)}") # DEBUG
                                    if reasoning and not any(r[0] == step for r in collected_reasoning_list):
                                        collected_reasoning_list.append((step, reasoning))

                                elif msg_type == "final_recommendations": # Indented correctly inside the `if isinstance` block
                                    report = chunk.get("report", "No report generated.")
                                    reasoning = chunk.get("reasoning")
                                    print(f"DEBUG: Captured final_recommendations ({step}): Report exists={bool(report)}, Reasoning exists={bool(reasoning)}") # DEBUG
                                    collected_final_text = report # Assign report here
                                    print(f"DEBUG: Assigned to collected_final_text: '{collected_final_text[:100]}...'") # DEBUG - Show partial assignment
                                    print(f"DEBUG: Checking condition for adding reasoning: exists={bool(reasoning)}, not already added={not any(r[0] == step for r in collected_reasoning_list)}") # DEBUG
                                    if reasoning and not any(r[0] == step for r in collected_reasoning_list):
                                        collected_reasoning_list.append((step, reasoning))

                                elif msg_type == "error": # Indented correctly inside the `if isinstance` block
                                    workflow_failed = True
                                    step_where_failed = step
                                    error_message = chunk.get("message", "An unknown error occurred.")
                                    status_placeholder.error(f"Failed at: {step_where_failed.replace('_', ' ').title()}")
                                    collected_final_text = f"**Workflow failed during {step_where_failed.replace('_', ' ').title()}.**"
                                    error_details = f"**Error Details ({step_where_failed.replace('_', ' ').title()}):**\n```\n{error_message}\n```"
                                    print(f"DEBUG: Captured error ({step})") # DEBUG
                                    break # Stop processing on error
                            # This else corresponds to `if isinstance(chunk, dict)`
                            # else:
                            #     print(f"DEBUG: Received non-dict chunk: {type(chunk)}")

                    except Exception as e:
                        import traceback
                        tb_str = traceback.format_exc()
                        workflow_failed = True; step_where_failed = "Main Processing Loop"
                        error_details = f"**Unexpected Error:**\n```\n{e}\n\n{tb_str}\n```"
                        collected_final_text += "\n\n**Workflow failed unexpectedly.**"
                        status_placeholder.error("Workflow Error")
                        print(f"FATAL ERROR in Streamlit handler: {e}\n{tb_str}") # Log error
                        print(f"DEBUG: Exception during router processing: {e}") # DEBUG

                    print(f"DEBUG: Collected reasoning list AFTER loop: {collected_reasoning_list}") # DEBUG
                    print(f"DEBUG: collected_final_text AFTER loop: '{collected_final_text[:100]}...'") # DEBUG
                    # --- Finalize Assistant Message Content ---
                    status_placeholder.empty() # Clear status message

                    # Combine reasoning
                    reasoning_content = ""
                    if workflow_failed and error_details:
                        reasoning_content += error_details + "\n\n---\n\n"
                    elif workflow_failed:
                         reasoning_content += f"**Error Occurred:** Workflow stopped during the '{step_where_failed.replace('_', ' ').title()}' step.\n\n---\n\n"
                    reasoning_content += "\n\n---\n\n".join([f"**{step.replace('_', ' ').title()}:**\n{str(reason).strip()}" for step, reason in collected_reasoning_list if str(reason).strip()])
                    print(f"DEBUG: Final reasoning_content string: '{reasoning_content}'") # DEBUG

                    # Combine queries for display
                    queries_display_content = ""
                    if not workflow_failed and collected_queries_list:
                        queries_display_content = "\n\n".join(
                            [f"**{q.get('objective', f'Query {i+1}')}:**\n```cypher\n{q.get('query', 'N/A').strip()}\n```"
                             for i, q in enumerate(collected_queries_list)]
                        )
                    print(f"DEBUG: Constructed queries_display_content:\n{queries_display_content}") # DEBUG: Check the constructed string

                    # --- Execute Queries & Update Data Pane ---
                    query_execution_errors = []
                    if not workflow_failed and collected_queries_list:
                        with status_placeholder: # Temporarily reuse status for query execution info
                            st.info(f"Found {len(collected_queries_list)} queries. Executing...")
                            for i, q_item in enumerate(collected_queries_list):
                                query_text = q_item.get("query")
                                objective = q_item.get("objective", f"Query {i+1}")
                                result_item = {"objective": objective, "query": query_text} # Prepare result dict
                                if query_text:
                                    st.info(f"Running: {objective}...")
                                    df, error = await execute_neo4j_query(neo4j_driver_instance, query_text)
                                    if error:
                                        err_msg = f"**Failed to execute query for '{objective}':**\n```\n{error}\n```"
                                        query_execution_errors.append(err_msg)
                                        result_item["error"] = error # Store error in result dict
                                        print(f"Query Execution Error: {objective} - {error}") # Log
                                    elif df is not None: 
                                        result_item["dataframe"] = df # Store dataframe in result dict
                                        print(f"Query Execution Success: {objective} - {len(df)} rows") # Log
                                    # else: # df is None but no error (e.g., driver issue handled in execute_neo4j_query)
                                    #     result_item["error"] = "Failed to get data (driver issue?)"
                                # Add the result (with df or error) to the list for this turn
                                current_turn_results.append(result_item)
                                    
                            st.info("Query execution finished.")
                            # Update session state with ALL results and reset index
                            st.session_state.query_results = current_turn_results
                            st.session_state.current_result_index = 0 # Start viewing from the first result
                            
                    # --- Update Assistant Message in Chat ---
                    final_response_placeholder.markdown(collected_final_text if collected_final_text.strip() else "Processing complete.")
                    if reasoning_content.strip():
                         with reasoning_placeholder.expander("Reasoning & Details", expanded=(workflow_failed or bool(query_execution_errors))): # Expand if errors
                            st.markdown(reasoning_content)
                            # Add query execution errors to reasoning
                            if query_execution_errors:
                                st.markdown("\n\n---\n\n**Query Execution Issues:**")
                                st.error("\n\n".join(query_execution_errors))

                    if queries_display_content.strip():
                        with queries_placeholder.expander("Generated Queries"):
                            # Use st.markdown to render formatted queries correctly
                            st.markdown(queries_display_content)

                    # Append final assistant message structure to history
                    # Ensure reasoning and queries are added correctly
                    print(f"DEBUG: BEFORE creating final dict: collected_final_text.strip() is {bool(collected_final_text.strip())}") # DEBUG
                    print(f"DEBUG: BEFORE creating final dict: reasoning_content.strip() is {bool(reasoning_content.strip())}") # DEBUG
                    print(f"DEBUG: BEFORE creating final dict: queries_display_content.strip() is {bool(queries_display_content.strip())}") # DEBUG: Add this check
                    final_message_dict = {
                        "role": "assistant",
                        "content": collected_final_text if collected_final_text.strip() else "Processing complete.",
                        "reasoning": reasoning_content, # Make sure this is passed
                        "queries": queries_display_content # Make sure this is passed
                    }
                    print(f"DEBUG: Appending to messages: {final_message_dict}") # DEBUG
                    st.session_state.messages.append(final_message_dict)

            # Run the async function using asyncio
            try:
                # Primary strategy: Use asyncio.run() as it handles loop creation/cleanup
                print("Attempting to run process_request using asyncio.run()")
                asyncio.run(process_request())
                print("asyncio.run() completed successfully.")

            except RuntimeError as e:
                if "cannot be called from a running event loop" in str(e):
                    # Fallback strategy: If asyncio.run fails because a loop IS running,
                    # apply nest_asyncio and retry.
                    print("asyncio.run() failed as loop is running. Applying nest_asyncio patch.")
                    st.warning("Nested asyncio detected. Applying nest_asyncio patch.")
                    try:
                        import nest_asyncio
                        nest_asyncio.apply()
                        print("nest_asyncio applied. Retrying asyncio.run().")
                        # Retry running the function after applying the patch
                        asyncio.run(process_request())
                        print("asyncio.run() succeeded after nest_asyncio patch.")
                    except Exception as final_e:
                        st.error(f"Error even after applying nest_asyncio: {final_e}")
                        print(f"FATAL: Error after nest_asyncio: {final_e}")
                else:
                    # Re-raise other RuntimeErrors (like 'no current event loop' - though run should handle this)
                    st.error(f"Unhandled Runtime error during async processing: {e}")
                    print(f"FATAL: Unhandled Runtime error: {e}")
            except Exception as e:
                 st.error(f"General error processing request: {e}")
                 print(f"FATAL: General error in request processing: {e}")

            # --- Trigger Rerun to Update Data Pane & Chat ---
            st.rerun()


# --- Right Pane: Data Display/Editor ---
with col_data:
    st.header("Data Explorer")

    # Check if there are any results to display
    if not st.session_state.query_results:
        st.info("Ask the assistant for insights! Query results will appear here.")
    else:
        # Navigation Buttons
        num_results = len(st.session_state.query_results)
        current_index = st.session_state.current_result_index
        
        # Disable buttons if at bounds
        prev_disabled = (current_index <= 0)
        next_disabled = (current_index >= num_results - 1)

        nav_cols = st.columns([1, 1, 5]) # Adjust ratios for button spacing
        with nav_cols[0]:
            if st.button("⬅️ Previous", disabled=prev_disabled, use_container_width=True):
                st.session_state.current_result_index -= 1
                st.rerun()
        with nav_cols[1]:
            if st.button("Next ➡️", disabled=next_disabled, use_container_width=True):
                st.session_state.current_result_index += 1
                st.rerun()
        with nav_cols[2]:
             st.markdown(f"_Result {current_index + 1} of {num_results}_")

        # Get the specific result to display based on index
        result_to_display = st.session_state.query_results[current_index]
        objective = result_to_display.get("objective", "N/A")
        query = result_to_display.get("query", "N/A")
        dataframe = result_to_display.get("dataframe") # Could be None
        error = result_to_display.get("error")

        # Display Query Info
        st.markdown(f"Displaying results for: **{objective}**")
        with st.expander("Show Query"):
            st.code(query, language="cypher")

        # Display Data or Error
        if error:
            st.error(f"Failed to execute query:\n```\n{error}\n```")
        elif dataframe is not None:
            if dataframe.empty:
                st.success("Query executed successfully, but returned no data.")
            else:
                st.info("You can edit the data below (changes are not saved back to Neo4j yet).")
                st.data_editor(dataframe, use_container_width=True, key=f"data_editor_{current_index}") # Use index in key
        else:
             st.warning("No data available for this query result.")

# --- Neo4j Driver Cleanup ---
# Driver stored in session state will be garbage collected when session ends.
# Explicit close isn't strictly necessary for standard Streamlit running,
# but could be added using atexit or similar if needed for specific environments.
print("Streamlit script execution finished.") 