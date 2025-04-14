import chainlit as cl
import asyncio
import json
import os
import sys
from dotenv import load_dotenv
from langchain_core.tracers.log_stream import RunLogPatch
from chainlit.element import Text # Correct import

# --- Setup (Imports, Paths, Validation) ---
# Use __file__ if available, otherwise fallback for interactive environments
if "__file__" in globals():
    dirname = os.path.dirname(__file__)
    project_root = dirname
else:
    # Fallback for interactive sessions or environments where __file__ is not defined
    project_root = os.getcwd()
    print(f"Warning: '__file__' not found. Using current working directory as project root: {project_root}")

sys.path.insert(0, project_root)
# Ensure this path is correct relative to your project root
from langchain_arch.chains.router import Router

dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

SCHEMA_FILE_DEFAULT = "neo4j_schema.md" # Ensure this file exists relative to project_root
schema_path_abs = os.path.abspath(os.path.join(project_root, SCHEMA_FILE_DEFAULT))

# Validate schema file existence
if not os.path.exists(schema_path_abs):
    sys.exit(f"FATAL ERROR: Schema file not found at '{schema_path_abs}'")

# Validate environment variables
required_env_vars = ["OPENAI_API_KEY", "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    sys.exit(f"FATAL ERROR: Missing env vars: {', '.join(missing_vars)}")
# --- End Setup ---

@cl.on_chat_start
async def start_chat():
    cl.user_session.set("schema_filename", SCHEMA_FILE_DEFAULT)
    await cl.Message(content=f"Welcome! Ready for insights/optimizations (Schema: {SCHEMA_FILE_DEFAULT}).").send()

@cl.on_message
async def main(message: cl.Message):
    user_query = message.content
    schema_path = schema_path_abs

    try:
        router = Router(schema_file=schema_path)
    except Exception as e:
        await cl.Message(content=f"Error initializing the Router: {e}").send()
        return

    # --- Initialization for this message ---
    final_answer_msg = cl.Message(content="Processing your request...", author="Assistant", elements=[])
    await final_answer_msg.send()

    status_msg = cl.Message(content="Initializing...", author="Status", parent_id=final_answer_msg.id)
    await status_msg.send()

    # --- Data collection during run ---
    collected_final_text = ""
    collected_queries = []
    collected_reasoning = []
    workflow_failed = False
    step_where_failed = ""
    error_details = ""

    try:
        async for chunk in router.run(user_query=user_query):
            if isinstance(chunk, dict):
                msg_type = chunk.get("type")
                step = chunk.get("step", "Unknown")
                status = chunk.get("status", "...")

                # Update Status Message (intermediate feedback)
                if msg_type == "status":
                    details = chunk.get("details", "")
                    current_status_content = f"**{step.replace('_', ' ').title()}**: {status.replace('_', ' ').title()}"
                    if details: current_status_content += f" - {details}"
                    is_failure_status = status_msg.content.startswith("Failed at:") or status_msg.content == "Workflow Error"
                    if not (workflow_failed and is_failure_status):
                         status_msg.content = current_status_content
                         await status_msg.update() # Update for intermediate steps

                    # Capture queries on completion
                    if status == "completed":
                        if step == "generate_cypher" and "generated_queries" in chunk:
                            queries_data = chunk["generated_queries"]; collected_queries.extend(queries_data) if isinstance(queries_data, list) else None
                        elif step == "generate_opt_queries" and "generated_queries" in chunk:
                            queries_data = chunk["generated_queries"]; collected_queries.extend(queries_data) if isinstance(queries_data, list) else None

                # Capture Reasoning
                elif msg_type == "reasoning_summary":
                    reasoning_text = chunk.get("reasoning")
                    if reasoning_text and not any(r[0] == step for r in collected_reasoning): collected_reasoning.append((step, reasoning_text))

                # Capture Final Insight/Recommendations (Store text, add reasoning)
                elif msg_type == "final_insight":
                    insight = chunk.get("insight", "No insight generated.")
                    collected_final_text = insight # Overwrite previous text
                    reasoning = chunk.get("reasoning")
                    # Add reasoning only if not already captured for this step
                    if reasoning and not any(r[0] == step for r in collected_reasoning):
                        collected_reasoning.append((step, reasoning))
                
                elif msg_type == "final_recommendations":
                    # Use the 'report' key yielded by the workflow
                    report_content = chunk.get("report", "No optimization report generated.") 
                    collected_final_text = report_content # Display the full report as the main text
                    reasoning = chunk.get("reasoning")
                    # Add reasoning only if not already captured for this step
                    if reasoning and not any(r[0] == step for r in collected_reasoning):
                        collected_reasoning.append((step, reasoning))

                # Handle Errors
                elif msg_type == "error":
                    workflow_failed = True; step_where_failed = step
                    error_message = chunk.get("message", "An unknown error occurred.")
                    status_msg.content = f"Failed at: {step_where_failed.replace('_', ' ').title()}"
                    await status_msg.update() # Show failure status immediately
                    collected_final_text = f"**Workflow failed during {step_where_failed.replace('_', ' ').title()}.**"
                    # Make error details formatting consistent
                    error_details = f"**Error Details ({step_where_failed.replace('_', ' ').title()}):**\n```\n{error_message}\n```"
                    break

            # else: print(f"Warning: Received non-dict chunk: {type(chunk)}")

    except Exception as e:
        workflow_failed = True; step_where_failed = "Main Processing Loop"
        import traceback; tb_str = traceback.format_exc()
        error_details = f"**Unexpected Error:**\n```\n{e}\n\n{tb_str}\n```" # Consistent error formatting
        collected_final_text = f"\n\n**Workflow failed.** An unexpected error occurred."
        status_msg.content = "Workflow Error"; await status_msg.update() # Show failure status immediately
        print(f"FATAL ERROR in main handler: {e}"); traceback.print_exc()

    finally:
        # --- Construct Final Message Content and Elements ---
        final_elements = []

        # Determine final text content
        if not collected_final_text.strip():
            if workflow_failed: final_content = f"Workflow failed during the '{step_where_failed.replace('_', ' ').title()}' step." if step_where_failed else "Workflow failed."
            else: final_content = "Processing completed, but no final output was generated."
        else:
            # Add a bit of space before elements if there's main content
            final_content = collected_final_text.strip() + "\n" # Add a newline at the end of main text

        # --- Prepare Queries Element ---
        if not workflow_failed and collected_queries:
            formatted_queries_content = []
            for i, q_item in enumerate(collected_queries):
                query_text = q_item if isinstance(q_item, str) else q_item.get("query", "N/A")
                title = f"Query {i+1}" if isinstance(q_item, str) else q_item.get("objective", f"Objective {i+1}")
                # Ensure clean formatting for each query block
                formatted_queries_content.append(f"**{title}:**\n```cypher\n{query_text.strip()}\n```") # Use strip() on query_text
            if formatted_queries_content:
                # Join query blocks with double newlines for spacing
                queries_markdown = "\n\n".join(formatted_queries_content)
                final_elements.append(
                    Text(
                        name="쿼 Generated Queries", # Added emoji for visual cue
                        content=queries_markdown,
                        display="inline"
                    )
                )

        # --- Prepare Reasoning Element
        formatted_reasoning_content = []
        # Add error details first if workflow failed
        if workflow_failed and error_details:
             formatted_reasoning_content.append(error_details)
        elif workflow_failed: # Fallback generic error message
             formatted_reasoning_content.append(f"**Error Occurred:** Workflow stopped during the '{step_where_failed.replace('_', ' ').title()}' step.")

        # Add collected reasoning steps, handling non-string reasoning
        for step, reasoning in collected_reasoning:
            reasoning_text = ""
            if isinstance(reasoning, str):
                reasoning_text = reasoning.strip()
            elif isinstance(reasoning, list):
                # Join list elements into a single string
                reasoning_text = "\n".join(map(str, reasoning)).strip()
            else:
                # Attempt to convert other types to string
                try:
                    reasoning_text = str(reasoning).strip()
                except Exception:
                    reasoning_text = "[Could not format reasoning step]"
            
            # Only add if there is actual text after processing
            if reasoning_text:
                 formatted_reasoning_content.append(f"**{step.replace('_', ' ').title()}:**\n{reasoning_text}")

        if formatted_reasoning_content:
            # Join reasoning sections with a horizontal rule and double newlines
            reasoning_markdown = "\n\n---\n\n".join(formatted_reasoning_content)
            final_elements.append(
                Text(
                    name="⚙️ Reasoning & Details",
                    content=reasoning_markdown,
                    display="inline"
                )
            )

        # --- Update the MAIN message with FINAL content AND elements ---
        final_answer_msg.content = final_content # Already has trailing newline if content exists
        final_answer_msg.elements = final_elements
        await final_answer_msg.update()

        # --- Explicitly clear the status message at the very end ---
        status_msg.content = "" # Set content to empty string
        await status_msg.update() # Update the status message one last time to clear it

# To run: chainlit run your_script_name.py -w