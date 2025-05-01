import argparse
import asyncio
import json
import os
import sys
from dotenv import load_dotenv

# Ensure the package modules can be imported
# Get the absolute path of the directory containing main.py
dirname = os.path.dirname(__file__)
# Add the parent directory (project root) to sys.path
project_root = os.path.abspath(os.path.join(dirname, os.pardir))
sys.path.insert(0, project_root)

# Now import from the langchain_arch package
from langchain_arch.chains.router import Router

# Load environment variables from .env file at the project root
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

async def main(query: str, schema_file: str):
    """
    Main execution function.
    Initializes the Router and runs the query, printing streamed results.
    """
    print(f"Processing query: \"{query}\"")
    print(f"Using schema file: {schema_file}")
    print("--- Starting Workflow ---")

    router = Router(schema_file=schema_file)

    try:
        async for chunk in router.run(user_query=query):
            # Pretty print the JSON chunks
            print(json.dumps(chunk, indent=2))
            # Check if the chunk indicates an error and break the loop
            if isinstance(chunk, dict) and chunk.get("type") == "error":
                print("\n--- Error detected in workflow chunk, stopping iteration ---", file=sys.stderr) # Print to stderr
                break # Exit the loop
            # Optional: Add a small delay for better readability of the stream
            # await asyncio.sleep(0.05)

    except Exception as e:
        print(f"\n--- Workflow Error --- ")
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n--- Workflow Complete ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the LangChain Neo4j Agentic Architecture.")
    parser.add_argument("query", type=str, help="The natural language query to process.")
    parser.add_argument(
        "--schema",
        type=str,
        default="neo4j_schema.md",
        help="Path to the Neo4j schema Markdown file (relative to project root). Default: neo4j_schema.md"
    )
    args = parser.parse_args()

    # Construct the absolute path to the schema file based on the project root
    schema_path_abs = os.path.abspath(os.path.join(project_root, args.schema))

    # Check if schema file exists before starting
    if not os.path.exists(schema_path_abs):
        print(f"Error: Schema file not found at '{schema_path_abs}'")
        print(f"Please ensure the file '{args.schema}' exists in the project root directory ('{project_root}') or provide the correct path.")
        sys.exit(1)

    # Check for necessary environment variables
    required_env_vars = ["OPENAI_API_KEY", "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these in your .env file or environment.")
        sys.exit(1)

    # Run the async main function
    try:
        asyncio.run(main(args.query, schema_path_abs))
    except KeyboardInterrupt:
        print("\nExecution interrupted by user.")
        sys.exit(0)
