import os
import json
import asyncio
from typing import Dict, Any, AsyncIterator

from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnableConfig, RunnablePassthrough
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tracers.log_stream import LogEntry

from ..prompts.optimization_query_generator import create_optimization_query_generator_prompt

# Configuration
LLM_MODEL_NAME = "gpt-4o"

class OptimizationQueryGeneratorAgent:
    """
    Agent that decomposes optimization request and generates multiple Cypher queries.
    Uses LangChain's built-in streaming (.astream_log).
    """
    def __init__(self):
        self.prompt: ChatPromptTemplate = create_optimization_query_generator_prompt()
        self.llm = ChatOpenAI(
            model=LLM_MODEL_NAME,
            temperature=0,
            streaming=True,
        )
        self.chain = (
            RunnablePassthrough.assign(schema=lambda x: x['schema'])
            | self.prompt
            | self.llm
            | JsonOutputParser()
        )

    async def run(self, query: str, schema: str) -> AsyncIterator[LogEntry]:
        """
        Executes the optimization query generation chain using astream_log.

        Args:
            query: The user's natural language optimization request.
            schema: The Neo4j graph schema in Markdown format.

        Yields:
            LogEntry objects representing the execution log stream.
        """
        input_data = {"query": query, "schema": schema}
        # Use astream_log
        async for chunk in self.chain.astream_log(
            input_data,
            include_names=["ChatOpenAI", "JsonOutputParser"],
            include_types=["llm", "parser"]
        ):
            yield chunk

# Example usage (for testing - requires .env, schema)
if __name__ == '__main__':
    import os
    import asyncio
    from dotenv import load_dotenv
    from ..utils.neo4j_utils import Neo4jDatabase # To load schema

    load_dotenv()

    async def main():
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        schema_path = os.path.join(project_root, 'neo4j_schema.md')
        print(f"Attempting to load schema from: {schema_path}")
        schema_content = None
        try:
            with Neo4jDatabase() as db:
                 schema_content = db.get_schema_markdown(schema_path)
            if not schema_content and os.path.exists(schema_path):
                 print("DB util failed, attempting direct read.")
                 with open(schema_path, 'r', encoding='utf-8') as f: schema_content = f.read()
            if not schema_content:
                 print("Schema file not found or empty. Exiting test."); return
        except Exception as e:
             print(f"Error loading schema: {e}")
             if os.path.exists(schema_path):
                 try:
                     with open(schema_path, 'r', encoding='utf-8') as f: schema_content = f.read()
                     print("Using schema from file as DB connection failed.")
                 except Exception as e2:
                     print(f"Failed to read schema file: {e2}. Exiting."); return
             else:
                 print("Schema file not found, cannot proceed."); return
        if not schema_content: print("Schema empty. Exiting."); return

        agent = OptimizationQueryGeneratorAgent()
        test_query = "How can I improve the CTR for my low-performing ads?"

        print(f"--- Testing Optimization Query Generator: '{test_query}' ---")
        print(f"--- Using Schema (first 200 chars): ---\n{schema_content[:200]}...\n---")

        final_result = None
        log_entries = []
        async for entry in agent.run(query=test_query, schema=schema_content):
            print(f"Stream Chunk: {entry}")
            log_entries.append(entry)
            if entry.name == "JsonOutputParser" and entry.state == "end":
                final_result = entry.data.get('output')

        print("\n--- Streaming Complete ---")
        if final_result:
            print("\n--- Final Parsed Output ---")
            print(json.dumps(final_result, indent=2))
        else:
            print("\n--- No Final Output Received ---")

    asyncio.run(main())
