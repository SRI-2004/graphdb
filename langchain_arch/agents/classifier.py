import os
import json
from typing import Dict, Any, AsyncIterator

from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnableConfig
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tracers.log_stream import LogEntry

from ..prompts.classifier import create_classifier_prompt

# Ensure OPENAI_API_KEY is set (handled by load_dotenv in utils/neo4j_utils.py or main.py)
# Consider adding load_dotenv() here if this module might be run independently

# Configuration
LLM_MODEL_NAME = "gpt-4o" # Or your preferred GPT-4 model

class ClassifierAgent:
    """
    Agent responsible for classifying user queries into 'insight' or 'optimization' workflows.
    Uses LangChain's built-in streaming (.astream_log).
    """
    def __init__(self):
        self.prompt: ChatPromptTemplate = create_classifier_prompt()
        self.llm = ChatOpenAI(
            model=LLM_MODEL_NAME,
            temperature=0,
            streaming=True, # Still needed for token streaming within log
        )
        # Define the chain: prompt -> llm -> json_parser
        self.chain = self.prompt | self.llm | JsonOutputParser()

    async def run(self, query: str) -> AsyncIterator[LogEntry]:
        """
        Executes the classification chain using astream_log and streams LogEntry chunks.

        Args:
            query: The user's natural language query.

        Yields:
            LogEntry objects representing the execution log stream.
        """
        # Use astream_log which handles streaming internally
        # No need for external callback handlers or queues
        async for chunk in self.chain.astream_log(
            {"query": query},
            include_names=["ChatOpenAI", "JsonOutputParser"], # Specify components to include
            include_types=["llm", "parser"] # Specify event types
        ):
            # The chunk IS the LogEntry object
            yield chunk

# Example usage (for testing)
if __name__ == '__main__':
    import asyncio
    from dotenv import load_dotenv
    load_dotenv() # Load .env file for API key

    async def main():
        classifier = ClassifierAgent()
        test_query_insight = "Show me the top 5 ads by clicks last week."
        test_query_optimization = "How can I reduce the cost per conversion?"

        print(f"--- Testing Insight Query: '{test_query_insight}' ---")
        log_entries_insight = []
        async for entry in classifier.run(test_query_insight):
            print(entry) # Print the raw LogEntry
            log_entries_insight.append(entry)

        print(f"\n--- Testing Optimization Query: '{test_query_optimization}' ---")
        log_entries_opt = []
        async for entry in classifier.run(test_query_optimization):
            print(entry)
            log_entries_opt.append(entry)

        # Example: Extract final output from the log (might need refinement)
        print("\n--- Example: Extracting Final Output ---")
        final_output_insight = None
        for entry in reversed(log_entries_insight):
            if entry.name == "JsonOutputParser" and entry.state == "end":
                final_output_insight = entry.data.get('output')
                break
        print(f"Insight Final Output: {final_output_insight}")

        final_output_opt = None
        for entry in reversed(log_entries_opt):
            if entry.name == "JsonOutputParser" and entry.state == "end":
                 final_output_opt = entry.data.get('output')
                 break
        print(f"Optimization Final Output: {final_output_opt}")

    # In a Jupyter notebook or environment with an event loop:
    # await main()
    # For standard Python script:
    asyncio.run(main())
