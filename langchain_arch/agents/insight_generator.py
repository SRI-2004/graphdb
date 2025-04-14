import os
import json
from typing import Dict, Any, AsyncIterator, List

from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnableConfig, RunnablePassthrough
from langchain_core.output_parsers import JsonOutputParser
from langchain.output_parsers import OutputFixingParser
from langchain_core.prompts import ChatPromptTemplate

from ..prompts.insight_generator import create_insight_generator_prompt

# Configuration
LLM_MODEL_NAME = "gpt-4o"

class InsightGeneratorAgent:
    """
    Agent that synthesizes natural language insights from query results.
    Now uses OutputFixingParser.
    """
    def __init__(self):
        self.prompt: ChatPromptTemplate = create_insight_generator_prompt()
        self.llm = ChatOpenAI(
            model=LLM_MODEL_NAME,
            temperature=0.1,
            streaming=True,
        )
        
        # Create the base parser
        base_parser = JsonOutputParser()
        
        # Create the OutputFixingParser, wrapping the base parser and the LLM
        self.output_parser = OutputFixingParser.from_llm(parser=base_parser, llm=self.llm)
        
        # Update the chain to use the fixing parser
        self.chain = (
            RunnablePassthrough.assign(
                data=lambda x: json.dumps(x['data'], indent=2)
            )
            | self.prompt
            | self.llm
        )

# Example usage (for testing - requires .env)
if __name__ == '__main__':
    import os
    import asyncio
    from dotenv import load_dotenv

    load_dotenv()

    async def main():
        agent = InsightGeneratorAgent()

        test_query = "What were the top 2 campaigns by clicks last week?"
        test_data = [
            { "campaignName": "Summer Sale", "totalClicks": 1500 },
            { "campaignName": "New Product Launch", "totalClicks": 1250 },
        ]

        print(f"--- Testing Insight Generator ---")
        print(f"Original Query: {test_query}")
        print(f"Input Data: {json.dumps(test_data, indent=2)}")

        final_result = None
        async for entry in agent.chain.astream_log(
            {"query": test_query, "data": test_data},
            include_names=["ChatOpenAI"],
            include_types=["llm"]
        ):
            print(f"Stream Chunk: {entry}")
            if entry.name == "ChatOpenAI" and entry.state == "end":
                 final_result = entry.data.get('output')

        print("\n--- Streaming Complete ---")
        if final_result:
            print("\n--- Final Parsed Output ---")
            print(json.dumps(final_result, indent=2))
        else:
            print("\n--- No Final Output Received ---")

        # Test with empty data
        test_query_empty = "Show ads with CTR below 0.1%."
        test_data_empty = []
        print(f"\n--- Testing Insight Generator (Empty Data) ---")
        print(f"Original Query: {test_query_empty}")
        print(f"Input Data: {json.dumps(test_data_empty, indent=2)}")

        final_result_empty = None
        async for entry in agent.chain.astream_log(
            {"query": test_query_empty, "data": test_data_empty},
            include_names=["ChatOpenAI"],
            include_types=["llm"]
        ):
            print(f"Stream Chunk: {entry}")
            if entry.name == "ChatOpenAI" and entry.state == "end":
                 final_result_empty = entry.data.get('output')

        print("\n--- Streaming Complete (Empty Data) ---")
        if final_result_empty:
            print("\n--- Final Parsed Output (Empty Data) ---")
            print(json.dumps(final_result_empty, indent=2))
        else:
            print("\n--- No Final Output Received (Empty Data) ---")

    asyncio.run(main())
