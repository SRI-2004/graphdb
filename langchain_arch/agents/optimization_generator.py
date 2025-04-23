import os
import json
import asyncio
from typing import Dict, Any, AsyncIterator, List

from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnableConfig, RunnablePassthrough
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tracers.log_stream import LogEntry

from ..prompts.optimization_generator import create_optimization_generator_prompt

# Configuration
LLM_MODEL_NAME = "gpt-4o"

class OptimizationRecommendationGeneratorAgent:
    """
    Agent that synthesizes optimization recommendations from multiple data sources.
    Uses LangChain's built-in streaming (.astream_log).
    """
    def __init__(self):
        print(">>> Using UPDATED OptimizationRecommendationGeneratorAgent! <<<") # Add verification print
        self.prompt: ChatPromptTemplate = create_optimization_generator_prompt()
        self.llm = ChatOpenAI(
            model=LLM_MODEL_NAME,
            temperature=0.1,
            streaming=True,
        )
        self.chain = (
            RunnablePassthrough.assign(
                data=lambda x: json.dumps(x['data'], indent=2)
            )
            | self.prompt
            | self.llm
        
            | JsonOutputParser()
        )

    async def run(self, query: str, data: Dict[str, List[Dict[str, Any]]]) -> AsyncIterator[LogEntry]:
        """
        Executes the recommendation generation chain using astream_log.

        Args:
            query: The original user's natural language optimization request.
            data: A dictionary where keys are objectives (str) and values are
                  lists of dictionaries (results from Neo4j for that objective).

        Yields:
            LogEntry objects representing the execution log stream.
        """
        input_data = {"query": query, "data": data}
        # Use astream_log
        async for chunk in self.chain.astream_log(
            input_data,
            include_names=["ChatOpenAI", "JsonOutputParser"],
            include_types=["llm", "parser"]
        ):
            yield chunk

# Example usage (for testing - requires .env)
if __name__ == '__main__':
    import os
    import asyncio
    from dotenv import load_dotenv

    load_dotenv()

    async def main():
        agent = OptimizationRecommendationGeneratorAgent()

        test_query = "Suggest how I can improve the performance of my search campaigns."
        test_data = {
          "Find high spending ad groups with low conversions": [
            { "adGroupId": 123, "adGroupName": "Old Generic Terms", "totalCost": 500, "totalConversions": 1 }
          ],
          "Identify keywords with low Quality Score": [
            { "adGroupId": 123, "criterionId": 987, "keywordText": "cheap stuff", "qualityScore": 3 },
            { "adGroupId": 456, "criterionId": 654, "keywordText": "buy now", "qualityScore": 2 }
          ],
          "Find ad groups with low Click-Through Rate (CTR)": [
            { "adGroupId": 456, "adGroupName": "Broad Match Explore", "avgCTR": 0.005, "totalImpressions": 5000 }
          ],
          "Campaigns with recent status changes": []
        }

        print(f"--- Testing Optimization Recommendation Generator ---")
        print(f"Original Query: {test_query}")
        print(f"Input Data: {json.dumps(test_data, indent=2)}")

        final_result = None
        log_entries = []
        async for entry in agent.run(query=test_query, data=test_data):
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
