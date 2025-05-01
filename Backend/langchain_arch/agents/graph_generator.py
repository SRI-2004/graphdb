import os
import json
from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate

from ..prompts.graph_generator import create_graph_generator_prompt

# Configuration (consider making this configurable)
LLM_MODEL_NAME = "gpt-4o" 

class GraphGeneratorAgent:
    """
    Agent that analyzes query results and suggests a graph visualization.
    Outputs a JSON structure containing graph type and column mappings.
    """
    def __init__(self):
        # Initialize LLM (should ideally be passed in, but following current pattern)
        try:
            openai_api_key = os.getenv("OPENAI_API_KEY")
            if not openai_api_key:
                raise ValueError("OPENAI_API_KEY environment variable not set.")
            self.llm = ChatOpenAI(
                model=LLM_MODEL_NAME,
                temperature=0.1, # Low temp for predictable JSON output
                # streaming=False, # No streaming needed for this agent usually
                model_kwargs={"response_format": {"type": "json_object"}} # Force JSON output
            )
        except Exception as e:
            print(f"CRITICAL: Failed to initialize LLM in GraphGeneratorAgent: {e}")
            raise RuntimeError(f"Failed to initialize LLM: {e}") from e
            
        self.prompt: ChatPromptTemplate = create_graph_generator_prompt()
        self.output_parser = JsonOutputParser()
        
        self.chain = (
            RunnablePassthrough.assign(
                # Ensure the list of result objects is passed as a JSON string to the prompt
                data=lambda x: json.dumps(x.get('data', []), indent=2) 
            )
            | self.prompt
            | self.llm
            | self.output_parser
        )

# Example usage (for testing - requires .env)
if __name__ == '__main__':
    import asyncio
    from dotenv import load_dotenv

    load_dotenv()

    async def main():
        agent = GraphGeneratorAgent()

        test_query = "Compare clicks vs cost for my top 5 campaigns"
        test_data = [
            { "campaignName": "Summer Sale", "totalClicks": 1500, "totalCost": 750 },
            { "campaignName": "New Launch", "totalClicks": 2100, "totalCost": 1500 },
            { "campaignName": "Winter Promo", "totalClicks": 800, "totalCost": 950 },
            { "campaignName": "Brand Awareness", "totalClicks": 500, "totalCost": 200 },
            { "campaignName": "Lead Gen Q1", "totalClicks": 1200, "totalCost": 600 },
        ]

        print(f"--- Testing Graph Generator ---")
        print(f"Original Query: {test_query}")
        # print(f"Input Data: {json.dumps(test_data, indent=2)}")

        try:
            # Use ainvoke since the chain ends with a parser
            final_result = await agent.chain.ainvoke({"query": test_query, "data": test_data})
            print("\n--- Final Parsed Output ---")
            print(json.dumps(final_result, indent=2))
        except Exception as e:
            print(f"\n--- Error during invocation ---")
            print(e)
            import traceback
            traceback.print_exc()
            
        # Test empty data
        test_query_empty = "How are my campaigns performing?"
        test_data_empty = []
        print(f"\n--- Testing Graph Generator (Empty Data) ---")
        try:
            final_result_empty = await agent.chain.ainvoke({"query": test_query_empty, "data": test_data_empty})
            print("\n--- Final Parsed Output (Empty Data) ---")
            print(json.dumps(final_result_empty, indent=2))
        except Exception as e:
             print(f"\n--- Error during invocation (Empty Data) ---")
             print(e)

    asyncio.run(main()) 