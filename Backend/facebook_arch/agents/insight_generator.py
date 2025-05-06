import os
import json
from typing import Dict, Any, List, Optional
import asyncio

from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnableConfig, RunnablePassthrough
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
# Mem0 Async Import - REMOVED
# from mem0 import AsyncMemoryClient

from ..prompts.insight_generator import create_insight_generator_prompt

# Configuration
LLM_MODEL_NAME = "gpt-4o"

class InsightGeneratorAgent:
    """
    Agent that synthesizes insights from multiple data sources.
    Uses .ainvoke to get the final result directly.
    Memory saving functionality has been removed.
    Returns the final insight dictionary.
    """
    def __init__(self):
        # Initialize LLM
        try:
            # Ensure API keys are set
            openai_api_key = os.getenv("OPENAI_API_KEY")
            if not openai_api_key:
                raise ValueError("OPENAI_API_KEY environment variable not set.")
            # mem0_api_key = os.getenv("MEM0_API_KEY") # Removed Mem0 init
            # if not mem0_api_key:
            #     print("Warning: MEM0_API_KEY environment variable not set. Memory saving will not function.")
            #     self.mem0 = None
            # else:
            #     # Initialize Async Client
            #     self.mem0 = AsyncMemoryClient()
            # self.mem0 = None # Ensure mem0 attribute doesn't exist or is None

            self.llm = ChatOpenAI(
                model=LLM_MODEL_NAME,
                temperature=0.1,
                # Streaming=True is not strictly needed for ainvoke, but doesn't hurt
                streaming=True,
            )
        except Exception as e:
            print(f"CRITICAL: Failed to initialize LLM in InsightGeneratorAgent: {e}") # Updated error
            raise RuntimeError(f"Failed to initialize LLM: {e}") from e

        self.prompt: ChatPromptTemplate = create_insight_generator_prompt()
        self.output_parser = JsonOutputParser()

        # Define the internal chain (original, no memory retrieval)
        # NOTE: Prompt NOW needs query_generation_reasoning
        self._chain = (
            RunnablePassthrough.assign(
                # Pass data as JSON string, handle potential missing data gracefully
                data=lambda x: json.dumps(x.get('data', {}), indent=2)
                # query_generation_reasoning is passed directly in the input dict to ainvoke
            )
            | self.prompt
            | self.llm
            | self.output_parser
        )

    async def run(self, query: str, data: Dict[str, List[Dict[str, Any]]], user_id: str, query_generation_reasoning: str) -> Optional[Dict[str, Any]]:
        """
        Executes the insight generation chain using ainvoke
        and returns the final insight dictionary or None on error. Memory saving removed.

        Args:
            query: The original user's natural language insight request.
            data: A dictionary where keys are objectives (str) and values are
                  lists of dictionaries (results from Neo4j for that objective).
            user_id: A unique identifier for the user (currently unused).
            query_generation_reasoning: The reasoning text generated during query planning.

        Returns:
            A dictionary containing the insights, or None if processing fails.
        """
        # 1. Invoke the chain to get the final result directly
        # Pass query_generation_reasoning in the input dictionary
        input_data = {
            "query": query,
            "data": data,
            "query_generation_reasoning": query_generation_reasoning
        }
        final_output = None
        invoke_exception = None

        try:
            final_output = await self._chain.ainvoke(input_data)
        except Exception as e:
            invoke_exception = e
            print(f"InsightGeneratorAgent: Exception during ainvoke: {e}")

        # 2. Save interaction to memory asynchronously (if Mem0 is available) - REMOVED
        # if self.mem0 is not None:
        #     assistant_content = "Error generating insights."
        #     if invoke_exception:
        #         assistant_content = f"Error during insight generation: {invoke_exception}"
        #     elif final_output and isinstance(final_output, dict):
        #         assistant_content = json.dumps(final_output)
        #     elif final_output:
        #          assistant_content = f"Insight generation produced unexpected output type: {type(final_output)}, Content: {str(final_output)[:200]}"
        #     else: # No output and no exception
        #          assistant_content = "Insight generation produced no output."
        #
        #     try:
        #         interaction = [
        #             {"role": "user", "content": query}, # Still logs original user query
        #             {"role": "assistant", "content": assistant_content}
        #         ]
        #         await self.mem0.add(interaction, user_id=user_id)
        #     except Exception as e:
        #         print(f"InsightGeneratorAgent: Error adding interaction to Mem0: {e}")

        # 3. Return the final output if it's a dictionary, otherwise None
        if isinstance(final_output, dict):
            return final_output
        else:
            if invoke_exception is None and final_output is not None:
                 print(f"InsightGeneratorAgent: chain.ainvoke produced non-dict type: {type(final_output)}")
            return None

# Example usage (add if needed for testing)
# if __name__ == '__main__':
#    import asyncio
#    from dotenv import load_dotenv
#    # ... setup and run agent.run(...)
#    # Remember to provide a user_id

