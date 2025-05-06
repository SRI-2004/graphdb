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
LLM_MODEL_NAME = "gpt-4o-mini"

class OptimizationQueryGeneratorAgent:
    """
    Agent that decomposes optimization request and generates multiple Cypher queries.
    Uses LangChain's built-in streaming (.astream_log).
    """
    def __init__(self):
        self.prompt: ChatPromptTemplate = create_optimization_query_generator_prompt()
        self.llm = ChatOpenAI(
            model=LLM_MODEL_NAME,
            temperature=0.1,
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

