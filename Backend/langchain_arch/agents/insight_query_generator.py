import os
import json
import re
from typing import Dict, Any, AsyncIterator, Union

from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnableConfig, RunnablePassthrough
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tracers.log_stream import LogEntry
from langchain_core.messages import BaseMessage

from ..prompts.insight_query_generator import create_insight_query_generator_prompt

# Configuration
LLM_MODEL_NAME = "gpt-4o"

class InsightQueryGeneratorAgent:
    """
    Agent that generates Cypher queries based on user query and graph schema.
    Uses LangChain's built-in streaming (.astream_log).
    """
    def __init__(self):
        self.prompt: ChatPromptTemplate = create_insight_query_generator_prompt()
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
        Executes the Cypher query generation chain using astream_log.

        Args:
            query: The user's natural language query.
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

