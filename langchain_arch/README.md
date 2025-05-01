# LangChain Agentic Architecture for Neo4j Insights and Optimizations

This project implements a LangChain-based agentic architecture to interact with a Neo4j graph database.
Users can provide natural language queries, which are classified into either insight generation or optimization recommendation workflows.

## Architecture Overview

- **Classifier Agent**: Classifies user queries.
- **Insight Workflow**: Generates Cypher queries and synthesizes insights from Neo4j data.
- **Optimization Workflow**: Generates Cypher queries for feature extraction and produces actionable optimization recommendations.
- **Neo4j Utilities**: Handles connection and querying of the Neo4j database.
- **Streaming Support**: Streams reasoning steps from each agent.

## Setup

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  Set up environment variables (e.g., in a `.env` file):
    ```
    OPENAI_API_KEY="your_openai_api_key"
    NEO4J_URI="your_neo4j_aura_uri"
    NEO4J_USERNAME="your_neo4j_username"
    NEO4J_PASSWORD="your_neo4j_password"
    NEO4J_DATABASE="neo4j" # Or your specific database name
    ```
3.  Run the main application:
    ```bash
    python main.py "Your natural language query here"
    ```

## Components

- `agents/`: Contains the implementations for each agent (Classifier, InsightQueryGenerator, etc.).
- `chains/`: Defines the LangChain workflows (Insight, Optimization, Router).
- `utils/`: Includes helper functions (Neo4j connection, streaming callbacks).
- `prompts/`: Stores the prompt templates for each agent.
- `main.py`: The main entry point for running the system.
