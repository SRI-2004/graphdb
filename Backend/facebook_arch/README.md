# Facebook LangChain Architecture

This architecture provides the components for loading, analyzing, and generating insights and optimizations from Facebook Ads data using LangChain.

## Overview

The Facebook architecture is structured as follows:

- `agents/` - LangChain agents with specific purposes
  - `classifier.py` - Determines whether query is best handled as insight or optimization
  - `insight_generator.py` - Generates insights from Facebook Ads data
  - `insight_query_generator.py` - Generates Cypher queries for insights from Facebook Ads
  - `optimization_generator.py` - Generates optimization recommendations for Facebook Ads
  - `optimization_query_generator.py` - Generates Cypher queries for optimizations from Facebook Ads
  - `graph_generator.py` - Generates graph visualization suggestions

- `chains/` - Orchestration of workflows
  - `router.py` - Main router that classifies and directs to appropriate workflow
  - `insight_workflow.py` - Workflow for generating insights from Facebook Ads data
  - `optimization_workflow.py` - Workflow for generating optimizations for Facebook Ads

- `prompts/` - LLM prompts for agents
  - Various prompt templates for different agents and purposes

- `utils/` - Utility functions and classes

## Usage

This architecture is used by the main chatbot through the Facebook tools to generate insights and optimizations from Facebook Ads data.

## Development

This is adapted from the Google Ads architecture but specialized for Facebook's specific metrics, terminology, and optimization approaches.
