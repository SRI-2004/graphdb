"""
Facebook Architecture Package

This package provides the components for loading, analyzing, 
and generating insights from Facebook Ads data using LangChain.
"""

__version__ = "0.1.0"

# Make components accessible from the top level package
from .chains.router import Router
from .utils.neo4j_utils import Neo4jDatabase

__all__ = ["Router", "Neo4jDatabase"]
