"""
Tool for triggering Facebook Ads Insight workflows via the FastAPI backend.
"""

import os
import json
import httpx
from typing import Optional, Dict, Any
from langchain.tools import Tool

# Define the function with a simple string input
async def facebook_insight_trigger(tool_input: str) -> str:
    """
    Triggers a Facebook Ads Insight workflow in the backend.
    The input must be a JSON string with 'query' and 'session_id' fields.
    
    Args:
        tool_input: Raw string input from the agent
    
    Returns:
        A message confirming the workflow has been started
    """
    print(f"[Tool Debug] Received raw input: {tool_input}")
    
    # Parse the input as JSON
    try:
        # Try parsing as JSON first
        try:
            input_data = json.loads(tool_input)
            query = input_data.get("query")
            session_id = input_data.get("session_id")
        except json.JSONDecodeError:
            # If JSON parsing fails, try to extract values from a string format
            # This is a fallback in case the input isn't valid JSON
            import re
            query_match = re.search(r'"query"\s*:\s*"([^"]+)"', tool_input)
            session_match = re.search(r'"session_id"\s*:\s*"([^"]+)"', tool_input)
            
            query = query_match.group(1) if query_match else None
            session_id = session_match.group(1) if session_match else None
        
        # Validate required fields
        if not query:
            return "Error: Missing 'query' field in the input"
        if not session_id:
            return "Error: Missing 'session_id' field in the input"
            
        print(f"[Tool Debug] Extracted input: query={query}, session_id={session_id}")
        
        # Make the backend request
        backend_url = os.getenv("BACKEND_API_URL", "http://localhost:8050")
        endpoint = f"{backend_url}/api/v1/workflows/trigger/facebook_insight"
        
        # Backend expects user_id
        payload = {
            "query": query,
            "user_id": session_id,  # Map session_id to user_id for backend
            "workflow_type": "insight"
        }
        print(f"[Tool Debug] Sending Payload: {payload}")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(endpoint, json=payload)
            if response.status_code == 200:
                response_data = response.json()
                workflow_id = response_data.get("workflow_id", "unknown")
                return f"Facebook Ads insight analysis started (workflow ID: {workflow_id}). Results will appear in the dashboard shortly."
            else:
                error_detail = response.text
                return f"Error triggering insight analysis: HTTP {response.status_code}: {error_detail}"
    
    except Exception as e:
        import traceback
        print(f"[Tool Error] {traceback.format_exc()}")
        return f"Failed to process the tool input or connect to the backend service: {str(e)}"

# Create the tool with minimal overhead
facebook_insight_trigger_tool = Tool(
    name="facebook_insight_trigger",
    description="""
    Useful for answering questions about Facebook Ads performance metrics and insights.
    Use this tool when the user asks about Facebook ad performance, CTR, CPC, CPM, reach, frequency,
    campaign or ad set performance, conversion rates, or other Facebook advertising metrics.
    The analysis will be displayed in the dashboard.
    The input must be a JSON string with "query" and "session_id" fields.
    Example input: {"query": "how is my Facebook campaign performing", "session_id": "user-123456"}
    """,
    func=lambda x: None,  # Placeholder
    coroutine=facebook_insight_trigger,
    args_schema=None  # Skip schema validation completely
) 