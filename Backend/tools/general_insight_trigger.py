import httpx
import os
import json
from langchain.tools import BaseTool
from typing import Dict, Any

# Configuration for the backend API
BACKEND_API_URL = os.getenv("BACKEND_API_URL", "http://localhost:8050")

class GeneralInsightTriggerTool(BaseTool):
    name: str = "GeneralInsightTrigger"
    description: str = (
        "Triggers a general insight workflow that combines information from Google and Facebook. "
        "Use this for broad questions about overall campaign performance, or when the user asks for a summary across all platforms. "
        "The input must be a JSON string with 'query' and 'session_id' fields. "
        "Example input: {\"query\": \"how are my campaigns performing\", \"session_id\": \"user-123\"}. "
        "The 'session_id' is used as the user_id for backend communication."
    )

    async def _arun(self, tool_input: str, **kwargs: Any) -> Dict[str, Any]:
        """Use the tool asynchronously. tool_input is expected to be a JSON string."""
        print(f"[GeneralInsightTriggerTool Debug] Received raw tool_input: {tool_input}")
        
        if not tool_input:
            return {"status": "error", "message": "Error: Empty tool input."}

        # Clean up the input - remove any trailing backticks or newlines
        tool_input = tool_input.strip('`').strip()
        
        # Handle double-quoted JSON strings (e.g. "{"key": "value"}")
        if tool_input.startswith('"') and tool_input.endswith('"'):
            # Remove outer quotes and handle escaped quotes within
            try:
                # This converts the string: "{\"key\": \"value\"}" -> {"key": "value"} 
                tool_input = json.loads(tool_input)
            except json.JSONDecodeError as e:
                print(f"ERROR: Could not parse outer quoted string: {e}")
        
        try:
            # Now try to parse the actual JSON content
            input_data = json.loads(tool_input)
            query = input_data.get("query")
            session_id = input_data.get("session_id")

            if not query:
                return {"status": "error", "message": "Error: Missing 'query' field in the JSON input string."}
            if not session_id:
                return {"status": "error", "message": "Error: Missing 'session_id' field in the JSON input string."}

        except json.JSONDecodeError as e:
            error_message = f"Error: Invalid JSON input string. {e}"
            print(f"ERROR in GeneralInsightTriggerTool: {error_message}")
            # Try to salvage the situation by doing a regex extraction as a fallback
            import re
            query_match = re.search(r'"query"[:\s]+"([^"]+)"', tool_input)
            session_match = re.search(r'"session_id"[:\s]+"([^"]+)"', tool_input)
            
            query = query_match.group(1) if query_match else None
            session_id = session_match.group(1) if session_match else None
            
            if not query or not session_id:
                return {"status": "error", "message": error_message, "details": tool_input}
            
            print(f"[GeneralInsightTriggerTool Recovery] Extracted via regex: query='{query}', session_id='{session_id}'")
            
        except Exception as e: # Catch other potential errors during parsing
            error_message = f"Error parsing tool input: {e}"
            print(f"ERROR in GeneralInsightTriggerTool: {error_message}")
            return {"status": "error", "message": error_message, "details": tool_input}

        print(f"[GeneralInsightTriggerTool Debug] Parsed query: {query}, session_id: {session_id}")

        endpoint_url = f"{BACKEND_API_URL}/api/v1/workflows/trigger/general_insight"
        payload = {"query": query, "user_id": session_id}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(endpoint_url, json=payload)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                error_message = f"HTTP error occurred: {e.response.status_code} - {e.response.text}"
                print(f"ERROR in GeneralInsightTriggerTool: {error_message}")
                return {"status": "error", "message": error_message, "details": str(e)}
            except httpx.RequestError as e:
                error_message = f"Request error occurred: {e}"
                print(f"ERROR in GeneralInsightTriggerTool: {error_message}")
                return {"status": "error", "message": error_message, "details": str(e)}
            except Exception as e:
                error_message = f"An unexpected error occurred during API call: {e}"
                print(f"ERROR in GeneralInsightTriggerTool: {error_message}")
                return {"status": "error", "message": error_message, "details": str(e)}

    def _run(self, tool_input: str, **kwargs: Any) -> Dict[str, Any]:
        """Use the tool synchronously (not recommended for FastAPI background tasks)."""
        raise NotImplementedError("Synchronous execution is not implemented for GeneralInsightTriggerTool. Use arun.")

# Example usage (for testing purposes)
if __name__ == '__main__':
    import asyncio

    async def main():
        test_session_id = "test-user-123"
        test_query = "How are my campaigns performing overall?"
        
        tool = GeneralInsightTriggerTool()
        
        json_input_str = json.dumps({"query": test_query, "session_id": test_session_id})
        print(f"Testing tool with JSON string input: {json_input_str}")
        result = await tool.arun(tool_input=json_input_str) # Pass the JSON string directly
        print(f"Tool Result from JSON string: {result}")

    print("To test this tool, ensure the FastAPI backend is running and uncomment the asyncio.run(main()) line.")
    print("Also, ensure BACKEND_API_URL environment variable is set or defaults correctly.")

