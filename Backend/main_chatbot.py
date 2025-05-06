import asyncio
import os
import sys
from dotenv import load_dotenv
import httpx  # For making API calls in tools

from langchain_openai import ChatOpenAI
# Import ReAct agent components
from langchain.agents import AgentExecutor, create_react_agent 
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import HumanMessage, AIMessage
from langchain.memory import ConversationBufferMemory
# Import prompt tools for ReAct
from langchain.tools.render import render_text_description
from langchain_core.prompts import PromptTemplate

# --- Setup: Paths and Environment ---

# Define project root (one level up from this file)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))
# Add project root to sys.path to allow importing tools etc. if needed structure changes
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load environment variables from .env file at the project root
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

# Check for necessary environment variables
required_env_vars = ["OPENAI_API_KEY"] # Add more as needed, e.g., BACKEND_API_URL
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}. Ensure they are set.")
    sys.exit(1)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# Define the base URL for the FastAPI backend
# TODO: Make this configurable via .env or settings
BACKEND_API_URL = "http://localhost:8050" # Assuming default FastAPI port

# --- Tool Definitions ---

from tools import (
    google_insight_trigger_tool,
    google_optimization_trigger_tool,
    facebook_insight_trigger_tool,
    facebook_optimization_trigger_tool,
    general_insight_trigger_tool,
    general_optimization_trigger_tool
)

tools = [
    google_insight_trigger_tool,
    google_optimization_trigger_tool,
    facebook_insight_trigger_tool,
    facebook_optimization_trigger_tool,
    general_insight_trigger_tool,
    general_optimization_trigger_tool,
]

if not tools:
    print("Warning: No tools are defined or loaded. The agent will only use its base knowledge.")


# --- Agent Setup (ReAct) ---

# Define the ReAct prompt template
# Based on hub prompt: hwchase17/react-chat
prompt_template = """Assistant is a large language model trained by OpenAI.

Assistant is designed to be able to assist with a wide range of tasks, from answering simple questions to providing in-depth explanations and discussions on a wide range of topics. As a language model, Assistant is able to generate human-like text based on the input it receives, allowing it to engage in natural-sounding conversations and provide responses that are coherent and relevant to the topic at hand.

Assistant is specifically configured to help with Google Ads and Facebook Ads analysis and optimization. It has access to a set of tools to trigger backend workflows for these tasks.

TOOLS:
------
Assistant has access to the following tools:

{tools}

To use a tool, please use the following format:

```
Thought: Do I need to use a tool? Yes
Action: The action to take. Should be one of [{tool_names}]
Action Input: A JSON string representing a dictionary with keys for *all* required arguments for the chosen Action, as specified by the tool description. 
**IMPORTANT**: The required arguments are typically "query" and "session_id". 
 - For the "query" argument, use the user's specific question or request.
 - For the "session_id" argument, you MUST get the value from the 'session_id' key in the input dictionary provided to you.
Example Action Input: {{"query": "user's actual question here", "session_id": "the_session_id_value_from_input"}}
Observation: The result of the action.
```

When you have a response to say to the Human, or if you do not need to use a tool, you MUST use the format:

```
Thought: Do I need to use a tool? No
Final Answer: [your response here]
```

Begin!

Previous conversation history:
{chat_history}

New input:
{input}

Session ID provided by user (use this value for the tool's 'session_id' argument):
{session_id}

{agent_scratchpad}
"""

# Create the prompt
agent_prompt = PromptTemplate.from_template(prompt_template)

# Render tool descriptions for the prompt
tool_description = render_text_description(tools)
tool_names = ", ".join([t.name for t in tools])

# Partial the prompt with tool info
agent_prompt = agent_prompt.partial(
    tools=tool_description,
    tool_names=tool_names,
)

# Initialize LLM
# TODO: Consider making model name configurable
llm = ChatOpenAI(model="gpt-4o", temperature=0, api_key=OPENAI_API_KEY)

# Initialize Memory
# TODO: Consider user-specific memory or persistence if needed
memory = ConversationBufferMemory(
    memory_key="chat_history", 
    return_messages=True,
    input_key="input" # Explicitly tell memory which key is the user input
)

# Create the ReAct Agent
agent = create_react_agent(llm, tools, agent_prompt)

# Create the Agent Executor
agent_executor = AgentExecutor(
    agent=agent, 
    tools=tools, 
    memory=memory, 
    verbose=True, 
    return_intermediate_steps=True,
    handle_parsing_errors=True # Add robust handling for ReAct parsing
)

# --- Main Interaction Loop ---

# async def chat_loop():
#     """
#     Simple command-line interaction loop for testing the agent.
#     In a real application, this would be replaced with an API endpoint.
#     """
#     print("Chatbot Agent Initialized. Type 'exit' to quit.")
#     
#     # In a real application, this user_id would come from the frontend
#     # The frontend would have received it from the WebSocket connection
#     user_id = input("Enter your user ID (as received from WebSocket): ")
#     if not user_id:
#         user_id = "default-user"  # Fallback for testing
#         print(f"Using default user ID: {user_id}")
#     
#     while True:
#         user_input = input("You: ")
#         if user_input.lower() == 'exit':
#             print("Exiting chatbot.")
#             break
#         
#         try:
#             # Pass both the input and user_id to the agent
#             response = await agent_executor.ainvoke({
#                 "input": user_input,
#                 "user_id": user_id
#             })
#             print(f"Agent: {response['output']}")
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             # Optionally add more robust error handling

# --- API Endpoint for Production --- 

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Chatbot Agent API")

class ChatRequest(BaseModel):
    message: str
    user_id: str

class AgentApiResponse(BaseModel):
    response: str
    tool_called: bool


@app.get("/")  # or .head("/")
async def root():
    return {"status": "ok"}

@app.post("/api/chat", response_model=AgentApiResponse)
async def chat(request: ChatRequest):
    try:
        print(f"Received chat request for user_id: {request.user_id}")
        
        # Pass input and session_id to the agent executor
        full_agent_response = await agent_executor.ainvoke({
            "input": request.message,
            "session_id": request.user_id 
        })
        
        tool_called = bool(full_agent_response.get('intermediate_steps'))
        agent_output_text = full_agent_response.get('output', 'Agent did not return an output.')
        
        print(f"Agent raw response for user_id {request.user_id}: {full_agent_response}")
        print(f"Agent output text: {agent_output_text}")
        print(f"Tool called: {tool_called}")
        
        return {"response": agent_output_text, "tool_called": tool_called}
        
    except Exception as e:
        print(f"ERROR processing chat for user_id {request.user_id}: {e}") 
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error processing chat: {str(e)}")

if __name__ == "__main__":
    # Run the FastAPI app instead of the chat loop
    import uvicorn
    print("Starting Chatbot Agent API server (ReAct Agent)...")
    uvicorn.run(app, host="0.0.0.0", port=8060) # Use a different port, e.g., 8060

# Old main block commented out:
# if __name__ == "__main__":
#     try:
#         asyncio.run(chat_loop())
#     except KeyboardInterrupt:
#         print("\nExecution interrupted by user.")
#         sys.exit(0) 