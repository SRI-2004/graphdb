import asyncio
from typing import Any, Dict, List, Union, AsyncIterator, Optional
import json

from langchain_core.callbacks.base import AsyncCallbackHandler
from langchain_core.outputs import LLMResult
from langchain_core.agents import AgentAction, AgentFinish

class AsyncStreamCallbackHandler(AsyncCallbackHandler):
    """
    Callback handler to capture streamed tokens and put them into an async queue.
    Also handles AgentAction and AgentFinish events for reasoning.
    """
    def __init__(self):
        # Initialize an asyncio Queue to hold the streamed tokens and events
        self.queue = asyncio.Queue()
        # Keep track of the final answer status
        self._final_answer_reached = False

    async def on_llm_new_token(self, token: str, **kwargs: Any) -> None:
        """Handle new LLM token stream. Put token in the queue."""
        if token: # Ensure token is not empty
            await self.queue.put({"type": "token", "content": token})

    async def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        """Signal the end of the LLM stream for a particular step."""
        # You might add logic here if needed, e.g., putting a specific marker
        # await self.queue.put({"type": "llm_end"})
        pass # Usually handled by the main streaming loop finishing

    async def on_tool_start(
        self, serialized: Dict[str, Any], input_str: str, **kwargs: Any
    ) -> None:
        """Handle start of tool execution."""
        await self.queue.put({"type": "tool_start", "tool_name": serialized.get("name"), "input": input_str})

    async def on_tool_end(self, output: str, **kwargs: Any) -> None:
        """Handle end of tool execution."""
        await self.queue.put({"type": "tool_end", "output": output})

    async def on_agent_action(self, action: AgentAction, **kwargs: Any) -> Any:
        """Handle agent action."""
        # Agent actions often contain thoughts/reasoning before tool use
        log_entry = action.log.strip()
        if log_entry:
            await self.queue.put({"type": "agent_action", "action": action.tool, "input": action.tool_input, "log": log_entry})

    async def on_agent_finish(self, finish: AgentFinish, **kwargs: Any) -> Any:
        """Handle agent finish."""
        # Agent finish contains the final output
        log_entry = finish.log.strip()
        # Don't put the final answer directly; let the chain return it.
        # Only put the final thought/log if it exists.
        if log_entry:
             await self.queue.put({"type": "agent_finish_log", "log": log_entry})
        # Mark that the final answer has been reached
        self._final_answer_reached = True
        # Signal the end of the stream after processing finish event
        # await self.queue.put(None)

    async def on_chain_end(self, outputs: Dict[str, Any], **kwargs: Any) -> None:
        """Ensure the queue is closed when the chain ends, especially if no agent finish event occurs."""
        if not self._final_answer_reached:
             # If the chain ends without a proper agent finish (e.g., simple chains),
             # ensure the stream queue knows it's done.
             # However, avoid closing it prematurely if an agent finish is expected.
             # The `generate_stream` function will handle the `None` signal from the consumer task.
             pass

async def generate_stream(
    chain_coroutine: asyncio.Task,
    queue: asyncio.Queue,
) -> AsyncIterator[Dict[str, Any]]:
    """
    Consumes the chain's execution task and the callback queue simultaneously.
    Yields structured chunks for tokens, reasoning steps, and the final output.
    """
    while True:
        # Wait for either the chain to finish or a new item in the queue
        done, pending = await asyncio.wait(
            [chain_coroutine, queue.get()],
            return_when=asyncio.FIRST_COMPLETED
        )

        # Process items from the queue first
        for task in done:
            if task == queue.get(): # Check if the completed task was queue.get()
                item = task.result() # Get the item from the queue
                if item is None: # End of stream signal
                    # If queue signals None, wait for chain_coroutine to finish
                    if not chain_coroutine.done():
                        await chain_coroutine # Ensure chain coroutine has finished
                    if chain_coroutine.exception():
                        yield {"type": "error", "content": str(chain_coroutine.exception())}
                        return # Stop iteration on error
                    # Retrieve and yield the final result if available from the coroutine
                    final_result_task = next((t for t in pending if t == chain_coroutine), None)
                    if final_result_task and not final_result_task.done():
                         await final_result_task # Wait if not done
                    if chain_coroutine.done() and not chain_coroutine.exception():
                         result = chain_coroutine.result()
                         # Yield final result if it wasn't streamed via callbacks (e.g., simple chains)
                         # Check if result is already yielded in `consume_stream` as `final_output`
                         # In the current ClassifierAgent implementation, final_output is yielded separately.
                         # This part might be useful for other chain types.
                         pass # Final output handled by the consumer task
                    return # End iteration
                else:
                    # Yield the structured chunk (token, action, etc.)
                    yield item
                # Ensure the queue task is removed from pending if it was completed
                if queue.get() in pending:
                     pending.remove(queue.get())
            elif task == chain_coroutine: # Check if the completed task was the chain coroutine
                # Chain finished. If queue is empty and signaled done, we are finished.
                if queue.empty():
                    if chain_coroutine.exception():
                         yield {"type": "error", "content": str(chain_coroutine.exception())}
                    # Final result yielding is handled when queue returns None
                    return # Exit loop
                # else: continue processing queue items

        # Cancel the queue.get() task if chain_coroutine completed to avoid waiting indefinitely
        if chain_coroutine.done():
            queue_get_task = next((t for t in pending if t == queue.get()), None)
            if queue_get_task:
                queue_get_task.cancel()
