"""
Tools for interacting with various advertising data workflows.
"""

# Import pre-instantiated tools from their respective files
from .google_insight_trigger import google_insight_trigger_tool
from .google_optimization_trigger import google_optimization_trigger_tool
from .facebook_insight_trigger import facebook_insight_trigger_tool
from .facebook_optimization_trigger import facebook_optimization_trigger_tool

# Import the class for general_insight_trigger and instantiate it
from .general_insight_trigger import GeneralInsightTriggerTool
general_insight_trigger_tool = GeneralInsightTriggerTool()

# Import the class for general_optimization_trigger and instantiate it
from .general_optimization_trigger import GeneralOptimizationTriggerTool
general_optimization_trigger_tool = GeneralOptimizationTriggerTool()

# Export the tool instances for easier importing by main_chatbot.py
__all__ = [
    "google_insight_trigger_tool",
    "google_optimization_trigger_tool",
    "facebook_insight_trigger_tool",
    "facebook_optimization_trigger_tool",
    "general_insight_trigger_tool",
    "general_optimization_trigger_tool",
]

# Optional: A helper function to get all tool instances
# This function would need to be updated if we want to use it elsewhere,
# as it currently tries to instantiate classes that may not exist for all tools.
# For now, main_chatbot.py imports directly from __all__.
# def get_all_tools():
#     return [
#         google_insight_trigger_tool, # Already an instance
#         google_optimization_trigger_tool, # Already an instance
#         facebook_insight_trigger_tool, # Already an instance
#         facebook_optimization_trigger_tool, # Already an instance
#         GeneralInsightTriggerTool(), # This one is a class, so instantiate
#         GeneralOptimizationTriggerTool(), # This one is a class, so instantiate
#     ] 