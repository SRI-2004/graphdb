from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate

SYSTEM_MESSAGE = """
You are an expert data visualization assistant tasked with suggesting the best way(s) to visualize query results.
Your goal is to analyze the provided data, which may come from one or more queries addressing different objectives related to an original user query.
For each distinct objective/query result set in the input data, recommend the most appropriate chart type and the data columns to use.

**Input:**
- Original User Query: Provides context on the user's overall goal.
- Query Results Data: A list of result objects, where each object typically contains an 'objective', 'query', 'data' (list of records), and potentially an 'error'.

**Task:**
- Iterate through the input data list. For each result object that contains successful data (i.e., no 'error' key or error is null/empty, and 'data' is a non-empty list):
    - Analyze the records in the 'data' list.
    - Determine the best chart type to visualize these specific records.
    - Identify the relevant column names from the records for the chart's properties (x, y, color, names, values).
    - Create a graph suggestion object for this specific objective/result.

**Constraints:**
- Choose the chart type from this list: 'bar', 'line', 'scatter', 'pie', 'table', 'none'.
- If a specific result's data is unsuitable for charting (e.g., single value, unstructured text), suggest 'table' or 'none' for that specific result.
- Base column name suggestions STRICTLY on the keys present in the data records for that specific result.
- Prioritize clarity. A simple 'table' is often better than a confusing chart.

**Output Format:**
# --- Temporarily Simplified Output Format Description ---
# You MUST respond ONLY with a single JSON object containing the key `graph_suggestions`.
# The value MUST be a JSON list `[]` containing one or more graph suggestion objects.
# Each object should contain keys like 'objective', 'type', 'columns', and 'title'.
# Return an empty list `[]` if no graphs can be suggested.
# --- Restore Original Detailed Description & Escape JSON Braces ---
You MUST respond ONLY with a single JSON object containing the key `graph_suggestions`. 
The value MUST be a JSON list `[]` containing one or more graph suggestion objects.
Each suggestion object in the list should have the following structure:

```json
{{{{
  "objective": "<The objective string from the corresponding input result object>",
  "type": "<chart_type>",
  "columns": {{{{
    "x": "<column_name_for_x_axis_or_null>",
    "y": "<column_name_for_y_axis_or_null>",
    "names": "<column_name_for_pie_names_or_null>",
    "values": "<column_name_for_pie_values_or_null>",
    "color": "<optional_column_name_for_color_or_null>"
  }}}},
  "title": "<A concise, descriptive title for this specific chart>"
}}}}
```

- Include one suggestion object in the list for each input result that has plottable data.
- If an input result has an error or no data, do NOT include a suggestion object for it in the list.
- If *no* input results have plottable data, return an empty list: `{{"graph_suggestions": []}}`.
- Ensure the entire output is a single, valid JSON object with the `graph_suggestions` key holding a list.
"""

HUMAN_MESSAGE = """
Original User Query: {query}

Query Results Data (JSON):
```json
{data}
```

Based on the original query context and the actual data retrieved for each objective, provide the JSON output containing the list of graph suggestions only.
"""

def create_graph_generator_prompt() -> ChatPromptTemplate:
    """Creates the ChatPromptTemplate for the GraphGeneratorAgent."""
    # Use tuple format directly instead of from_template constructors
    return ChatPromptTemplate.from_messages([
        ("system", SYSTEM_MESSAGE),
        ("human", HUMAN_MESSAGE)
    ])

# Example usage (for testing)
if __name__ == '__main__':
    import json
    prompt = create_graph_generator_prompt()
    test_query = "Compare clicks vs cost for my top campaigns, and also show their status."
    # Simulating combined results from multiple queries/objectives
    test_data_combined = [
      {
        "objective": "Fetch clicks vs cost",
        "query": "MATCH (c:Campaign) RETURN c.name as Name, c.metrics.clicks as Clicks, c.metrics.cost as Cost LIMIT 5",
        "data": [
            { "Name": "Summer Sale", "Clicks": 1500, "Cost": 750 },
            { "Name": "New Launch", "Clicks": 2100, "Cost": 1500 },
        ],
        "error": None
      },
      {
        "objective": "Fetch status",
        "query": "MATCH (c:Campaign) RETURN c.name as Name, c.status as Status LIMIT 5",
        "data": [
            { "Name": "Summer Sale", "Status": "ENABLED" },
            { "Name": "New Launch", "Status": "ENABLED" },
        ],
        "error": None
      },
      {
          "objective": "Fetch budgets",
          "query": "MATCH (c:Campaign) RETURN c.budget as Budget",
          "data": [], # Example of no data returned
          "error": None
      },
       {
          "objective": "Fetch errors",
          "query": "MATCH (c:Campaign) RETURN c.name",
          "data": None,
          "error": "Query execution failed"
      }
    ]
    formatted_prompt = prompt.format_prompt(
        query=test_query,
        data=json.dumps(test_data_combined, indent=2) # Pass combined results list
    )
    print(formatted_prompt.to_string()) 