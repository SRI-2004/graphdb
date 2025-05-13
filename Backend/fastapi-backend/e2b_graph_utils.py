import os
import base64
import json
from io import BytesIO
from typing import List, Dict, Optional, Any
from e2b import AsyncSandbox as Sandbox # type: ignore
from dotenv import load_dotenv

# Try to load .env from current directory and parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # Backend/ directory

# First check current directory
if os.path.exists(os.path.join(current_dir, '.env')):
    load_dotenv(os.path.join(current_dir, '.env'))
    print(f"Loaded .env from {current_dir}")
# Then check parent directory
elif os.path.exists(os.path.join(parent_dir, '.env')):
    load_dotenv(os.path.join(parent_dir, '.env'))
    print(f"Loaded .env from {parent_dir}")
else:
    load_dotenv()  # Try default search paths
    print("No .env file found in current or parent directory, trying default locations")

# Load E2B API Key
E2B_API_KEY = os.getenv("E2B_API_KEY")
if not E2B_API_KEY:
    print("WARNING: E2B_API_KEY not found in environment variables. Make sure it's set in your .env file or system environment.")
    print(f"Checked in: {current_dir} and {parent_dir}")
else:
    print(f"Found E2B_API_KEY: {E2B_API_KEY[:5]}...{E2B_API_KEY[-5:]}")  # Print first and last 5 chars for verification

# Python script template to be executed in the E2B Sandbox
# This script will use matplotlib to generate a plot.
# It expects data as a JSON string and plot_parameters as a JSON string.
PYTHON_GRAPHING_SCRIPT = """
import matplotlib
matplotlib.use('Agg') # Use non-interactive backend
import matplotlib.pyplot as plt
import pandas as pd
import json
import io
import base64

def generate_plot(json_data_str, json_plot_params_str, json_theme_params_str=None):
    try:
        data = json.loads(json_data_str)
        plot_params = json.loads(json_plot_params_str)
        theme_params = json.loads(json_theme_params_str) if json_theme_params_str else {}

        if not data:
            print("Error: No data provided.")
            return None

        df = pd.DataFrame(data)

        if df.empty:
            print("Error: DataFrame is empty after loading data.")
            return None

        graph_type = plot_params.get("type", "bar")
        x_col = plot_params.get("x")
        y_col = plot_params.get("y")
        title = plot_params.get("title", "Generated Graph")
        
        xlabel = plot_params.get("xlabel", x_col)
        ylabel = plot_params.get("ylabel", y_col)

        if not x_col or not y_col:
            print(f"Error: X-column ('{x_col}') or Y-column ('{y_col}') not specified or found.")
            return None
        
        if x_col not in df.columns:
            print(f"Error: X-column '{x_col}' not found in data. Available columns: {list(df.columns)}")
            return None
        if y_col not in df.columns:
            print(f"Error: Y-column '{y_col}' not found in data. Available columns: {list(df.columns)}")
            return None

        # Apply basic theming
        fig, ax = plt.subplots(figsize=plot_params.get("figsize", (10, 6)))
        
        bg_color = theme_params.get("backgroundColor", "white")
        fig.patch.set_facecolor(bg_color)
        ax.set_facecolor(bg_color)

        text_color = theme_params.get("textColor", "black")
        plt.rcParams['text.color'] = text_color
        plt.rcParams['axes.labelcolor'] = text_color
        plt.rcParams['xtick.color'] = text_color
        plt.rcParams['ytick.color'] = text_color
        ax.spines['bottom'].set_color(text_color)
        ax.spines['top'].set_color(text_color) 
        ax.spines['right'].set_color(text_color)
        ax.spines['left'].set_color(text_color)

        grid_color = theme_params.get("gridColor", "lightgrey")
        ax.grid(color=grid_color, linestyle='--', linewidth=0.5, alpha=0.7)


        if graph_type == "bar":
            df.plot(kind='bar', x=x_col, y=y_col, ax=ax, legend=plot_params.get("legend", False), color=theme_params.get("barColor", "skyblue"))
        elif graph_type == "line":
            df.plot(kind='line', x=x_col, y=y_col, ax=ax, legend=plot_params.get("legend", False), marker=plot_params.get("marker", "o"), color=theme_params.get("lineColor", "steelblue"))
        elif graph_type == "scatter":
            df.plot(kind='scatter', x=x_col, y=y_col, ax=ax, color=theme_params.get("scatterMarkerColor", "coral"), s=plot_params.get("markerSize", 50))
        # Add more types (e.g., pie, histogram) as needed
        else:
            print(f"Error: Unsupported graph type '{graph_type}'. Supported types: bar, line, scatter.")
            return None
        
        ax.set_title(title, fontsize=plot_params.get("titleFontSize", 16), color=text_color)
        ax.set_xlabel(xlabel, fontsize=plot_params.get("labelFontSize", 12), color=text_color)
        ax.set_ylabel(ylabel, fontsize=plot_params.get("labelFontSize", 12), color=text_color)
        
        plt.xticks(rotation=plot_params.get("xtick_rotation", 45), ha="right")
        plt.tight_layout()

        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', facecolor=fig.get_facecolor())
        img_buffer.seek(0)
        img_base64 = base64.b64encode(img_buffer.read()).decode('utf-8')
        
        plt.close(fig) # Close the figure to free memory
        return img_base64

    except Exception as e:
        print(f"Error during plot generation: {e}")
        import traceback
        print(traceback.format_exc())
        return None

# The E2B sandbox will call this function
# result = generate_plot(json_data_str=input_data_json, json_plot_params_str=input_params_json, json_theme_params_str=input_theme_json)
# print(result if result else "None") # Ensure something is printed for E2B to capture

"""

async def generate_e2b_graph_image(
    data: List[Dict[str, Any]],
    plot_parameters: Dict[str, Any],
    theme_parameters: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """
    Generates a graph image using E2B sandbox.

    Args:
        data: The data to plot, as a list of dictionaries.
        plot_parameters: Dictionary containing plot type, x/y columns, title, etc.
                         Example: {"type": "bar", "x": "category", "y": "value", "title": "Sales Data"}
        theme_parameters: Optional dictionary for theming (e.g., colors).
                          Example: {"backgroundColor": "#f0f0f0", "textColor": "#333333", "barColor": "blue"}

    Returns:
        Base64 encoded PNG image string, or None if generation fails.
    """
    if not E2B_API_KEY:
        print("Error: E2B_API_KEY not configured.")
        return None
    if not data:
        print("Error: No data provided for graph generation.")
        return None
    if not plot_parameters or not plot_parameters.get("x") or not plot_parameters.get("y"):
        print("Error: Insufficient plot parameters (missing x or y column).")
        return None

    sandbox = None
    try:
        print(f"Initializing E2B sandbox for graph generation...")
        sandbox = await Sandbox.create(api_key=E2B_API_KEY)
        
        timeout_duration = 120  # seconds for command execution

        # Ensure libraries are installed
        libs_to_install = "pandas matplotlib"
        print(f"Attempting to install {libs_to_install} in sandbox...")
        install_proc = await sandbox.commands.run(
            cmd=f"pip install --disable-pip-version-check --no-cache-dir {libs_to_install}", 
            timeout=timeout_duration
        )
        
        if install_proc.exit_code != 0:
            print(f"Error installing Python libraries in E2B sandbox. Exit Code: {install_proc.exit_code}")
            print(f"Install Stdout: {install_proc.stdout}")
            print(f"Install Stderr: {install_proc.stderr}")
            return None 
        print(f"{libs_to_install} installed successfully in sandbox (or already present).")

        # Proceed with graph generation script execution
        json_data_str = json.dumps(data)
        json_plot_params_str = json.dumps(plot_parameters)
        json_theme_params_str = json.dumps(theme_parameters if theme_parameters else {})

        main_script_path = "/tmp/graph_generator_script.py"
        await sandbox.files.write(main_script_path, PYTHON_GRAPHING_SCRIPT)
        
        input_data = json_data_str
        input_params = json_plot_params_str
        input_theme = json_theme_params_str

        bootstrap_script = f"""
import sys
sys.path.append('/tmp') # Ensure graph_generator_script can be imported
import graph_generator_script

input_data = r'''{input_data}'''
input_params = r'''{input_params}'''
input_theme = r'''{input_theme}'''

result = graph_generator_script.generate_plot(
    json_data_str=input_data,
    json_plot_params_str=input_params,
    json_theme_params_str=input_theme
)
print(result if result else "GENERATION_FAILED")
"""
        
        bootstrap_script_path = "/tmp/bootstrap_executor.py"
        await sandbox.files.write(bootstrap_script_path, bootstrap_script)
        
        proc = await sandbox.commands.run(
            cmd=f"python {bootstrap_script_path}", 
            timeout=timeout_duration
        )
        
        output_stdout = proc.stdout.strip()
        output_stderr = proc.stderr.strip()

        if proc.exit_code != 0:
            print(f"E2B script execution failed. Exit code: {proc.exit_code}")
            print(f"Stdout: {output_stdout}")
            print(f"Stderr: {output_stderr}")
            return None

        if output_stdout == "GENERATION_FAILED" or not output_stdout or output_stdout == "None":
            print(f"E2B script indicated generation failure or no output.")
            print(f"Stdout: {output_stdout}")
            if output_stderr: print(f"Stderr: {output_stderr}")
            return None
            
        return output_stdout

    except Exception as e:
        print(f"An error occurred during E2B graph generation: {e}")
        import traceback
        print(traceback.format_exc())
        return None
    finally:
        if sandbox:
            print("Attempting to kill sandbox...")
            await sandbox.kill()
            print("E2B sandbox kill signal sent.")


# async def main_test():
#     sample_data = [
#         {'category': 'A', 'value': 10, 'other': 5},
#         {'category': 'B', 'value': 20, 'other': 7},
#         {'category': 'C', 'value': 15, 'other': 12}
#     ]
#     sample_plot_params = {'type': 'bar', 'x': 'category', 'y': 'value', 'title': 'Sample Bar Chart'}
#     sample_theme_params = {"backgroundColor": "#DDDDDD", "textColor": "darkblue", "barColor": "coral"}
    
#     if not E2B_API_KEY:
#         print("Please set the E2B_API_KEY environment variable to test.")
#         return

#     base64_image = await generate_e2b_graph_image(sample_data, sample_plot_params, sample_theme_params)
#     if base64_image:
#         print(f"Generated image (first 100 chars): {base64_image[:100]}...")
#     else:
#         print("Failed to generate image.")

# if __name__ == '__main__':
#     import asyncio
#     if os.name == 'nt':
#          asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
#     try:
#         asyncio.run(main_test())
#     except RuntimeError as e:
#         if "Event loop is closed" in str(e):
#             pass 
#         else:
#             raise 