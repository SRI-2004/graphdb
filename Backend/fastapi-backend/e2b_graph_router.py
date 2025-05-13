from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import json
import base64
from e2b_graph_utils import generate_e2b_graph_image

router = APIRouter(prefix="/api/v1/graphs", tags=["graphs"])

class GraphGenerationRequest(BaseModel):
    data: List[Dict[str, Any]]
    plot_parameters: Dict[str, Any]
    theme_parameters: Optional[Dict[str, str]] = None
    
class GraphResponse(BaseModel):
    image_base64: Optional[str] = None
    error: Optional[str] = None
    
@router.post("/generate", response_model=GraphResponse)
async def generate_graph(request: GraphGenerationRequest):
    """
    Generate a graph using E2B sandbox with matplotlib.
    
    The request should include:
    - data: The dataset as a list of dictionaries
    - plot_parameters: A dictionary with graph configuration (type, x, y, title, etc.)
    - theme_parameters: Optional styling parameters
    
    Returns a base64 encoded PNG image or an error message.
    """
    try:
        base64_image = await generate_e2b_graph_image(
            data=request.data,
            plot_parameters=request.plot_parameters,
            theme_parameters=request.theme_parameters
        )
        
        if not base64_image:
            return GraphResponse(
                error="Failed to generate graph. Check server logs for details."
            )
            
        return GraphResponse(image_base64=base64_image)
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in generate_graph: {e}\n{error_details}")
        return GraphResponse(
            error=f"Error generating graph: {str(e)}"
        ) 