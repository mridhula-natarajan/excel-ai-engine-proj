
import os
import google.generativeai as genai
from fastapi import HTTPException
from app.core.logger import get_logger

logger = get_logger(__name__)
logger.info("LOADED: %s", os.path.abspath(__file__))

# Configure Gemini api key
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

MODEL_NAME = "models/gemini-2.5-flash" # using 2.5 flash model since it is good in json creation and understanding natural text.

def generate_text(prompt: str, model: str = MODEL_NAME):
    """Sends a prompt to the Gemini API and returns the generated JSON response text."""

    try:
        model_instance = genai.GenerativeModel(model)
        response = model_instance.generate_content(
            [prompt],
            generation_config={
                "temperature": 0, 
                "response_mime_type": "application/json"
            },
        )

        # Handle all possible response types
        if hasattr(response, "text") and response.text:
            return response.text
        elif getattr(response, "candidates", None):
            for c in response.candidates:
                if c.content and c.content.parts:
                    part = c.content.parts[0]
                    if hasattr(part, "text") and part.text:
                        return part.text
                    if hasattr(part, "data") and part.data:
                        return part.data.decode("utf-8")
        raise HTTPException(status_code=500, detail="Gemini returned empty response.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calling Gemini API: {e}")