import google.generativeai as genai
import os
from dotenv import load_dotenv
load_dotenv()
# Load your API key (make sure it's in your .env)
api_key=os.getenv("GEMINI_API_KEY")
print(api_key)
genai.configure(api_key=api_key)

# List all models
for model in genai.list_models():
    print(model.name, "→", model.supported_generation_methods)
