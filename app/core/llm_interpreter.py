import json
from fastapi import HTTPException
from app.services.gemini_service import generate_text
import re
from app.core.logger import get_logger
import os
logger = get_logger(__name__)
logger.info("LOADED: %s", os.path.abspath(__file__))

SYSTEM_PROMPT = """
You are an AI data planner that converts natural-language Excel queries
into structured JSON instructions for a pandas executor.

Your output MUST be **valid JSON only** — no explanations, no markdown, no Python code.

Follow this exact format:

{
  "operation" : one of ["aggregate", "math", "filter", "pivot", "unpivot", "join", "date_ops", "text_analysis", "describe", "sample", "multi_step"],
  "parameters": {
      "column": "sales",
      "group_by": "region",
      "method": "sum",
      "sort_by": "sales",
      "order": "desc",
      "limit": 10
  }
}

Rules:
- Never use synonyms (like “aggregation”, “grouping”, etc.). Always match exactly one operation keyword from the above list.
- Never include text before or after JSON.
- Always return a single JSON object.
- If the query mentions sorting or 'top N', include 'sort_by', 'order', and 'limit'.
- All keys must be lowercase.
- Use field names directly from the query (e.g., 'sales', 'region').
"""

def call_llm_for_plan(user_query: str, sample_columns=None):
    """Sends the user query to the LLM, extracts and returns the generated JSON plan for execution."""
    logger.info("Starting LLM plan generation.")
    logger.info("[LLM] Query -> %s", user_query)
    prompt = SYSTEM_PROMPT + "\n\nUser Query: " + user_query
    if sample_columns:
        prompt += f"\n\nAvailable columns: {', '.join(sample_columns)}"
    # get plan from llm
    raw_response = generate_text(prompt)
    logger.info("[LLM] Raw response (truncated 1000): %s", str(raw_response)[:1000])

    # Clean and extract JSON even if Gemini adds markdown/code
    match = re.search(r'\{[\s\S]*\}', raw_response)
    if not match:
        raise HTTPException(status_code=500, detail="Gemini did not return valid JSON")

    json_str = match.group(0)
    try:
        plan = json.loads(json_str)
        logger.info("Parsed LLM plan successfully.")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM output as JSON:")
        raise HTTPException(status_code=500, detail=f"Invalid JSON from LLM: {e}: {json_str[:300]}")

    return plan

def call_llm_verifier(user_query: str, plan: dict, result_sample: list) -> dict:
    """Asks the LLM to verify if the generated result matches the user query and returns a JSON verdict."""
    logger.info("Starting LLM plan verification.")
    verify_prompt = f"""User Query: {user_query}
Plan: {json.dumps(plan)}
Result sample (first rows): {json.dumps(result_sample[:5])}
Is this result consistent with the user query? Respond only JSON like: {{ "ok": true, "note": "..." }}."""

    txt = generate_text(verify_prompt, temperature=0.0, max_output_tokens=200)

    # Handle ```json code fences
    if txt.startswith("```"):
        txt = "\n".join(txt.splitlines()[1:-1])

    try:
        logger.info("Plan verification successful.")
        return json.loads(txt)
    except Exception:
        return {"ok": True, "note": "could not parse verifier output, assuming ok"}
