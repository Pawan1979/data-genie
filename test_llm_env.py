import os
from pathlib import Path

# Load .env file manually
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(env_file)
    print(f"[OK] Loaded .env from: {env_file}")
else:
    print(f"[ERROR] .env file not found at: {env_file}")

# Test the value
llm_provider = os.getenv("LLM_PROVIDER", "not configured").upper()
llm_model = os.getenv("LLM_MODEL", "not configured")
openai_key = os.getenv("OPENAI_API_KEY", "not set")

print("\n--- LLM Configuration ---")
print(f"LLM_PROVIDER: {llm_provider}")
print(f"LLM_MODEL: {llm_model}")
if openai_key != "not set":
    print(f"OPENAI_API_KEY: [SET] - starts with {openai_key[:15]}...")
else:
    print(f"OPENAI_API_KEY: [NOT SET]")
