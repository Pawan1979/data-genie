"""LLM Client — wrapper around liteLLM for provider-agnostic API calls."""

import os
from typing import List, Dict, Optional
from datetime import datetime

try:
    import litellm
except ImportError:
    litellm = None

class LLMClient:
    """Chat client supporting multiple LLM providers via liteLLM."""

    def __init__(self):
        """Initialize LLM client from .env configuration."""
        self.provider = os.getenv("LLM_PROVIDER", "").strip().lower()
        self.model = os.getenv("LLM_MODEL", "").strip()
        self.api_key = self._get_api_key()

        # Azure-specific config
        self.azure_endpoint = os.getenv("AZURE_ENDPOINT", "").strip()
        self.azure_deployment = os.getenv("AZURE_DEPLOYMENT", "").strip()
        self.azure_api_version = os.getenv("AZURE_API_VERSION", "2024-02-15-preview").strip()

        # Validate configuration
        self.available = False
        config_errors = []

        if self.provider == "azure":
            if not self.api_key:
                config_errors.append("AZURE_API_KEY not set")
            if not self.azure_endpoint:
                config_errors.append("AZURE_ENDPOINT not set")
            if not self.azure_deployment:
                config_errors.append("AZURE_DEPLOYMENT not set")
            self.available = len(config_errors) == 0
        elif self.provider in ["openai", "anthropic", "google", "groq"]:
            if not self.model:
                config_errors.append("LLM_MODEL not set")
            if not self.api_key:
                config_errors.append(f"{self.provider.upper()}_API_KEY not set")
            self.available = len(config_errors) == 0
        else:
            if self.provider:
                config_errors.append(f"Unknown provider: {self.provider}")
            else:
                config_errors.append("LLM_PROVIDER not set")

        if not self.available:
            error_msg = "⚠ LLM not configured — review agent disabled\n  " + "\n  ".join(config_errors)
            print(error_msg)

    def _get_api_key(self) -> Optional[str]:
        """Get API key for configured provider."""
        if self.provider == "anthropic":
            return os.getenv("ANTHROPIC_API_KEY", "").strip()
        elif self.provider == "openai":
            return os.getenv("OPENAI_API_KEY", "").strip()
        elif self.provider == "azure":
            return os.getenv("AZURE_API_KEY", "").strip()
        elif self.provider == "google":
            return os.getenv("GOOGLE_API_KEY", "").strip()
        elif self.provider == "groq":
            return os.getenv("GROQ_API_KEY", "").strip()
        return None

    def chat(
        self,
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
        max_retries: int = 1,
    ) -> str:
        """
        Send chat message to LLM.

        Args:
            messages: List of {"role": "user"/"assistant", "content": "..."}
            system_prompt: Optional system message to prepend
            max_retries: Number of retries on rate limit

        Returns:
            Response text from LLM, or error message if unavailable
        """
        if not self.available:
            return "LLM not configured (missing API key or config)"

        if not litellm:
            return "liteLLM not installed"

        try:
            # Build messages with optional system prompt
            full_messages = messages
            if system_prompt:
                full_messages = [
                    {"role": "system", "content": system_prompt},
                    *messages,
                ]

            # Call LLM with timeout and retry logic
            retries = 0
            while retries <= max_retries:
                try:
                    # Prepare completion params
                    completion_params = {
                        "messages": full_messages,
                        "temperature": 0.7,
                        "timeout": 30,
                    }

                    # For Azure, use deployment-specific format
                    if self.provider == "azure":
                        completion_params["model"] = f"azure/{self.azure_deployment}"
                        completion_params["api_base"] = self.azure_endpoint
                        completion_params["api_key"] = self.api_key
                        completion_params["api_version"] = self.azure_api_version
                    else:
                        completion_params["model"] = self.model

                    response = litellm.completion(**completion_params)

                    # Extract text from response
                    content = response.choices[0].message.content
                    return content

                except litellm.RateLimitError:
                    retries += 1
                    if retries > max_retries:
                        return "Rate limited — too many requests"
                    # Retry
                    continue

        except Exception as e:
            return f"LLM error: {type(e).__name__}: {str(e)}"

        return ""


def get_llm_client() -> LLMClient:
    """Get or create global LLM client."""
    global _llm_client
    if "_llm_client" not in globals():
        _llm_client = LLMClient()
    return _llm_client
