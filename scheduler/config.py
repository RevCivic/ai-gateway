"""
Configuration management for the AI Gateway scheduler.
"""

import os
from typing import Dict, List, Any, Optional
from dataclasses import asdict

from models import WorkerConfig


class Config:
    """Configuration manager for the AI Gateway."""
    
    def __init__(self):
        """Initialize configuration from environment."""
        # LiteLLM configuration
        self.litellm_url = os.getenv(
            "LITELLM_URL",
            "http://litellm:4000"
        ).rstrip("/")
        
        self.litellm_master_key = os.environ.get("LITELLM_MASTER_KEY", "")
        
        # Worker URLs
        self.kaideshark_agent_url = os.environ.get("KAIDESHARK_AGENT_URL", "")
        self.kharessaadara_agent_url = os.environ.get("KHARESSAADARA_AGENT_URL", "")
        self.galactus_ollama_url = os.environ.get("GALACTUS_OLLAMA_URL", "").rstrip("/")
        
        # Timeout configuration
        self.streaming_first_byte_timeout = float(
            os.getenv("STREAMING_FIRST_BYTE_TIMEOUT", "30.0")
        )
        self.streaming_idle_timeout = float(
            os.getenv("STREAMING_IDLE_TIMEOUT", "60.0")
        )
        self.worker_health_check_timeout = float(
            os.getenv("WORKER_HEALTH_CHECK_TIMEOUT", "3.0")
        )
        
        # Health check caching
        self.health_check_cache_ttl = float(
            os.getenv("HEALTH_CHECK_CACHE_TTL", "10.0")
        )
        
        # Circuit breaker configuration
        self.circuit_breaker_failure_threshold = int(
            os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "3")
        )
        self.circuit_breaker_recovery_base = float(
            os.getenv("CIRCUIT_BREAKER_RECOVERY_BASE", "5.0")
        )
        self.circuit_breaker_recovery_max = float(
            os.getenv("CIRCUIT_BREAKER_RECOVERY_MAX", "300.0")
        )
        
        # Request queuing
        self.max_queue_size = int(
            os.getenv("MAX_QUEUE_SIZE", "1000")
        )
        self.request_timeout = float(
            os.getenv("REQUEST_TIMEOUT", "900.0")
        )
        
        # HTTP client configuration
        self.max_connections = int(
            os.getenv("MAX_CONNECTIONS", "100")
        )
        self.max_keepalive_connections = int(
            os.getenv("MAX_KEEPALIVE_CONNECTIONS", "50")
        )
        
        # Hardcoded routes (can be extended from config file in future)
        self.routes = {
            "fast": [
                {
                    "worker": "KaideShark",
                    "deployment": "fast-kaide",
                    "tps": 111.85,
                },
                {
                    "worker": "KharessaAdara",
                    "deployment": "fast-kharessa",
                    "tps": 62.97,
                },
                {
                    "worker": "Galactus",
                    "deployment": "fast-galactus",
                    "tps": 29.84,
                },
            ],
            "balanced": [
                {
                    "worker": "KaideShark",
                    "deployment": "balanced-kaide",
                    "tps": 70.64,
                },
                {
                    "worker": "KharessaAdara",
                    "deployment": "balanced-kharessa",
                    "tps": 39.80,
                },
            ],
            "heavy": [
                {
                    "worker": "KaideShark",
                    "deployment": "heavy-kaide",
                    "tps": 9.45,
                },
            ],
            "background": [
                {
                    "worker": "Galactus",
                    "deployment": "background-galactus",
                    "tps": 29.84,
                },
            ],
        }
        
        # Worker URLs configuration
        self.worker_urls = {
            "KaideShark": self.kaideshark_agent_url,
            "KharessaAdara": self.kharessaadara_agent_url,
        }
        
        self.env_vars = {
            "GALACTUS_OLLAMA_URL": self.galactus_ollama_url,
            "KAIDESHARK_AGENT_URL": self.kaideshark_agent_url,
            "KHARESSAADARA_AGENT_URL": self.kharessaadara_agent_url,
        }
    
    def get_routes(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get available model routes."""
        return self.routes
    
    def get_worker_urls(self) -> Dict[str, str]:
        """Get worker URLs."""
        return self.worker_urls
    
    def validate(self) -> bool:
        """
        Validate configuration is complete.
        
        Returns:
            True if valid, raises exception otherwise
        """
        if not self.litellm_master_key:
            raise ValueError("LITELLM_MASTER_KEY not set")
        
        if not self.kaideshark_agent_url or not self.kharessaadara_agent_url:
            raise ValueError("Agent URLs not configured")
        
        if not self.galactus_ollama_url:
            raise ValueError("Galactus Ollama URL not configured")
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "litellm_url": self.litellm_url,
            "streaming_first_byte_timeout": self.streaming_first_byte_timeout,
            "streaming_idle_timeout": self.streaming_idle_timeout,
            "worker_health_check_timeout": self.worker_health_check_timeout,
            "health_check_cache_ttl": self.health_check_cache_ttl,
            "circuit_breaker_failure_threshold": self.circuit_breaker_failure_threshold,
            "circuit_breaker_recovery_base": self.circuit_breaker_recovery_base,
            "circuit_breaker_recovery_max": self.circuit_breaker_recovery_max,
            "max_queue_size": self.max_queue_size,
            "request_timeout": self.request_timeout,
            "max_connections": self.max_connections,
            "max_keepalive_connections": self.max_keepalive_connections,
        }


# Global configuration instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get or create global configuration."""
    global _config
    if _config is None:
        _config = Config()
        _config.validate()
    return _config


def reset_config() -> None:
    """Reset configuration (useful for testing)."""
    global _config
    _config = None
