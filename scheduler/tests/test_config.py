"""
Test cases for configuration management.
"""

import pytest
import os
from unittest.mock import patch
from config import Config, get_config, reset_config


class TestConfig:
    """Test Config class."""
    
    def test_config_from_environment(self):
        """Test loading configuration from environment."""
        with patch.dict(os.environ, {
            "LITELLM_MASTER_KEY": "test-key",
            "KAIDESHARK_AGENT_URL": "http://kaide:8000",
            "KHARESSAADARA_AGENT_URL": "http://kharessa:8000",
            "GALACTUS_OLLAMA_URL": "http://galactus:11434",
        }):
            config = Config()
            assert config.litellm_master_key == "test-key"
            assert config.kaideshark_agent_url == "http://kaide:8000"
    
    def test_config_timeouts(self):
        """Test timeout configuration."""
        with patch.dict(os.environ, {
            "STREAMING_FIRST_BYTE_TIMEOUT": "45.0",
            "LITELLM_MASTER_KEY": "key",
            "KAIDESHARK_AGENT_URL": "url1",
            "KHARESSAADARA_AGENT_URL": "url2",
            "GALACTUS_OLLAMA_URL": "url3",
        }):
            config = Config()
            assert config.streaming_first_byte_timeout == 45.0
    
    def test_config_circuit_breaker(self):
        """Test circuit breaker configuration."""
        with patch.dict(os.environ, {
            "CIRCUIT_BREAKER_FAILURE_THRESHOLD": "5",
            "CIRCUIT_BREAKER_RECOVERY_BASE": "10.0",
            "LITELLM_MASTER_KEY": "key",
            "KAIDESHARK_AGENT_URL": "url1",
            "KHARESSAADARA_AGENT_URL": "url2",
            "GALACTUS_OLLAMA_URL": "url3",
        }):
            config = Config()
            assert config.circuit_breaker_failure_threshold == 5
            assert config.circuit_breaker_recovery_base == 10.0
    
    def test_config_validation(self):
        """Test configuration validation."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError):
                config = Config()
                config.validate()
    
    def test_config_to_dict(self):
        """Test converting config to dictionary."""
        with patch.dict(os.environ, {
            "LITELLM_MASTER_KEY": "key",
            "KAIDESHARK_AGENT_URL": "url1",
            "KHARESSAADARA_AGENT_URL": "url2",
            "GALACTUS_OLLAMA_URL": "url3",
        }):
            config = Config()
            config_dict = config.to_dict()
            
            assert "litellm_url" in config_dict
            assert "streaming_first_byte_timeout" in config_dict
            assert "circuit_breaker_failure_threshold" in config_dict
    
    def test_get_routes(self):
        """Test getting available routes."""
        with patch.dict(os.environ, {
            "LITELLM_MASTER_KEY": "key",
            "KAIDESHARK_AGENT_URL": "url1",
            "KHARESSAADARA_AGENT_URL": "url2",
            "GALACTUS_OLLAMA_URL": "url3",
        }):
            config = Config()
            routes = config.get_routes()
            
            assert "fast" in routes
            assert "balanced" in routes
            assert "heavy" in routes
            assert "background" in routes


class TestGlobalConfig:
    """Test global configuration management."""
    
    def test_get_config_singleton(self):
        """Test that get_config returns singleton."""
        reset_config()
        with patch.dict(os.environ, {
            "LITELLM_MASTER_KEY": "key",
            "KAIDESHARK_AGENT_URL": "url1",
            "KHARESSAADARA_AGENT_URL": "url2",
            "GALACTUS_OLLAMA_URL": "url3",
        }):
            config1 = get_config()
            config2 = get_config()
            assert config1 is config2
    
    def test_reset_config(self):
        """Test resetting configuration."""
        reset_config()
        with patch.dict(os.environ, {
            "LITELLM_MASTER_KEY": "key",
            "KAIDESHARK_AGENT_URL": "url1",
            "KHARESSAADARA_AGENT_URL": "url2",
            "GALACTUS_OLLAMA_URL": "url3",
        }):
            config1 = get_config()
            reset_config()
            config2 = get_config()
            assert config1 is not config2
