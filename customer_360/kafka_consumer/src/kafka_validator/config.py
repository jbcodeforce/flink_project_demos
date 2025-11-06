"""
Configuration management for Kafka validator.
"""

import os
from pathlib import Path
from typing import Dict, Optional
import yaml


class ConfigManager:
    """
    Load and manage configuration from config.yaml.
    
    Handles:
    - Loading YAML configuration
    - Building Kafka consumer configuration
    - Building Schema Registry configuration
    - Resolving credentials from environment variables
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to config.yaml file. If None, uses CONFIG_FILE env variable.
        """
        self.config_path = self._resolve_config_path(config_path)
        self.config = self._load_config()
    
    def _resolve_config_path(self, config_path: Optional[str]) -> Path:
        """Resolve configuration file path."""
        if config_path:
            return Path(config_path)
        
        # Try CONFIG_FILE environment variable
        env_config = os.getenv('CONFIG_FILE')
        if env_config:
            return Path(env_config)
        
        # Try default locations
        default_locations = [
            Path.home() / '.shift_left' / 'config.yaml',
            Path('config.yaml'),
        ]
        
        for location in default_locations:
            if location.exists():
                return location
        
        raise FileNotFoundError(
            "Configuration file not found. "
            "Specify --config option or set CONFIG_FILE environment variable."
        )
    
    def _load_config(self) -> Dict:
        """Load YAML configuration file."""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get_kafka_config(self, group_id: Optional[str] = None, 
                        from_beginning: bool = False) -> Dict[str, str]:
        """
        Build Kafka consumer configuration.
        
        Args:
            group_id: Consumer group ID (generated if None)
            from_beginning: Start from earliest or latest offset
            
        Returns:
            Dictionary of Kafka consumer configuration
        """
        import uuid
        
        config = {
            'bootstrap.servers': self.config['kafka']['bootstrap.servers'],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self._get_kafka_api_key(),
            'sasl.password': self._get_kafka_api_secret(),
            'group.id': group_id or f'validator-{uuid.uuid4()}',
            'auto.offset.reset': 'earliest' if from_beginning else 'latest',
            'enable.auto.commit': False,
        }
        
        return config
    
    def get_schema_registry_config(self) -> Dict[str, str]:
        """
        Build Schema Registry configuration.
        
        Returns:
            Dictionary of Schema Registry configuration
        """
        return {
            'url': self._build_schema_registry_url(),
            'basic.auth.user.info': f'{self._get_sr_api_key()}:{self._get_sr_api_secret()}'
        }
    
    def _build_schema_registry_url(self) -> str:
        """
        Build Schema Registry URL for Confluent Cloud.
        
        Format: https://psrc-{region}.{provider}.confluent.cloud
        """
        region = self.config['confluent_cloud']['region']
        provider = self.config['confluent_cloud']['provider']
        return f'https://psrc-{region}.{provider}.confluent.cloud'
    
    def _get_kafka_api_key(self) -> str:
        """Get Kafka API key from environment or config."""
        key = os.getenv('KAFKA_API_KEY')
        if not key:
            raise ValueError("KAFKA_API_KEY environment variable not set")
        return key
    
    def _get_kafka_api_secret(self) -> str:
        """Get Kafka API secret from environment or config."""
        secret = os.getenv('KAFKA_API_SECRET')
        if not secret:
            raise ValueError("KAFKA_API_SECRET environment variable not set")
        return secret
    
    def _get_sr_api_key(self) -> str:
        """Get Schema Registry API key from environment or config."""
        key = os.getenv('SCHEMA_REGISTRY_API_KEY')
        if not key:
            raise ValueError("SCHEMA_REGISTRY_API_KEY environment variable not set")
        return key
    
    def _get_sr_api_secret(self) -> str:
        """Get Schema Registry API secret from environment or config."""
        secret = os.getenv('SCHEMA_REGISTRY_API_SECRET')
        if not secret:
            raise ValueError("SCHEMA_REGISTRY_API_SECRET environment variable not set")
        return secret

