"""
Configuration management module for PMG Issue AI Pipeline
Handles loading and validation of configuration settings
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors"""
    pass


class Settings:
    """
    Configuration management class that loads and provides access to all settings
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize settings by loading configuration
        
        Args:
            config_path: Path to configuration YAML file. If None, uses default path.
        """
        self.config_path = config_path or self._get_default_config_path()
        self.config = self._load_config()
        self._validate_config()
        
    def _get_default_config_path(self) -> str:
        """Get the default configuration file path"""
        current_dir = Path(__file__).parent
        return str(current_dir / "config.yaml")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            if not os.path.exists(self.config_path):
                raise ConfigurationError(f"Configuration file not found: {self.config_path}")
            
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            
            if not config:
                raise ConfigurationError("Configuration file is empty or invalid")
            
            logger.info(f"Configuration loaded from: {self.config_path}")
            return config
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")
    
    def _validate_config(self) -> None:
        """Validate required configuration sections and keys"""

        required_sections = [
            'pipeline', 'logging', 'data_paths', 'pmg_api', 'llm_api', 
            'embeddings', 'database'  # Make sure database is required
        ]
        
        for section in required_sections:
            if section not in self.config:
                raise ConfigurationError(f"Required configuration section missing: {section}")
        
        # Validate PMG API configuration
        pmg_api = self.config.get('pmg_api', {})
        required_pmg_keys = ['base_url', 'auth_endpoint', 'endpoints', 'auth']
        for key in required_pmg_keys:
            if key not in pmg_api:
                raise ConfigurationError(f"Required PMG API configuration missing: {key}")
        
        # Validate authentication credentials
        auth = pmg_api.get('auth', {})
        if not auth.get('username') or not auth.get('password'):
            raise ConfigurationError("PMG API authentication credentials are required")
        
        # Validate LLM API configuration
        llm_api = self.config.get('llm_api', {})
        required_llm_keys = ['url', 'token', 'model']
        for key in required_llm_keys:
            if key not in llm_api:
                raise ConfigurationError(f"Required LLM API configuration missing: {key}")
        
        logger.info("Configuration validation successful")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key path (supports nested keys with dot notation)
        
        Args:
            key: Configuration key (e.g., 'pmg_api.base_url')
            default: Default value if key is not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration"""
        return self.config.get('pipeline', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.config.get('logging', {})
    
    def get_data_paths(self) -> Dict[str, str]:
        """Get data paths configuration"""
        return self.config.get('data_paths', {})
    
    def get_pmg_api_config(self) -> Dict[str, Any]:
        """Get PMG API configuration"""
        return self.config.get('pmg_api', {})
    
    def get_llm_api_config(self) -> Dict[str, Any]:
        """Get LLM API configuration"""
        return self.config.get('llm_api', {})
    
    def get_embeddings_config(self) -> Dict[str, Any]:
        """Get embeddings configuration"""
        return self.config.get('embeddings', {})
    
    def get_feature_engineering_config(self) -> Dict[str, Any]:
        """Get feature engineering configuration"""
        return self.config.get('feature_engineering', {})
    
    def get_similar_issues_config(self) -> Dict[str, Any]:
        """Get similar issues configuration"""
        return self.config.get('similar_issues', {})
    
    def get_timeline_config(self) -> Dict[str, Any]:
        """Get timeline prediction configuration"""
        return self.config.get('timeline', {})
    
    def get_action_recommendation_config(self) -> Dict[str, Any]:
        """Get action recommendation configuration"""
        return self.config.get('action_recommendation', {})
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self.config.get('database', {})
    
    def get_debug_config(self) -> Dict[str, Any]:
        """Get debug configuration"""
        return self.config.get('debug', {})
    
    def is_debug_mode(self) -> bool:
        """Check if debug mode is enabled"""
        return self.get('debug.verbose_logging', False)
    
    def is_input_fetch_enabled(self) -> bool:
        """Check if input fetch is enabled"""
        return self.get('pipeline.input_fetch', True)
    
    def get_start_stage(self) -> str:
        """Get pipeline start stage"""
        return self.get('pipeline.start_stage', 'DATA_FETCH')
    
    def get_end_stage(self) -> str:
        """Get pipeline end stage"""
        return self.get('pipeline.end_stage', 'POST_PROCESSING')

    def get_post_processing_config(self) -> Dict[str, Any]:
        """Get post-processing configuration"""
        return self.config.get('post_processing', {
            'validate_all_inputs': True,
            'generate_summary_report': True
        })

    def get_environment_config(self) -> Dict[str, Any]:
        """Get environment configuration"""
        return self.config.get('environment', {
            'current': 'development',
            'debug_mode': True
        })

    def get_error_handling_config(self) -> Dict[str, Any]:
        """Get error handling configuration"""
        return self.config.get('error_handling', {
            'retry_failed_operations': True,
            'max_retries': 3,
            'continue_on_errors': True
        })

    def get_current_environment(self) -> str:
        """Get current environment name"""
        return self.get('environment.current', 'development')
    
    def create_directories(self) -> None:
        """Create necessary directories based on configuration"""
        dirs_to_create = [
            self.get('logging.logs_path'),
            self.get('data_paths.raw_data_dir'),
            self.get('data_paths.processed_data_dir'),
            self.get('data_paths.features_data_dir'),
            self.get('data_paths.embeddings_dir'),
            self.get('data_paths.models_dir'),
            self.get('data_paths.outputs_dir'),
            # Specific output directories
            os.path.dirname(self.get('data_paths.descriptions_file')),
            os.path.dirname(self.get('data_paths.classifications_file')),
            os.path.dirname(self.get('data_paths.similar_issues_file')),
            os.path.dirname(self.get('data_paths.timeline_predictions_file')),
            os.path.dirname(self.get('data_paths.action_recommendations_file')),
            os.path.dirname(self.get('data_paths.final_results_file')),
            os.path.dirname(self.get('data_paths.final_excel_file')),
            os.path.dirname(self.get('data_paths.final_json_file')),
        ]
        
        for dir_path in dirs_to_create:
            if dir_path:
                Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        logger.info("Created necessary directories")
    
    def update_config(self, key: str, value: Any) -> None:
        """
        Update configuration value at runtime
        
        Args:
            key: Configuration key (supports dot notation)
            value: New value
        """
        keys = key.split('.')
        config = self.config
        
        # Navigate to the parent of the target key
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        # Set the value
        config[keys[-1]] = value
        logger.info(f"Updated configuration: {key} = {value}")
    
    def save_config(self, output_path: Optional[str] = None) -> None:
        """
        Save current configuration to file
        
        Args:
            output_path: Path to save configuration. If None, overwrites current file.
        """
        save_path = output_path or self.config_path
        
        try:
            with open(save_path, 'w', encoding='utf-8') as file:
                yaml.dump(self.config, file, default_flow_style=False, indent=2)
            
            logger.info(f"Configuration saved to: {save_path}")
            
        except Exception as e:
            raise ConfigurationError(f"Error saving configuration: {e}")


# Global settings instance
_settings = None


def get_settings(config_path: Optional[str] = None) -> Settings:
    """
    Get global settings instance (singleton pattern)
    
    Args:
        config_path: Path to configuration file (only used on first call)
        
    Returns:
        Settings instance
    """
    global _settings
    
    if _settings is None:
        _settings = Settings(config_path)
    
    return _settings


def reload_settings(config_path: Optional[str] = None) -> Settings:
    """
    Force reload of settings (useful for testing or config changes)
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        New settings instance
    """
    global _settings
    _settings = Settings(config_path)
    return _settings