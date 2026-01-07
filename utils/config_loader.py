"""
Configuration Loader
Loads and manages scheduler configuration from scheduler_config.json
"""
import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime


class ConfigLoader:
    """
    Configuration loader for scheduler
    Manages loading, validation, and access to scheduler configuration
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize configuration loader
        
        Args:
            config_path: Path to scheduler_config.json
        """
        if config_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, '..', 'scheduler_config.json')
        
        self.config_path = config_path
        self.config = {}
        self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from file
        
        Returns:
            Configuration dictionary
        """
        try:
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
            
            # Validate configuration
            self._validate_config()
            
            return self.config
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def _validate_config(self):
        """Validate configuration structure"""
        required_keys = ['timezone', 'trading_calendar', 'strategies', 'execution_settings', 'logging']
        
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration key: {key}")
        
        # Validate strategies
        if not isinstance(self.config['strategies'], dict):
            raise ValueError("'strategies' must be a dictionary")
        
        for strategy_name, strategy_config in self.config['strategies'].items():
            self._validate_strategy_config(strategy_name, strategy_config)
    
    def _validate_strategy_config(self, strategy_name: str, strategy_config: Dict[str, Any]):
        """
        Validate individual strategy configuration
        
        Args:
            strategy_name: Name of the strategy
            strategy_config: Strategy configuration dictionary
        """
        # Check if 'enabled' key exists
        if 'enabled' not in strategy_config:
            raise ValueError(f"Strategy '{strategy_name}' missing required key: enabled")
        
        # Check if this is a signal generation/execution strategy
        if 'signal_generation' in strategy_config:
            # Validate signal generation config
            gen_config = strategy_config['signal_generation']
            if 'frequency' not in gen_config or 'time' not in gen_config:
                raise ValueError(f"Strategy '{strategy_name}' signal_generation missing frequency or time")
            
            # Validate signal execution config
            if 'signal_execution' not in strategy_config:
                raise ValueError(f"Strategy '{strategy_name}' missing signal_execution")
            
            exec_config = strategy_config['signal_execution']
            if 'frequency' not in exec_config or 'time' not in exec_config:
                raise ValueError(f"Strategy '{strategy_name}' signal_execution missing frequency or time")
        
        # Check if this is a data fetch strategy
        elif 'data_fetch' in strategy_config:
            # Validate data fetch config
            fetch_config = strategy_config['data_fetch']
            if 'frequency' not in fetch_config or 'time' not in fetch_config:
                raise ValueError(f"Strategy '{strategy_name}' data_fetch missing frequency or time")
            
            # Validate fetch module
            if 'fetch_module' not in strategy_config:
                raise ValueError(f"Strategy '{strategy_name}' missing fetch_module")
        
        else:
            raise ValueError(f"Strategy '{strategy_name}' must have either 'signal_generation' or 'data_fetch' configuration")
    
    def reload_config(self) -> Dict[str, Any]:
        """
        Reload configuration from file
        
        Returns:
            Updated configuration dictionary
        """
        return self.load_config()
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get full configuration
        
        Returns:
            Configuration dictionary
        """
        return self.config
    
    def get_timezone(self) -> str:
        """
        Get configured timezone
        
        Returns:
            Timezone string (e.g., 'Asia/Kolkata')
        """
        return self.config.get('timezone', 'Asia/Kolkata')
    
    def get_trading_calendar(self) -> Dict[str, Any]:
        """
        Get trading calendar configuration
        
        Returns:
            Trading calendar configuration
        """
        return self.config.get('trading_calendar', {})
    
    def get_all_strategies(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all strategy configurations
        
        Returns:
            Dictionary of strategy configurations
        """
        return self.config.get('strategies', {})
    
    def get_strategy_config(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific strategy
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            Strategy configuration or None if not found
        """
        return self.config.get('strategies', {}).get(strategy_name)
    
    def is_strategy_enabled(self, strategy_name: str) -> bool:
        """
        Check if a strategy is enabled
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            True if enabled, False otherwise
        """
        strategy_config = self.get_strategy_config(strategy_name)
        if strategy_config is None:
            return False
        
        return strategy_config.get('enabled', False)
    
    def get_enabled_strategies(self) -> List[str]:
        """
        Get list of enabled strategy names
        
        Returns:
            List of enabled strategy names
        """
        enabled = []
        
        for strategy_name, strategy_config in self.get_all_strategies().items():
            if strategy_config.get('enabled', False):
                enabled.append(strategy_name)
        
        return enabled
    
    def get_execution_settings(self) -> Dict[str, Any]:
        """
        Get execution settings
        
        Returns:
            Execution settings dictionary
        """
        return self.config.get('execution_settings', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration
        
        Returns:
            Logging configuration dictionary
        """
        return self.config.get('logging', {})
    
    def get_max_retries(self) -> int:
        """Get maximum retry attempts for webhook execution"""
        return self.get_execution_settings().get('max_retries', 3)
    
    def get_retry_delay_minutes(self) -> int:
        """Get delay between retry attempts in minutes"""
        return self.get_execution_settings().get('retry_delay_minutes', 5)
    
    def get_webhook_timeout(self) -> int:
        """Get webhook timeout in seconds"""
        return self.get_execution_settings().get('webhook_timeout_seconds', 30)
    
    def should_cleanup_executed_signals(self) -> bool:
        """Check if executed signals should be cleaned up"""
        return self.get_execution_settings().get('cleanup_executed_signals', False)


# Singleton instance
_config_instance = None


def get_config_loader(config_path: str = None) -> ConfigLoader:
    """
    Get singleton configuration loader instance
    
    Args:
        config_path: Path to scheduler_config.json
        
    Returns:
        ConfigLoader instance
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigLoader(config_path)
    
    return _config_instance


def reload_config():
    """Reload configuration from file"""
    global _config_instance
    
    if _config_instance is not None:
        _config_instance.reload_config()


if __name__ == "__main__":
    # Test configuration loader
    loader = get_config_loader()
    
    print("Configuration Loader Test")
    print("=" * 60)
    
    print(f"\nTimezone: {loader.get_timezone()}")
    print(f"\nEnabled strategies: {loader.get_enabled_strategies()}")
    
    print("\nStrategy Configurations:")
    for strategy_name in loader.get_enabled_strategies():
        config = loader.get_strategy_config(strategy_name)
        print(f"\n{strategy_name}:")
        print(f"  Signal Generation: {config['signal_generation']['frequency']} at {config['signal_generation']['time']}")
        print(f"  Signal Execution: {config['signal_execution']['execution_rule']} at {config['signal_execution']['time']}")
    
    print(f"\nExecution Settings:")
    print(f"  Max Retries: {loader.get_max_retries()}")
    print(f"  Retry Delay: {loader.get_retry_delay_minutes()} minutes")
    print(f"  Webhook Timeout: {loader.get_webhook_timeout()} seconds")
    print(f"  Cleanup Executed Signals: {loader.should_cleanup_executed_signals()}")
