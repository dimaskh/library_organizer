from pathlib import Path
import yaml
from typing import Dict


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""

    pass


class Config:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()

    def __getitem__(self, key):
        return self.config[key]

    def _load_config(self) -> Dict:
        """Load and merge configurations"""
        # Load default config
        default_config = self._load_yaml("config/default_config.yaml")

        # Load user config and merge
        if self.config_path.exists():
            user_config = self._load_yaml(self.config_path)
            return self._merge_configs(default_config, user_config)
        return default_config

    def _load_yaml(self, path: Path) -> Dict:
        """Load YAML file"""
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            msg = f"Error loading config from {path}: {str(e)}"
            raise ConfigurationError(msg)

    def _merge_configs(self, default: Dict, user: Dict) -> Dict:
        """Deep merge two configurations"""
        merged = default.copy()
        for key, value in user.items():
            if (
                key in merged
                and isinstance(merged[key], dict)
                and isinstance(value, dict)
            ):
                merged[key] = self._merge_configs(merged[key], value)
            else:
                merged[key] = value
        return merged

    def _validate_config(self) -> None:
        """Validate configuration"""
        required_keys = ["directories", "topics", "reading_speeds"]
        for key in required_keys:
            if key not in self.config:
                raise ConfigurationError(f"Missing required config key: {key}")
