import yaml
from pathlib import Path
from typing import Dict
from config import Config


class ConnectionManager:
    def __init__(self):
        self.config_path = Path(Config.CONFIG_FILE)
        self.connections = self._load_connections()

    def _load_connections(self) -> Dict:
        if self.config_path.exists():
            with open(self.config_path, "r") as f:
                return yaml.safe_load(f) or {}
        return {}

    def save_connections(self):
        with open(self.config_path, "w") as f:
            yaml.dump(self.connections, f)

    def add_connection(self, name: str, details: Dict):
        self.connections[name] = details
        self.save_connections()

    def remove_connection(self, name: str):
        if name in self.connections:
            del self.connections[name]
            self.save_connections()
