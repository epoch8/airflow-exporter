import yaml
from pathlib import Path

CONFIG_FILE = Path.cwd() / "config.yaml"


def load_xcom_config():
    """Loads the XCom config if present."""
    try:
        with open(CONFIG_FILE) as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            return yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        return {}