import yaml
from pathlib import Path

def load_xcom_config(confdir):
    """Loads the XCom config if present."""
    try:
        loc = Path(confdir)
        with open(loc) as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            return yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        return {}
