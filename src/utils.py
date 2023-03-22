import functools
import yaml
import os


@functools.lru_cache()
def load_config():
    with open(os.path.join(os.path.dirname(__file__), "../config_files/mycredentials.yaml")) as f:
        return yaml.load(f, Loader=yaml.FullLoader)
