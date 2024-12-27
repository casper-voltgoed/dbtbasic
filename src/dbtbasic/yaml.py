import yaml


def load_yaml_file(yaml_file_path) -> dict:
    with open(yaml_file_path, 'r') as stream:
        result = yaml.safe_load(stream)
    return result
