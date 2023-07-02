"""Module to build dataset from yml file."""
import sys
import os
import importlib
import yaml

REPO_NAME = 'sewer-nfl'
CWD = str(os.getcwd())
REPO_DIR = CWD[:CWD.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,REPO_DIR)

def build_dataset(input_file = 'source.yml'):
    """Function Docstring here"""
    with open(input_file,encoding='utf-8') as file:
        source_config = yaml.safe_load(file)
    feature = source_config['data'][0]
    print(feature)
    mod = importlib.import_module(feature['file_path'], package=feature['function'])
    res = getattr(mod,feature['function'])(feature['args'])
    print(res)

build_dataset(input_file = './source_example.yml')
