
import sys
import os
import pandas as pd
import yaml
import importlib

REPO_NAME = 'sewer-nfl'
CWD = str(os.getcwd())
REPO_DIR = CWD[:CWD.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,REPO_DIR)

"""Module to build dataset from yml file."""
def build_dataset(input_file = 'source.yml'):
    source_config = yaml.safe_load(open(input_file))
    feature = source_config['data'][0]
    print(feature)
    mod = importlib.import_module(feature['file_path'], package=feature['function'])
    res = getattr(mod,feature['function'])(feature['args'])
    print(res)

build_dataset(input_file = './source_example.yml')
