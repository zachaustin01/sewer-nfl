# Goal: Identify pipelines from input file (source.yml) and run them on initialization to build modeling dataset.and

REPO_NAME = 'sewer-nfl'
import sys, os
cwd = str(os.getcwd())
repo_dir = cwd[:cwd.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,repo_dir)


import pandas as pd
import yaml
import importlib

def build_dataset(input_file = 'source.yml'):
    source_config = yaml.safe_load(open(input_file))
    feature = source_config['data'][0]
    print(feature)
    mod = importlib.import_module(feature['file_path'], package=feature['function'])
    res = getattr(mod,feature['function'])(feature['args'])
    print(res)
    

build_dataset(input_file = './source_example.yml')