# Goal: Identify pipelines from input file (source.yml) and run them on initialization to build modeling dataset.and

import pandas as pd
import yaml

def build_dataset(input_file = 'source.yml'):
    source_config = yaml.safe_load(open(input_file))
    
    print(source_config)

build_dataset(input_file = './source_example.yml')