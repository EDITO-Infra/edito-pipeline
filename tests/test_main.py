import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import logging
from typing import TYPE_CHECKING, List, Tuple
from datetime import datetime, date

from metagisconfig2stac.pipeline import Pipeline
from metagisconfig2stac.utils.core import CoreUtils
datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
logger = CoreUtils.get_logger(append_logs=False)

def test_full_pipeline():
    config = CoreUtils.load_yaml(f'{datadir}/config.yaml')
    config = CoreUtils.validate_config(config)
    pipeline = Pipeline(config)
    pipeline.run_full_pipeline()

if __name__ == '__main__':
    test_full_pipeline()