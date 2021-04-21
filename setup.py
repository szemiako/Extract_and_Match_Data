import argparse
from datetime import datetime
import pandas as pd
from pandas import DataFrame
import numpy as np
import os
import re
from typing import Callable

"""Import all needed packages / libraries, assign CONSTANTS, create argument parser."""

ENGINE = f'{os.path.dirname(os.path.abspath(__file__))}' # Base directory where this script runs. This folder contains all dependencies.
BASE_DIR = '{0}/{1}'.format(ENGINE.split('\\')[0], 'reports') # Folder where input files are stored and output files are written.

def make_arg_parser():
    """Create an argument parser so that we can pass through paramters in order to scale the solution up to future customers."""
    parser = argparse.ArgumentParser(description="""
        A script which attempts to take a list of accounts included on
        a customer's vendor feeds and determine which accounts are not
        disclosed in the customer's interface. In addition, we attempt to
        "match" them back to possible users disclosed in in their interface.

        Example:
            python run.py --server PROD --company 42

        See -h/--help for more information.
    """)
    parser.add_argument('-s', '--server', help='The production server where the customer is hosted.', required=True, choices=['JEFF', 'PROD', 'IRE', 'CS1P'])
    parser.add_argument('-c', '--company', help='The ID of the corresponding customer.', required=True)
    return parser.parse_args()