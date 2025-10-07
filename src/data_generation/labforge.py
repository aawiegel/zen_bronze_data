"""
Module for "forging" data for demonstration purposes
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string

def generate_barcode(prefix: str='', length: int=10, placeholder_location: int=None, placeholder: str=" "):
    """
    Generate a fake barcode based on the following parameters
    prefix (str): An optional prefix to the barcode (e.g., ABC)
    length (int): the string length of the barcode
    placeholder (str): a placeholder string to break up the barcode
    placeholder_location (int): the location of the placeholder in the string
    """
    barcode = prefix
    for i in range(length):
        if placeholder_location is not None and i == placeholder_location:
            barcode += placeholder
        barcode += random.randint(0, 9)
    
    return barcode
