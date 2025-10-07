"""
Module for "forging" data for demonstration purposes
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string

def forge_barcode(prefix='', total_length=10, placeholder_location=None, placeholder='-'):
    """
    Generate a parameterizable fake barcode - MAXIMUM FLEXIBILITY!
    
    Parameters:
    -----------
    prefix : str
        Optional prefix for the barcode (e.g., 'LAB', 'SMPL')
    total_length : int
        Total length of the barcode INCLUDING prefix
    placeholder : str
        Character(s) to insert at placeholder_location (e.g., '-', ' ')
    placeholder_location : int
        Position to insert placeholder (0-indexed, relative to total length)
        
    Returns:
    --------
    str : Generated barcode
    
    Examples:
    ---------
    >>> forge_barcode(length=10, placeholder_location=5, placeholder='-')
    '12355-39234'
    >>> forge_barcode(prefix='LAB', length=9, placeholder_location=6, placeholder=' ')
    'LAB123 456'
    """
    # Calculate how many random chars we need (total - prefix length - placeholder length)
    prefix_len = len(prefix)
    placeholder_len = len(placeholder) if placeholder_location is not None else 0
    random_chars_needed = total_length - prefix_len - placeholder_len
    
    if random_chars_needed <= 0:
        raise ValueError(f"Length {total_length} too short for prefix '{prefix}' and placeholder!")

    random_part = ''.join(np.random.randint(0, 9, size=random_chars_needed).astype(str))

    if placeholder_location is not None:

        insert_pos = placeholder_location - prefix_len
        if 0 <= insert_pos <= len(random_part):
            random_part = random_part[:insert_pos] + placeholder + random_part[insert_pos:]
    
    return prefix + random_part
