'''
input.py - Read and marshal scheduling input (targets, telescopes, etc)

description

Author: Eric Saunders
November 2011
'''

import ast

def file_to_dicts(filename):
    fh = open(filename, 'r')

    data = fh.read()

    return ast.literal_eval(data)
