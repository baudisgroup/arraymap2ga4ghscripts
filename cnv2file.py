#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  2 10:04:18 2018

@author: bogao
"""


from pymongo import MongoClient

collection_in = MongoClient()['arraymap_ga4gh']['variants_cnv_grch36']

segments_36= {}
for sample in collection_in.find():
    segments_36[sample['id']] = [sample['reference_name'], sample['start'], sample['end']]

# Write cnvs to a file
with open('/Users/bogao/DataFiles/tmp/segments_36.txt', 'w') as fo:
    print(*['id', 'chro', 'start', 'stop'], sep='\t', file=fo)
    for k,v in segments_36.items():
        print(k, end='\t', file=fo)
        print(*v, sep='\t', file=fo)