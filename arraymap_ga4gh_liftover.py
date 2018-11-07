#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  2 12:42:26 2018

@author: bogao
"""

from pymongo import MongoClient

collection_in = MongoClient()['arraymap_ga4gh']['variants_cnv_grch36']

##########################################
##### Read in lifted file of grch37 ######
##########################################
segments_37 = {}
with open('/Users/bogao/DataFiles/tmp/segments_37.txt', 'r') as fi:
    next(fi)
    for line in fi:
        line = line.strip().split('\t')
        segments_37[line[0]] = line[1:]

# Generate new cnv collections by copying orignals and updating coordinates
cnv_37 = []
for sample in collection_in.find({},{'_id':0}):
    key = sample['id']
    if key in segments_37:
        sample['start'] = int(segments_37[key][1])
        sample['end'] = int(segments_37[key][2])
        sample['variantset_id'] = 'AM_VS_GRCH37'
        cnv_37.append(sample)
    

# write to db
collection_out = MongoClient()['arraymap_ga4gh']['variants_cnv_grch37']
collection_out.delete_many({})
collection_out.insert_many(cnv_37)


##########################################
##### Read in lifted file of grch38 ######
##########################################
segments_38 = {}
with open('/Users/bogao/DataFiles/tmp/segments_38.txt', 'r') as fi:
    next(fi)
    for line in fi:
        line = line.strip().split('\t')
        segments_38[line[0]] = line[1:]
        

# Generate new cnv collections by copying orignals and updating coordinates
cnv_38 = []
for sample in collection_in.find({},{'_id':0}):
    key = sample['id']
    if key in segments_38:
        sample['start'] = int(segments_38[key][1])
        sample['end'] = int(segments_38[key][2])
        sample['variantset_id'] = 'AM_VS_GRCH38'
        cnv_38.append(sample)

# write to db
collection_out = MongoClient()['arraymap_ga4gh']['variants']
collection_out.delete_many({})
collection_out.insert_many(cnv_38)

# Generate new callsets
collection_in = MongoClient()['arraymap_ga4gh']['callsets_cnv_grch36']
callsets_37 = []
callsets_38 = []
for sample in collection_in.find({},{'_id':0}):
    sample['variantset_id'] = 'AM_VS_GRCH37'
    callsets_37.append(sample)
# Write to db
collection_out = MongoClient()['arraymap_ga4gh']['callsets_cnv_grch37']
collection_out.delete_many({})
collection_out.insert_many(callsets_37)    
    
    
for sample in collection_in.find({},{'_id':0}):
    sample['variantset_id'] = 'AM_VS_GRCH38'
    callsets_38.append(sample)
collection_out = MongoClient()['arraymap_ga4gh']['callsets']
collection_out.delete_many({})
collection_out.insert_many(callsets_38)

# Generate new variant sets
variant_sets = [{'id': 'AM_VS_GRCH37',
        'dataset_id': 'arraymap',
        'reference_set_id:': 'GRCh37',
        },
                {'id': 'AM_VS_GRCH38',
        'dataset_id': 'arraymap',
        'reference_set_id:': 'GRCh38',
        },      
                {'id': 'AM_VS_GRCH36',
        'dataset_id': 'arraymap',
        'reference_set_id:': 'GRCh36',
        }]
collection_out = MongoClient()['arraymap_ga4gh']['variantsets']
collection_out.delete_many({})
collection_out.insert_many(variant_sets)