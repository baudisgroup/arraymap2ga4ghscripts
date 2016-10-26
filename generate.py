from pymongo import MongoClient
import re
# import argparse, sys, json

client = MongoClient()
db = client.arraymap
samples = db.samples
variants = {}
sampleno = -1

i = 1
varid = 1
callno = 0

for sample in samples.find({}, {'UID': 1, 'BIOSAMPLEID': 1, 'SEGMENTS_HG18': 1}):
    if ('SEGMENTS_HG18' in sample) and (sample['SEGMENTS_HG18'] is not None) and (len(sample['SEGMENTS_HG18']) > 1):
        callset_id = sample['UID']
        biosample_id = sample['BIOSAMPLEID']

        # check if BIOSAMPLEID has string & use this as 'biosample_id';
        # if not, create biosample_id as 'AM_BS__' + callset_id
        matchObj = re.search('^\w+.$', sample['BIOSAMPLEID'])
        if not matchObj:
            biosample_id = 'AM_BS__'+callset_id

        for seg in sample['SEGMENTS_HG18']:
            alternate_bases = ''
            start = int()
            end = int()
            varvalue = float()
            if seg['SEGTYPE'] < 0:
                alternate_bases = 'DEL'
            elif seg['SEGTYPE'] > 0:
                alternate_bases = 'DUP'

            try:
                start = int(float(seg['SEGSTART']))
            except TypeError:
                print 'TypeError: '+str(callset_id)+' - SEGSTART'
                continue

            try:
                end = int(float(seg['SEGSTOP']))
            except TypeError:
                print 'TypeError: '+str(callset_id)+' - SEGSTOP'
                continue

            tag = str(seg['CHRO'])+'_'+str(seg['SEGSTART'])+'_'+str(seg['SEGSTOP'])+'_'+alternate_bases
            call = {'call_set_id': str(sample['UID']), 'biosample_id': str(biosample_id)}

            try:
                varvalue = float(seg['SEGVALUE'])
            except ValueError:
                print 'ValueError: '+str(callset_id)+' - VALUE'
            else:
                call['VALUE'] = float(seg['SEGVALUE'])

            if tag in variants:
                variants[tag]['CALLS'].append(call)
                callno += 1
            else:
                variants[tag] = { 'id': str(varid), 'start': start, 'end': end, 'reference_name': str(seg['CHRO']), 'alternate_bases': str(alternate_bases), 'CALLS':[call]}
                varid += 1
                callno += 1

        matchObj = re.search('000$', str(i))
        if matchObj:
            print i

        i += 1

        if sampleno > 0:
            if i > sampleno:
                varid -= 1
                print str(varid)+' variants were created'
                break

db_variants = db.myvariants
db_variants.remove()
for k,v in variants.items():
	insert_id = db_variants.insert(v)

print str(callno)+' calls were found for '+str(varid)+' variants'

# with open('variants.json', 'w') as outfile:
#     json.dump(variants, outfile, indent=4, sort_keys=True, separators=(',', ':'))