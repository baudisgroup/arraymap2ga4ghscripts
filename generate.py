from pymongo import MongoClient
import re
import datetime
# import argparse, sys, json

client = MongoClient()
db = client.arraymap
samples = db.samples
variants = {}
sampleno = -1

if sampleno > 0:
    print 'Test run - only '+str(sampleno)+' samples will be processed!'

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

            typevalue = int()
            alternate_bases = ''
            start = int()
            end = int()
            varvalue = float()

            try:
                typevalue = int(seg['SEGTYPE'])
                if seg['SEGTYPE'] < 0:
                    alternate_bases = 'DEL'
                elif seg['SEGTYPE'] > 0:
                    alternate_bases = 'DUP'
            except TypeError:
                print 'TypeError: '+str(callset_id)+' - SEGTYPE; skipping'
                continue

            try:
                start = int(float(seg['SEGSTART']))
            except TypeError:
                print 'TypeError: '+str(callset_id)+' - SEGSTART; skipping'
                continue

            try:
                end = int(float(seg['SEGSTOP']))
            except TypeError:
                print 'TypeError: '+str(callset_id)+' - SEGSTOP; skipping'
                continue

            tag = str(seg['CHRO'])+'_'+str(seg['SEGSTART'])+'_'+str(seg['SEGSTOP'])+'_'+alternate_bases
            call = { 'call_set_id': str(sample['UID']), 'biosample_id': str(biosample_id), 'genotype': ['.', '.'] }

            try:
                varvalue = float(seg['SEGVALUE'])
            except ValueError:
                print 'ValueError: '+str(callset_id)+' - VALUE; continuing'
            else:
                call['VALUE'] = float(seg['SEGVALUE'])

            if tag in variants:
                variants[tag]['updated'] = datetime.datetime.utcnow()
                variants[tag]['CALLS'].append(call)
                callno += 1
            else:
                variants[tag] = { 'id': str(varid), 'start': start, 'end': end, 'reference_name': str(seg['CHRO']), 'created': datetime.datetime.utcnow(), 'updated': datetime.datetime.utcnow(), 'reference_bases': '.', 'alternate_bases': str(alternate_bases), 'CALLS':[call]}
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

print str(callno)+' calls were found for '+str(varid)+' variants'

i = 0

db_variants = db.variants
db_variants.remove()
for k,v in variants.items():
    insert_id = db_variants.insert(v)
    i += 1
    matchObj = re.search('00000$', str(i))
    if matchObj:
        print i

print str(i)+' variants were loaded into the variants collection.'

# with open('variants.json', 'w') as outfile:
#     json.dump(variants, outfile, indent=4, sort_keys=True, separators=(',', ':'))
