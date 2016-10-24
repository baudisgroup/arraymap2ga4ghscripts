from pymongo import MongoClient

client = MongoClient()
db = client.arraymap
samples = db.samples
variants = {}

i = 1

for sample in samples.find({}, {'UID': 1, 'SEGMENTS_HG18': 1}):

    if ('SEGMENTS_HG18' in sample) and (sample['SEGMENTS_HG18'] is not None) and (len(sample['SEGMENTS_HG18']) > 1):
        callset_id = sample['UID']
        for seg in sample['SEGMENTS_HG18']:
            tag = str(seg['CHRO'])+'_'+str(seg['SEGSTART'])+'_'+str(seg['SEGSTOP'])+'_'+str(seg['SEGTYPE'])
            # seg_type = int(seg['SEGTYPE'])
            # start = int(seg['SEGSTART'])
            # end = int(seg['SEGSTOP'])
            # value = seg['SEGVALUE']
            # chrom = int(seg['CHRO'])
            # arraymap_id = sample['_id']
            # call = {'type': seg_type, 'start': start, 'end': end, 'value': value,
            #         'arraymap_id': arraymap_id, 'callset_id': callset_id}
            call = {'CALLSET_ID': sample['UID'], 'SAMPLE_ID': sample['_id'], 'VALUE': seg['SEGVALUE']}
            #print(str(sample['_id']))
            if tag in variants:
                variants[tag]['CALLS'].append(call)
            else:
                variants[tag] = {'START': seg['SEGSTART'], 'STOP': seg[
                    'SEGSTOP'], 'CHRO': seg['CHRO'], 'ALT': seg['SEGTYPE'], 'CALLS':[call]}
        # i += 1
        # if i > 100:
        #     break

db_variants = db.myvariants
db_variants.remove()
for k,v in variants.items():
	insert_id = db_variants.insert(v)

