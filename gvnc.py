from pymongo import MongoClient
import re
import click
import sys

# import argparse, sys, json


@click.command()
@click.option('-db', '--dbname', default='arraymap', help='The name of the database, default is arraymap')
@click.option('-src', '--collection_src', default='samples', help='The collection to read from, default is samples')
@click.option('-dst', '--collection_dst', default='myvariants', help='The collection to write into, default is myvariants')
@click.option('--demo', default=0, type=click.IntRange(0, 10000), help='Only process a limited number of entries')
@click.option('--dnw', is_flag=True, help='Do Not Write to the db')
@click.option('--log',  type=click.File('w'), help='Output errors and warnings to a log file')
def cli(dbname, collection_src, collection_dst, demo, dnw, log):
    """
    This script checks through the given source db collection,
    generate new VARIANTs containing CALLs, and put them in the
    destination db collection.

    NOTE: Please manually create the destination collection to
    avoid accidental overwritting of existing collection.

    WARNING: This script will overwrite the destination db
    collection, check before running.

    Example: python3 gvnc.py --demo 1000 --dnw --log log.txt
    This will run for 1000 valid samples, the result wont be
    written to the database, and any error or warning messages
    will be shown in log.txt.
    """
    ######################
    # init the db handler
    ######################
    client = MongoClient()
    if dbname not in client.database_names():
        print(dbname + ' does not exist')
        sys.exit()
    db = client[dbname]
    if collection_src not in db.collection_names():
        print(collection_src +' does not exist')
        sys.exit()
    if collection_dst not in db.collection_names():
        print(collection_dst +' does not exist')
        sys.exit()
    samples = db[collection_src]

    # varaint data is stored here
    variants = {}
    # counter for demo mode
    sampleno = 1

    ######################
    # statistic counters
    ######################
    no_samples = 0
    no_validSamples = 0
    no_segments = 0
    no_uniqueSegments = 0
    no_variantOneSeg = 0
    no_variantMultSeg = 0

    i = 1
    varid = 1
    callno = 0

    ######################
    # return how many samples are processed.
    ######################
    def show_counter(item):
        if item is not None:
            return str(no_samples+1)+' samples processed'


    # draw the processing bar
    click.echo()
    with click.progressbar(samples.find({}, {'UID': 1, 'BIOSAMPLEID': 1, 'SEGMENTS_HG18': 1}), label='Processing',
                           fill_char=click.style('*', fg='green'), item_show_func=show_counter) as bar:

        ######################
        #scan every sample
        ######################
        for sample in bar:
            no_samples += 1

            # log the processed number
            if log is not None:
                matchObj = re.search('000$', str(i))
                if matchObj:
                    click.echo(i, file=log)

            # only simples with none-empty SEGMENTS_HG18 is valid and worthy checking
            if ('SEGMENTS_HG18' in sample) and (sample['SEGMENTS_HG18'] is not None) and (len(sample['SEGMENTS_HG18']) > 1):
                no_validSamples += 1
                callset_id = sample['UID']
                biosample_id = sample['BIOSAMPLEID']

                # check if BIOSAMPLEID has string & use this as 'biosample_id';
                # if not, create biosample_id as 'AM_BS__' + callset_id
                matchObj = re.search('^\w+.$', sample['BIOSAMPLEID'])
                if not matchObj:
                    biosample_id = 'AM_BS__'+callset_id

                ######################
                #scan every segment
                ######################
                for seg in sample['SEGMENTS_HG18']:
                    no_segments += 1
                    alternate_bases = ''
                    start = int()
                    end = int()
                    varvalue = float()

                    if int(seg['SEGTYPE']) < 0:
                        alternate_bases = 'DEL'
                    elif int(seg['SEGTYPE']) > 0:
                        alternate_bases = 'DUP'

                    if log is not None:
                        # uncomment is if want to find all SEGTYPEs that are not int
                        # if type(seg['SEGTYPE']) is not int:
                        #     click.echo('TpyeWarning: '+str(callset_id)+' SEGTYPE is not INT', file=log)
                        try:
                            start = int(float(seg['SEGSTART']))
                        except TypeError:
                            click.echo('TypeError: '+str(callset_id)+' - SEGSTART', file=log)
                            continue

                        try:
                            end = int(float(seg['SEGSTOP']))
                        except TypeError:
                            click.echo('TypeError: '+str(callset_id)+' - SEGSTOP', file=log)
                            continue

                        try:
                            varvalue = float(seg['SEGVALUE'])
                        except ValueError:
                            click.echo('ValueError: '+str(callset_id)+' - VALUE', file=log)
                            continue
                    #create a tag for each segment
                    tag = str(seg['CHRO'])+'_'+str(seg['SEGSTART'])+'_'+str(seg['SEGSTOP'])+'_'+alternate_bases
                    call = {'call_set_id': str(sample['UID']), 'biosample_id': str(biosample_id), 'value': float(seg['SEGVALUE'])}

                    if tag in variants:
                    	#exists same tag, append the segment
                        variants[tag]['CALLS'].append(call)
                        callno += 1
                    else:
                    	#new tag, create new variant
                        variants[tag] = {'id': str(varid), 'start': start, 'end': end, 'reference_name': str(
                            seg['CHRO']), 'alternate_bases': str(alternate_bases), 'CALLS': [call]}
                        varid += 1
                        callno += 1
                        no_uniqueSegments += 1

                i += 1

                # Demo mode
                if demo > 0:
                    if sampleno < demo:
                        sampleno += 1
                    else:
                        break

    ######################
    # display statistics
    ######################
    click.echo()
    for k, var in variants.items():
        numcall = len(var['CALLS'])
        if numcall > 1:
            no_variantMultSeg += 1
        else:
            no_variantOneSeg += 1    
    click.echo('*'*60)
    click.echo(str(no_samples) + '\t samples processed from db: ' + dbname + '.' + collection_src)
    click.echo(str(no_validSamples) + '\t valid samples found')
    click.echo(str(no_segments) + '\t segments(calls) found')
    click.echo(str(no_variantOneSeg) + '\t segments are along ('+ str(no_variantOneSeg) +
               '/'+str(no_segments)+' = ' + str(round(no_variantOneSeg/no_segments*100,2)) +'%)')
    click.echo(str(no_segments-no_variantOneSeg) + '\t segments have companies ('+ str(no_segments-no_variantOneSeg) +
               '/'+str(no_segments)+' = ' + str(round((no_segments-no_variantOneSeg)/no_segments*100,2)) +'%)')
    click.echo(str(no_uniqueSegments) + '\t variants created')
    click.echo(str(no_variantOneSeg) + '\t variants have a single call ('+ str(no_variantOneSeg) +
               '/'+str(no_uniqueSegments)+' = ' + str(round(no_variantOneSeg/no_uniqueSegments*100,2)) +'%)')
    click.echo(str(no_variantMultSeg) + '\t variants have multiple calls (' + str(no_variantMultSeg) +
               '/'+str(no_uniqueSegments)+' = ' + str(round(no_variantMultSeg/no_uniqueSegments*100,2)) +'%)')
    click.echo('*'*60)
    click.echo()

    ######################
    # write to the db
    ######################
    if not dnw:
        db_variants = db[collection_dst]
        db_variants.remove()
        with click.progressbar(variants.items(), label='Writing Database',
                               fill_char=click.style('>', fg='green')) as bar:
            for k, v in bar:
                insert_id = db_variants.insert(v)
    click.echo()


#main
if __name__ == '__main__':
    cli()


