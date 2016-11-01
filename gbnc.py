from pymongo import MongoClient
import re
import click
import sys
import datetime


@click.command()
@click.option('-db', '--dbname', default='arraymap', help='The name of the database, default is "arraymap"')
@click.option('-src', '--collection_src', default='samples', help='The collection to read from, default is "samples"')
@click.option('-dstb', '--collection_dst_biosamples', default='biosamples', help='The collection to write into, default is "biosamples"')
@click.option('-dstc', '--collection_dst_callsets', default='callsets', help='The collection to write into, default is "callsets"')
@click.option('--demo', default=0, type=click.IntRange(0, 10000), help='Only process a limited number of entries')
@click.option('--dnw', is_flag=True, help='Do Not Write to the db')
@click.option('--log',  type=click.File('w'), help='Output errors and warnings to a log file')
def cli(dbname, collection_src, collection_dst_biosamples, collection_dst_callsets, demo, dnw, log):
    """
    This script checks through the given source db collection,
    generate new BIOSAMPLEs and CALLSETs, and put them in the
    destination db collection.

    NOTE: Please manually create the destination collection to
    avoid accidental overwritting of existing collection.

    WARNING: This script will overwrite the destination db
    collection, check before running.

    Example: python3 gbnc.py --demo 1000 --dnw --log log.txt
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
        print(collection_src + ' does not exist')
        sys.exit()
    if collection_dst_biosamples not in db.collection_names():
        print(collection_dst_biosamples + ' does not exist')
        sys.exit()
    if collection_dst_callsets not in db.collection_names():
        print(collection_dst_callsets + ' does not exist')
        sys.exit()
    samples = db[collection_src]

    # BIOSAMPLE and CALLSET data are stored here
    biosamples = {}
    callsets = {}
    # counter for demo mode
    sampleno = 1

    ######################
    # statistic counters
    ######################
    no_samples = 0
    no_validSamples = 0
    no_biosamples = 0
    no_callsets = 0

    ######################
    # return how many samples are processed.
    ######################
    def show_counter(item):
        if item is not None:
            return str(no_samples+1)+' samples processed'

    def get_attribute(name,sample):
        try:
            val = sample[name]
        except KeyError:
            val = ''
            if log is not None:
                click.echo('KeyError: '+str(sample['_id'])+' has no '+name, file=log)
        return val


    # draw the processing bar
    click.echo()
    with click.progressbar(samples.find(), label='Processing',
                           fill_char=click.style('*', fg='green'), item_show_func=show_counter) as bar:

        ######################
        # scan every sample
        ######################
        for sample in bar:
            no_samples += 1

            # log the processed number
            if log is not None:
                matchObj = re.search('000$', str(no_samples))
                if matchObj:
                    click.echo(no_samples, file=log)

            # only samples with none-empty SEGMENTS_HG18 are valid and worthy checking
            if ('SEGMENTS_HG18' in sample) and (sample['SEGMENTS_HG18'] is not None) and (len(sample['SEGMENTS_HG18']) > 1):
                no_validSamples += 1
                callset_name = sample['UID']
                biosample_name = sample['BIOSAMPLEID']


                # check if BIOSAMPLEID has string & use this as 'biosample_name';
                # if not, create biosample_name as 'AM_BS__' + callset_name
                matchObj = re.search('^\w+.$', sample['BIOSAMPLEID'])
                if not matchObj:
                    biosample_name = 'AM_BS__'+callset_name


                ############################################
                #check and generate biosamples and callsets
                ############################################

                # check and generate biosample
                if biosample_name in biosamples:
                    # if same biosample_name exists, report an error
                    if log is not None:
                        click.echo('Duplicate biosample_name: '+biosample_name, file=log)
                else:
                    # new biosample_name, create new biosample
                    biosamples[biosample_name] = {'characteristics': '', 'created': datetime.datetime.utcnow(), 'updated': datetime.datetime.utcnow(), 'individual_id': '', 
                        'name': biosample_name, 
                        'description': get_attribute('DIAGNOSISTEXT',sample), 
                        'info': {'pubmed_id': get_attribute('PMID',sample), 
                            'icdo3_morphology': get_attribute('ICDMORPHOLOGY',sample), 
                            'icdo3_morphology_code': get_attribute('ICDMORPHOLOGYCODE',sample),
                            'icdo3_topography': get_attribute('ICDTOPOGRAPHY', sample),
                            'icdo3_topography_code': get_attribute('ICDTOPOGRAPHYCODE', sample),
                            'age': get_attribute('AGE', sample),
                            'city': get_attribute('CITY', sample),
                            'county': get_attribute('COUNTRY', sample),
                            'death': get_attribute('DEATH',sample),
                            'geo_lat': get_attribute('GEOLAT', sample),
                            'get_long': get_attribute('GEOLONG', sample),
                            'sex': get_attribute('SEX', sample),
                            'redirected_to': ''}}
                    no_biosamples += 1

                # check and generate callset
                if callset_name in callsets:
                    # if same callset_name exists, report an error
                    if log is not None:
                        click.echo('Duplicate callset_name:'+callset_name, file=log)
                else:
                    callsets[callset_name] = {'name': callset_name,  'bio_sample_name': biosample_name,
                                            'created': datetime.datetime.utcnow(), 'updated': datetime.datetime.utcnow()}
                    no_callsets += 1

                ######################
                # Demo mode
                ######################
                if demo > 0:
                    if sampleno < demo:
                        sampleno += 1
                    else:
                        break

    ######################
    # display statistics
    ######################
    click.echo()
    click.echo('*'*60)
    click.echo(str(no_samples) + '\t samples processed from db: ' + dbname + '.' + collection_src)
    click.echo(str(no_validSamples) + '\t valid samples found')
    click.echo(str(no_biosamples) + '\t biosamples created')
    click.echo(str(no_callsets) + '\t callsets created')
    click.echo('*'*60)
    click.echo()

    ######################
    # write to the db
    ######################
    if not dnw:

        # Commond line prompt to confirm the overwriting of db
        click.echo('New data will overwrite collections: '+collection_dst_biosamples + ' and ' + collection_dst_callsets +'.')
        while True:
            msg = input('Do you want to proceed? Please type y/n: ')
            if msg is 'n':
                print('Terminating: Data is not written into the db. \n')
                sys.exit()
            elif msg is 'y':

                # writing db
                db_biosamples = db[collection_dst_biosamples]
                db_biosamples.remove()
                with click.progressbar(biosamples.items(), label='Writing Database:' + collection_dst_biosamples,
                                       fill_char=click.style('>', fg='green')) as bar:
                    for k, v in bar:
                        insert_id = db_biosamples.insert(v)
                
                db_callsets = db[collection_dst_callsets]
                db_callsets.remove()
                with click.progressbar(callsets.items(), label='Writing Database:' + collection_dst_callsets,
                                       fill_char=click.style('>', fg='green')) as bar:
                    for k, v in bar:
                        insert_id = db_callsets.insert(v)
                break
            else:
                print('invalid input')
        click.echo()


# main
if __name__ == '__main__':
    cli()
