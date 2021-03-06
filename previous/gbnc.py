from pymongo import MongoClient
import re
import click
import sys
import datetime
import string
from bson import ObjectId


@click.command()
@click.option('-db', '--dbname', default='arraymap', help='The name of the database, default is "arraymap"')
@click.option('-src', '--collection_src', default='samples', help='The collection to read from, default is "samples"')
@click.option('-dsti', '--collection_dst_individuals', default='individuals', help='The collection to write into, default is "individuals"')
@click.option('-dstb', '--collection_dst_biosamples', default='biosamples', help='The collection to write into, default is "biosamples"')
@click.option('-dstc', '--collection_dst_callsets', default='callsets', help='The collection to write into, default is "callsets"')
@click.option('-d', '--demo', default=0, type=click.IntRange(0, 10000), help='Only process a limited number of entries')
@click.option('--dnw', is_flag=True, help='Do Not Write to the db')
@click.option('-l', '--log',  type=click.File('w'), help='Output errors and warnings to a log file')
@click.option('-s', '--status',  default='-exclude', help='Filter by STATUS value, param must be in format [+/-]keyword. "+" means to include, "-" means to exclude.')
def cli(dbname, collection_src, collection_dst_individuals, collection_dst_biosamples, collection_dst_callsets, demo, dnw, log, status):
    """
    This script checks through the given source db collection,
    generate new INDIVIDUALS, BIOSAMPLEs and CALLSETs, and put them in the
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
    if collection_dst_individuals not in db.collection_names():
        print(collection_dst_individuals + ' does not exist')
        print('You have to create it first with \"mongo ' + dbname +
              ' --eval \'db.createCollection("'+collection_dst_individuals+'")\'\"')
        sys.exit()
    if collection_dst_biosamples not in db.collection_names():
        print(collection_dst_biosamples + ' does not exist')
        print('You have to create it first with \"mongo ' + dbname +
              ' --eval \'db.createCollection("'+collection_dst_biosamples+'")\'\"')
        sys.exit()
    if collection_dst_callsets not in db.collection_names():
        print(collection_dst_callsets + ' does not exist')
        print('You have to create it first with \"mongo ' + dbname +
              ' --eval \'db.createCollection("'+collection_dst_callsets+'")\'\"')
        sys.exit()

    samples = db[collection_src]

    ######################
    # Validate --status input
    ######################
    try:
        status_op = status[0]
    except IndexError:
        click.echo('The length of --status paramter is invalid.')
        sys.exit()
    click.echo()
    if status_op is '+':
        qcondition = {'STATUS': re.compile(status[1:])}
        click.echo(str(samples.find(qcondition).count()) +
                   ' samples will be processed. (With STATUS containing: ' + status[1:] + ')')
    elif status_op is '-':
        qcondition = {'STATUS': {'$not': re.compile(status[1:])}}
        click.echo(str(samples.find(qcondition).count()) +
                   ' samples will be processed. (With STATUS NOT containing: ' + status[1:] + ')')
    else:
        click.echo('The first char of --status must be "+" or "-".')
        sys.exit()

    # INDIVIDUAL, BIOSAMPLE and CALLSET data are stored here
    individuals = {}
    biosamples = {}
    callsets = {}
    variantset_id = 'AM_VS_HG18'
    # counter for demo mode
    sampleno = 1

    ######################
    # statistic counters
    ######################
    no_samples = 0
    no_validSamples = 0
    no_individuals =0
    no_biosamples = 0
    no_callsets = 0

    ########################################
    # return how many samples are processed.
    ########################################
    def show_counter(item):
        if item is not None:
            return str(no_samples+1)+' samples processed'

    ########################################
    # return an attribute of a sample.
    ########################################
    def get_attribute(name, sample):
        try:
            val = sample[name]
        except KeyError:
            val = 'null'
            if log is not None:
                click.echo('KeyError: '+str(sample['_id'])+' has no '+name, file=log)
        return val

    # draw the processing bar
    click.echo()
    with click.progressbar(samples.find(qcondition), label='Processing',
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

            # generate ids
            callset_id = 'AM_CS_'+sample['UID']
            biosample_id = 'AM_BS_'+sample['UID']
            individual_id = 'PGIND_'+sample['UID']

            ############################################
            # check and generate biosamples and callsets
            ############################################
            #only samples with enough attributes are assumed to be valid, the threshold is set to 50 arbitrarily.
            if (len(sample) > 50):
                no_validSamples += 1





                # generate an individual
                if individual_id in individuals:
                    # if same individual_id exists, report an error
                    if log is not None:
                        click.echo('Duplicate individual_id: '+individual_id, file=log)
                else:
                    species = {'id': 'http://purl.obolibrary.org/obo/NCBITaxon_9606',
                                'term': 'Homo sapiens',
                                'source_name': 'NCBITaxon',
                                'source_version': 'null'}
                    if get_attribute('SEX', sample) is 'male':
                        sex_id = 'http://purl.obolibrary.org/obo/PATO_0020001'
                    elif get_attribute('SEX', sample) is 'female':
                        sex_id = 'http://purl.obolibrary.org/obo/PATO_0020002'
                    else:
                        sex_id = 'null'
                    sex = {'id': sex_id, 'term': 'genotypic sex', 'source_name': 'PATO',
                            'source_version':'http://purl.obolibrary.org/obo/pato/releases/2016-05-22/pato.owl'}

                    individuals[individual_id] = {'id': individual_id, 'name': 'null', 'description': get_attribute('DIAGNOSISTEXT', sample),
                                                    'characteristics': 'null', 'created': datetime.datetime.utcnow(), 'updated': datetime.datetime.utcnow(),
                                                    'species': species, 'sex': sex, 'redirected_to': 'null'}
                    no_individuals +=1





                # generate a biosample
                if biosample_id in biosamples:
                    # if same biosample_id exists, report an error
                    if log is not None:
                        click.echo('Duplicate biosample_id: '+biosample_id, file=log)
                else:
                    # new biosample_id, create new biosample
                    # TODO: ISO age
                    icdmcode = get_attribute('ICDMORPHOLOGYCODE', sample)
                    icdmcode_termid = 'ICDOM:'+re.sub('/', '_', icdmcode)
                    ncitcode = get_attribute('NCIT:CODE', sample)
                    ncitcode_termid = 'NCIT:'+ncitcode
                    snomedcode_termid = 'SNMI:M-'+re.sub('/', '', icdmcode)
                    country = string.capwords(get_attribute('COUNTRY', sample))
                    country = re.sub('USA', 'United States', country, flags=re.IGNORECASE)
                    biosamples[biosample_id] = {
                                                'created': datetime.datetime.utcnow(),
                                                'updated': datetime.datetime.utcnow(),
                                                'individual_id': individual_id,
                                                'id': biosample_id,
                                                'name': biosample_id,
                                                'description': get_attribute('DIAGNOSISTEXT', sample),
                                                'info': {
                                                    'pubmed_id': get_attribute('PMID', sample),
                                                    # 'icdo3_morphology': get_attribute('ICDMORPHOLOGY', sample),
                                                    # 'icdo3_morphology_code': icdmcode,
                                                    # 'ncit': get_attribute('NCIT:TERM', sample),
                                                    # 'ncit_code': ncitcode,
                                                    # 'icdo3_topography': get_attribute('ICDTOPOGRAPHY', sample),
                                                    # 'icdo3_topography_code': get_attribute('ICDTOPOGRAPHYCODE', sample),
                                                    'tnm': get_attribute('TNM', sample),
                                                    'age': float(get_attribute('AGE', sample)),
                                                    'city': get_attribute('CITY', sample),
                                                    'country': country,
                                                    'geo_lat': float(get_attribute('GEOLAT', sample)),
                                                    'geo_long': float(get_attribute('GEOLONG', sample)),
                                                    'sex': get_attribute('SEX', sample),
                                                    'death': get_attribute('DEATH', sample),
                                                    'followup_months': float(get_attribute('FOLLOWUP', sample)),
                                                    'redirected_to': 'null'},
                                                'characteristics': {
                                                    'diseases': [
                                                        {
                                                            'description': get_attribute('DIAGNOSISTEXT', sample),
                                                            'ontologyTerms': [
                                                                {
                                                                    'termId': ncitcode_termid,
                                                                    'termLabel': get_attribute('NCIT:TERM', sample)
                                                                },
                                                                {
                                                                    'termId': snomedcode_termid,
                                                                    'termLabel': get_attribute('ICDMORPHOLOGY', sample)
                                                                },
                                                                {
                                                                    'termId': icdmcode_termid,
                                                                    'termLabel': get_attribute('ICDMORPHOLOGY', sample)
                                                                },
                                                                {
                                                                    'termId': 'ICDOT:'+str(get_attribute('ICDTOPOGRAPHYCODE', sample)),
                                                                    'termLabel': get_attribute('ICDTOPOGRAPHY', sample)
                                                                }
                                                            ],
                                                            'negatedOntologyTerms': []
                                                        }
                                                    ],
                                                    'phenotypes': []
                                                }
                    }
                    no_biosamples += 1






                # check and generate callset
                if callset_id in callsets:
                    # if same callset_id exists, report an error
                    if log is not None:
                        click.echo('Duplicate callset_id:'+callset_id, file=log)
                else:
                    callsets[callset_id] = {'id': callset_id,  'biosample_id': biosample_id, 'variant_set_id': variantset_id,
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
        click.echo('New data will overwrite collections: '+ collection_dst_individuals +', '+collection_dst_biosamples + ' and ' + collection_dst_callsets + '.')
        while True:
            msg = input('Do you want to proceed? Please type y/n: ')
            if msg is 'n':
                print('Terminating: Data is not written into the db. \n')
                sys.exit()
            elif msg is 'y':

                # writing db
                db_individuals = db[collection_dst_individuals]
                db_individuals.remove()
                with click.progressbar(individuals.items(), label='Writing Database ' + collection_dst_individuals + ':\t',
                                       fill_char=click.style('>', fg='green')) as bar:
                    for k, v in bar:
                        insert_id = db_individuals.insert(v)

                db_biosamples = db[collection_dst_biosamples]
                db_biosamples.remove()
                with click.progressbar(biosamples.items(), label='Writing Database ' + collection_dst_biosamples + ':\t',
                                       fill_char=click.style('>', fg='green')) as bar:
                    for k, v in bar:
                        insert_id = db_biosamples.insert(v)

                db_callsets = db[collection_dst_callsets]
                db_callsets.remove()
                with click.progressbar(callsets.items(), label='Writing Database ' + collection_dst_callsets + ':\t',
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
