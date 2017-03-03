from pymongo import MongoClient
import re
import click
import sys
import datetime
import string
from bson import ObjectId


@click.command()
@click.option('-dbin', '--input_db', default='arraymap', help='The name of the input database, default is "arraymap"')
@click.option('-cin', '--input_collection', default='samples', help='The input collection, default is "samples"')
@click.option('-dbout', '--output_db', default='arraymap_ga4gh', help='The name of the output database, default is "arraymap_ga4gh"')
@click.option('-couti', '--output_collection_individuals', default='individuals', help='The output collection of individuals, default is "individuals"')
@click.option('-coutb', '--output_collection_biosamples', default='biosamples', help='The output collection of biosamples, default is "biosamples"')
@click.option('-coutc', '--output_collection_callsets', default='callsets', help='The output collection of callsets, default is "callsets"')
@click.option('-coutv', '--output_collection_variants', default='variants', help='The output collection of variants, default is "variants"')
@click.option('-d', '--demo', default=0, type=click.IntRange(0, 10000), help='Only to process a limited number of entries')
@click.option('--dnw', is_flag=True, help='Do Not Write to the db')
@click.option('--dna', is_flag=True, help='Do Not Ask before overwriting the database')
@click.option('-l', '--log',  type=click.File('w'), help='Output errors and warnings to a log file')
@click.option('-f', '--dbfilter', default="{'STATUS': {'$regex': '^[^e]'}}", help='The filter for the data to process, should be in mongodb syntax')
@click.option('-ff', '--file_dbfilter', type=click.File('r'), help="Read in filter from the first line of a file")
def cli(input_db, input_collection, output_db, output_collection_individuals, output_collection_biosamples, output_collection_callsets, output_collection_variants,
    demo, dnw, dna, log, dbfilter, file_dbfilter):



    """
    This script checks through the given input database collection,
    generate new collections of :
        INDIVIDUALs, BIOSAMPLEs, CALLSETs and VARIANTs.
    And save into the output database.

    WARNING: This script will overwrite the ouput collections,
            check before running.

    Example: python3 arraymap2ga4gh.py --demo 1000 --dnw --log log.txt
    With these options, the script will check through 1000 samples without
    committing the result to the database.
    Any error or warning messages will be shown in log.txt.
    """




    ######################
    # init the db handler
    ######################
    client = MongoClient()
    if input_db not in client.database_names():
        print(input_db + ' does not exist')
        sys.exit()
    db = client[input_db]

    if input_collection not in db.collection_names():
        print(input_collection + ' does not exist')
        sys.exit()
    samples = db[input_collection]



    # INDIVIDUAL, BIOSAMPLE, CALLSET and VARIANT data are stored here
    individuals = {}
    biosamples = {}
    callsets = {}
    variants = {}
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

    no_sample_w_segments = 0
    no_segments = 0
    no_uniqueSegments = 0
    no_variantOneSeg = 0
    no_variantMultSeg = 0

    i = 1
    varid = 1
    callno = 0




    ########################################
    # return the number of processed samples
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






    # check if filter contains valid query
    # also get the data size.
    bar_length=0
    query = ''
    # filter from a file
    if file_dbfilter:
        # only use the first line
        line = file_dbfilter.readline()
        query = eval(line)
        try:
            bar_length = samples.find(query).count()
        except:
            print("Filter File Contains Invalid Query!")
            sys.exit()
    else:
        # filter as a param
        query = eval(dbfilter)
        try:
            bar_length = samples.find(query).count()
        except:
            print("Filter Contains Invalid Query!")
            sys.exit()


    if demo>0 :
        bar_length = demo




    ##########################
    # draw the processing bar
    ##########################
    click.echo()
    with click.progressbar(samples.find(query), label='Processing',
                           fill_char=click.style('*', fg='green'), length=bar_length, item_show_func=show_counter) as bar:

        ######################
        # scan every sample  #
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

            ###########################################################
            # check and generate INDIVIDUALS, BIOSAMPLES and CALLSETS #
            ###########################################################

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
                    try:
                        geolat = float(get_attribute('GEOLAT', sample))
                    except ValueError:
                        geolat = ""
                    except TypeError:
                        geolat = ""
                    try:
                        geolong = float(get_attribute('GEOLONG', sample))
                    except ValueError:
                        geolong = ""
                    except TypeError:
                        geolong = ""
                    try:
                        age = float(get_attribute('AGE', sample))
                    except ValueError:
                        geolong = ""
                    except TypeError:
                        geolong = ""
                    try:
                        followup = float(get_attribute('FOLLOWUP', sample))
                    except ValueError:
                        followup = ""
                    except TypeError:
                        followup = ""
                    biosamples[biosample_id] = {
                                                'id': biosample_id,
                                                'name': biosample_id,
                                                'description': get_attribute('DIAGNOSISTEXT', sample),
                                                'bio_characteristics': [
                                                    {
                                                        'description': get_attribute('DIAGNOSISTEXT', sample),
                                                        'ontology_terms': [
                                                            {
                                                                'term_id': ncitcode_termid,
                                                                'term_label': get_attribute('NCIT:TERM', sample)
                                                            },
                                                            {
                                                                'term_id': snomedcode_termid,
                                                                'term_label': get_attribute('ICDMORPHOLOGY', sample)
                                                            },
                                                            {
                                                                'term_id': icdmcode_termid,
                                                                'term_label': get_attribute('ICDMORPHOLOGY', sample)
                                                            },
                                                            {
                                                                'term_id': 'ICDOT:'+str(get_attribute('ICDTOPOGRAPHYCODE', sample)),
                                                                'term_label': get_attribute('ICDTOPOGRAPHY', sample)
                                                            }
                                                        ],
                                                        'negated_ontology_terms': []
                                                    }
                                                ],
                                                'created': datetime.datetime.utcnow(),
                                                'updated': datetime.datetime.utcnow(),
                                                'individual_id': individual_id,
                                                'external_identifiers': [
                                                    {
                                                        'database': 'Pubmed',
                                                        'identifier': get_attribute('PMID', sample),
                                                    },
                                                ],
                                                'attributes': {
                                                    'geo_lat': { 'values': [ {'sint32_value': geolat } ] },
                                                    'geo_long': { 'values': [ {'sint32_value': geolong } ] },
                                                    'tnm': { 'values': [ { 'string_value': get_attribute('TNM', sample) } ] },
                                                    'age':  { 'values': [ { 'double_value': age } ] },
                                                    'city': { 'values': [ { 'string_value': get_attribute('CITY', sample) } ] },
                                                    'country': { 'values': [ { 'string_value': country} ] },
                                                    'sex': { 'values': [ { 'string_value': get_attribute('SEX', sample) } ] },
                                                    'death': { 'values': [ { 'string_value': get_attribute('DEATH', sample) } ] },
                                                    'followup_months':  { 'values': [ { 'double_value': followup } ] },
                                                    'redirected_to': 'null'
                                                },
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







            ###############################
            # check and generate VARIANTS #
            ###############################

            # only simples with none-empty SEGMENTS_HG18 is valid and worthy checking
            if ('SEGMENTS_HG18' in sample) and (sample['SEGMENTS_HG18'] is not None) and (len(sample['SEGMENTS_HG18']) > 1):
                no_sample_w_segments += 1

                # Generate callset id
                callset_id = 'AM_CS_'+sample['UID']
                #biosample_id = sample['BIOSAMPLEID']

                # DEPRECATED: check if BIOSAMPLEID has string & use this as 'biosample_id';
                # if not, create biosample_id as 'AM_BS__' + callset_id
                # matchObj = re.search('^\w+.$', sample['BIOSAMPLEID'])
                # if not matchObj:
                #     biosample_id = 'AM_BS__'+callset_id

                ######################
                # scan every segment
                ######################
                for seg in sample['SEGMENTS_HG18']:
                    no_segments += 1
                    alternate_bases = ''
                    start = int()
                    end = int()
                    varvalue = float()

                    try:
                        typevalue = int(seg['SEGTYPE'])
                    except TypeError:
                        if log is not None:
                            click.echo('TpyeWarning: '+str(callset_id)+' SEGTYPE is not INT', file=log)
                        continue

                    if int(seg['SEGTYPE']) < 0:
                        alternate_bases = 'DEL'
                    elif int(seg['SEGTYPE']) > 0:
                        alternate_bases = 'DUP'
                    elif int(seg['SEGTYPE']) == 0:
                        if log is not None:
                            click.echo('TpyeWarning: '+str(callset_id)+' SEGTYPE is "0"', file=log)
                        continue

                    try:
                        start = int(float(seg['SEGSTART']))
                    except TypeError:
                        if log is not None:
                            click.echo('TypeError: '+str(callset_id)+' - SEGSTART', file=log)
                        continue

                    try:
                        end = int(float(seg['SEGSTOP']))
                    except TypeError:
                        if log is not None:
                            click.echo('TypeError: '+str(callset_id)+' - SEGSTOP', file=log)
                        continue

                    # create a tag for each segment
                    tag = str(seg['CHRO'])+'_'+str(seg['SEGSTART'])+'_'+str(seg['SEGSTOP'])+'_'+alternate_bases
                    call = {'call_set_id': callset_id, 'genotype': ['.', '.'], 'info': {}}
                    info = {}

                    try:
                        varvalue = float(seg['SEGVALUE'])
                    except ValueError:
                        if log is not None:
                            click.echo('ValueError: '+str(callset_id)+' - SEGVALUE', file=log)
                        # continue
                    else:
                        call['info']['segvalue'] = float(seg['SEGVALUE'])

                    if tag in variants:
                        # exists same tag, append the segment
                        variants[tag]['updated'] = datetime.datetime.utcnow()
                        variants[tag]['calls'].append(call)
                        callno += 1
                    else:
                        # new tag, create new variant
                        variants[tag] = {'id': 'AM_V_'+str(varid), 'start': start, 'end': end, 'info': info, 'variant_set_id': variantset_id, 'reference_name': str(
                            seg['CHRO']), 'created': datetime.datetime.utcnow(), 'updated': datetime.datetime.utcnow(), 'reference_bases': '.', 'alternate_bases': str(alternate_bases), 'calls': [call]}
                        varid += 1
                        callno += 1
                        no_uniqueSegments += 1

                i += 1










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
    click.echo(str(no_samples) + '\t samples processed from db: ' + input_db + '.' + input_collection)
    click.echo(str(no_validSamples) + '\t valid samples found')
    click.echo(str(no_biosamples) + '\t biosamples created')
    click.echo(str(no_callsets) + '\t callsets created')
    click.echo()


    click.echo()
    for k, var in variants.items():
        numcall = len(var['calls'])
        if numcall > 1:
            no_variantMultSeg += 1
        else:
            no_variantOneSeg += 1

    click.echo(str(no_sample_w_segments) + '\t samples have at least one segment')
    click.echo(str(no_segments) + '\t segments(calls) found')
    click.echo(str(no_variantOneSeg) + '\t segments are single events in a sample (' + str(no_variantOneSeg) +
               '/'+str(no_segments)+' = ' + str(round(no_variantOneSeg/no_segments*100, 2)) + '%)')
    click.echo(str(no_segments-no_variantOneSeg) + '\t segments have companies (' + str(no_segments-no_variantOneSeg) +
               '/'+str(no_segments)+' = ' + str(round((no_segments-no_variantOneSeg)/no_segments*100, 2)) + '%)')
    click.echo(str(no_uniqueSegments) + '\t variants created')
    click.echo(str(no_variantOneSeg) + '\t variants have a single call (' + str(no_variantOneSeg) +
               '/'+str(no_uniqueSegments)+' = ' + str(round(no_variantOneSeg/no_uniqueSegments*100, 2)) + '%)')
    click.echo(str(no_variantMultSeg) + '\t variants have multiple calls (' + str(no_variantMultSeg) +
               '/'+str(no_uniqueSegments)+' = ' + str(round(no_variantMultSeg/no_uniqueSegments*100, 2)) + '%)')
    click.echo('*'*60)
    click.echo()


    ######################
    # write to the db
    ######################
    if not dnw:

        # Commond line prompt to confirm the overwriting of db
        click.echo('New data will overwrite collections: ' + output_collection_individuals +', ' + output_collection_biosamples + ', ' + output_collection_callsets + ' and ' + output_collection_variants + '.')
        while True:
            if not dna:
                msg = input('Do you want to proceed? Please type y/n: ')
            else:
                msg ='y'

            if msg is 'n':
                print('Terminating: Data is not written into the db. \n')
                sys.exit()
            elif msg is 'y':

                # writing db
                db_out = client[output_db]
                db_individuals = db_out[output_collection_individuals]
                db_individuals.remove()
                with click.progressbar(individuals.items(), label='Writing Database ' + output_collection_individuals + ':\t',
                                       fill_char=click.style('>', fg='green')) as bar:
                    for k, v in bar:
                        insert_id = db_individuals.insert(v)

                db_biosamples = db_out[output_collection_biosamples]
                db_biosamples.remove()
                with click.progressbar(biosamples.items(), label='Writing Database ' + output_collection_biosamples + ':\t',
                                       fill_char=click.style('>', fg='green')) as bar:
                    for k, v in bar:
                        insert_id = db_biosamples.insert(v)

                db_callsets = db_out[output_collection_callsets]
                db_callsets.remove()
                with click.progressbar(callsets.items(), label='Writing Database ' + output_collection_callsets + ':\t',
                                       fill_char=click.style('>', fg='green')) as bar:
                    for k, v in bar:
                        insert_id = db_callsets.insert(v)

                db_variants = db_out[output_collection_variants]
                db_variants.remove()
                with click.progressbar(variants.items(), label='Writing Database' + output_collection_variants + ':\t',
                                       fill_char=click.style('>', fg='green')) as bar:
                    for k, v in bar:
                        insert_id = db_variants.insert(v)

                break

            else:
                print('invalid input')
        click.echo()


# main
if __name__ == '__main__':
    cli()
