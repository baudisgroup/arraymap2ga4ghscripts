
## This version is for the new general schema: https://github.com/progenetix/schemas/tree/master/json
## 2018-09-11

from pymongo import MongoClient
import re
import click
import sys
import datetime


@click.command()
@click.option('-dbin', '--input_db', default='', help='The name of the input database, default is "arraymap"')
@click.option('-cin', '--input_collection', default='samples', help='The input collection, default is "samples"')
@click.option('-dbout', '--output_db', default='', help='The name of the output database, default is "arraymap"')
@click.option('-couti', '--output_collection_individuals', default='individuals', help='The output collection of individuals, default is "individuals"')
@click.option('-coutb', '--output_collection_biosamples', default='biosamples', help='The output collection of biosamples, default is "biosamples"')
@click.option('-coutc', '--output_collection_callsets', default='callsets', help='The output collection of callsets, default is "callsets"')
@click.option('-coutv', '--output_collection_variants', default='variants', help='The output collection of variants, default is "variants"')
@click.option('-d', '--demo', default=0, type=click.IntRange(0, 10000), help='Only to process a limited number of entries')
@click.option('--dnw', is_flag=True, help='Do Not Write to the db')
@click.option('--dna', is_flag=True, help='Do Not Ask before overwriting the database')
@click.option('-l', '--log',  type=click.File('w'), help='Output errors and warnings to a log file')
@click.option('-f', '--dbfilter', default="{'STATUS': {'$regex': '^[^e]|^$'}}", help='The filter for the data to process, should be in mongodb syntax')
@click.option('-ff', '--file_dbfilter', type=click.File('r'), help="Read in filter from the first line of a file")
def cli(input_db, input_collection, output_db, output_collection_individuals, output_collection_biosamples, output_collection_callsets,
    output_collection_variants, demo, dnw, dna, log, dbfilter, file_dbfilter):



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

    ############################################
    # Init input and output database names
    ############################################
    if input_db == '':
        input_db = 'arraymap'
    if output_db == '':
        output_db = input_db 


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
    variantset_id = 'AM_VS_GRCH38'

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

    ########################################
    # return the number of processed samples
    ########################################
    def show_counter(item):
        if item is not None:
            return str(no_samples+1)

    ########################################
    # return an attribute of a sample.
    # name: the attribute nanme
    # sample: the queried sample
    # returnType: (optional) convert the return value, support "str", "float', "int", default is no conversion.
    # nullValue: (optional) the return value when attribute doesn't exist nor of the desired type, default is "null"
    ########################################
    def get_attribute(name, sample, returnType='none', nullValue=None):

        try:
            val = sample[name]
        except KeyError:
            if log is not None:
                if 'UID' in sample:
                    click.echo('WARNING: '+str(sample['UID'])+' has no '+name, file=log)
                else:
                    click.echo('WARNING: {} does not exist. '.format(name), file=log)
            return nullValue

        if returnType == 'str':
            try:
                return str(val)
            except (ValueError, TypeError) as e:
                if log is not None:
                    click.echo('WARNING:' + str(sample['UID']) + '  '+ name, file=log)
                return nullValue
        elif returnType == 'float':
            try:
                return float(val)
            except (ValueError, TypeError) as e:
                if log is not None:
                    click.echo('WARNING:' + str(sample['UID']) + '  '+ name, file=log)
                return nullValue
        elif returnType == 'int':
            try:
                return int(val)
            except (ValueError, TypeError) as e:
                if log is not None:
                    click.echo('WARNING:' + str(sample['UID']) + '  '+ name, file=log)
                return nullValue
        else:
            return val

    # write the data into database
    def write_db(dbhandler, data, label):

        dbhandler.remove()
        with click.progressbar(data.items(), label='Writing ' + label + ':\t', width=25,
                               fill_char=click.style('>', fg='green')) as bar:
            for k, v in bar:
                dbhandler.insert(v)



    # check if filter contains valid query
    # also get the data size.
    bar_length=0
    query = ''
    # filter from a file
    click.echo('Preprocessing, please wait....')
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
    click.echo('Done.')

    print(bar_length)
    if demo>0 :
        bar_length = demo




    ##########################
    # Display settings
    ##########################
    print()
    print('Script settings:')
    print('input_db: ' + input_db)
    print('input_collection: ' + input_collection)
    print('output_db: ' + output_db)
    print('output collections: {}, {}, {}, {}'.format(output_collection_individuals, output_collection_biosamples,
                                                        output_collection_callsets, output_collection_variants))
    print('filtering condition: ' + str(query))
    print('demo mode: ' + (str(demo) if demo>0 else 'false'))
    print('no writing: ' + ('true' if dnw else 'false'))
    print('no overwrite warning: ' + ('true' if dna else 'false'))
    print('log file: ' + (str(log) if log else 'none'))





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

            ###########################################################
            # check and generate INDIVIDUALS, BIOSAMPLES and CALLSETS #
            ###########################################################

            #only samples with enough attributes are assumed to be valid.
            try:
                no_validSamples += 1

                # generate ids
                # TODO: check for SERIESID => if not (progenetix ...) => PMID
                callset_id = 'pgxcs::'+sample['SERIESID']+'::'+sample['UID']
                biosample_id = 'PGX_AM_BS_'+sample['UID']
                individual_id = 'PGX_IND_'+sample['UID']

                ######################################################################################
                # external identifiers
                # now just taking them from the sample collection (have been added there)
                ######################################################################################
                external_identifiers = sample['external_identifiers']
                external_identifiers = list(external_identifiers.values())

                ################################################################
                # generate a biosample
                ################################################################

                if biosample_id in biosamples:
                    # if same biosample_id exists, report an error
                    if log is not None:
                        click.echo('Duplicate biosample_id: '+biosample_id, file=log)
                    continue
                else:

                    # Processing specific attributes
                    # TODO: fixing country names should be in an external cleanup script
                    try:
                        country = get_attribute('country', sample['geo_data']['info'])
                        city = get_attribute('city', sample['geo_data']['info'])
                        geoLabel = get_attribute('geo_label', sample['geo_data'])
                        geoPrecision = get_attribute('geo_precision', sample['geo_data'])
                        geolat = get_attribute('coordinates', sample['geo_data']['geo_json'])[1]
                        geolong = get_attribute('coordinates', sample['geo_data']['geo_json'])[0]

                        if city:
                            geoPrecision = 'city'
                            if country:
                                geoLabel = city + ', ' + country
                            else:
                                geoLabel = city
                        elif country:
                            geoLabel = country
                            geoPrecision = 'country'
                    except:
                        if log:
                            print('KeyError: ' + sample['UID'] + ' has no "geo_data".', file=log)
                        geoLabel = None
                        geoPrecision = None
                        geolat = None
                        geolong = None
                        city = None
                        country = None


                    geo_info = { 'label': geoLabel,
                                'precision': geoPrecision, 
                                'city': city,
                                'country': country,
                                'latitude': geolat, 
                                'longitude': geolong }






                    biosamples[biosample_id] = {
                                                'id': biosample_id,
                                                'description': get_attribute('DIAGNOSISTEXT', sample),
                                                'biocharacteristics': [
                                                    {
                                                        'description': get_attribute('DIAGNOSISTEXT', sample),
                                                        'type': {
                                                            'id': 'pgx:icdot:'.format(str(get_attribute('ICDTOPOGRAPHYCODE', sample))),
                                                            'label': get_attribute('ICDTOPOGRAPHY', sample)
                                                        }
                                                    },
                                                    {
                                                        'description': get_attribute('DIAGNOSISTEXT', sample),
                                                        'type': {
                                                            'id': 'pgx:icdom:'.format(re.sub('/', '', str(get_attribute('ICDMORPHOLOGYCODE', sample)))),
                                                            'label': get_attribute('ICDMORPHOLOGY', sample)
                                                        }  
                                                    },
                                                    {
                                                        'description': get_attribute('DIAGNOSISTEXT', sample),
                                                        'type': {
                                                            # 'id': 'ncit:' + get_attribute('NCIT:CODE', sample),
                                                            'id': 'ncit:'.format(get_attribute('NCIT:CODE', sample)),
                                                            'label': get_attribute('NCIT:TERM', sample)
                                                       }
                                                    }
                                                        
                                                ],
                                                'updated': datetime.datetime.utcnow(),
                                                'individual_id': individual_id,
                                                'project_id': get_attribute('SERIESID', sample),
                                                'age_at_collection':{
                                                    'age_class':{
                                                        "label" : None,
                                                        "id" : None
                                                    },
                                                    'age': get_attribute('AGEISO', sample)
                                                },
                                                'external_ids': external_identifiers,
                                                'geo_provenance': geo_info,
                                                "data_use_conditions" : {
                                                      "label" : "no restriction",
                                                      "id" : "DUO:0000004"
                                                },
                                                'info':{
                                                   'death': get_attribute('DEATH', sample),
                                                    'followup_months': get_attribute('FOLLOWUP', sample),
                                                    'tnm': get_attribute('TNM', sample)
                                                } 
                    }


                    no_biosamples += 1

                ################################################################
                # generate an individual
                ################################################################
                if individual_id in individuals:
                    # if same individual_id exists, report an error
                    if log is not None:
                        click.echo('Duplicate individual_id: '+individual_id, file=log)

                else:
                    # processing specific attributes

                    # sex
                    sex = {'term_id': 'PATO:0020000', 'term_label': 'genotypic sex' }
                    FemaleMatchObj = re.search('^f', str(get_attribute('SEX', sample, 'str')))
                    MaleMatchObj = re.search('^m', str(get_attribute('SEX', sample, 'str')))
                    if MaleMatchObj:
                        sex = {'id': 'PATO:0020001', 'label': 'male genotypic sex' }
                    elif FemaleMatchObj:
                        sex = {'id': 'PATO:0020002', 'label': 'female genotypic sex' }

                    individuals[individual_id] = {
                                                'id': individual_id,
                                                'description': None,
                                                'biocharacteristics': [
                                                    {
                                                        'description': get_attribute('SEX', sample, 'str'),
                                                        'type': sex
                                                    },
                                                    {
                                                        'description' : None,
                                                        'type':{
                                                            "id" : "NCBITaxon:9606",
                                                            "label" : "Homo sapiens"
                                                        }
                                                    }
                                                ],
                                                "data_use_conditions" : {
                                                      "label" : "no restriction",
                                                      "id" : "DUO:0000004"
                                                },
                                                'geo_provenance': geo_info,
                                                'updated': datetime.datetime.utcnow()
                                                }
                    no_individuals +=1


                ################################################################
                # check and generate callset
                ################################################################
                if callset_id in callsets:
                    # if same callset_id exists, report an error
                    if log is not None:
                        click.echo('Duplicate callset_id:'+callset_id, file=log)
                else:
                    callsets[callset_id] = {
                                            'id': callset_id,
                                            'description': get_attribute('PLATFORMLABEL', sample),
                                            'biosample_id': biosample_id,
                                            "data_use_conditions" : {
                                                      "label" : "no restriction",
                                                      "id" : "DUO:0000004"
                                            },
                                            # 'variant_set_id': variantset_id,
                                            'updated': datetime.datetime.utcnow(),
                                            'geo_provenance': geo_info,
                                            'info': {
                                                'statusmaps':{

                                                        'dupmap': get_attribute('dupmap', sample['statusmaps']),
                                                        'delmap': get_attribute('delmap', sample['statusmaps']),
                                                        'dupmax': get_attribute('dupmax', sample['statusmaps']),
                                                        'delmin': get_attribute('delmin', sample['statusmaps']),
                                                        'binning': get_attribute('binning', sample['statusmaps']),
                                                        'intervals': get_attribute('intervals', sample['statusmaps'])
                                                            
                                                        }
                                                      
                                                
                                            }
                                        }
                    no_callsets += 1



                ###############################
                # check and generate VARIANTS #
                ###############################

                # only simples with none-empty "variants_cnv" is valid and worthy checking
                if ('variants' in sample) and (sample['variants'] is not None) and (len(sample['variants']) > 1):
                    no_sample_w_segments += 1

                    # Generate callset id
                    # TODO: see above, PMID etc.
                    # callset_id = 'pgxcs::'+sample['SERIESID']+'::'+sample['UID']


                    ######################
                    # scan every segment
                    ######################
                    for seg in sample['variants']:

                        no_segments += 1
                        # alternate_bases = get_attribute('alternate_bases', seg)
                        variant_type = get_attribute('variant_type', seg)
                        start = get_attribute('start',seg,'int')
                        end = get_attribute('end', seg,'int')
                        chrom = get_attribute('reference_name', seg, 'str')
                        # cipos,ciend may get values which reflect the array precision
                        # cipos = []
                        # ciend = []
                        segvalue = get_attribute('value', seg['info'], 'float')

                        digest = '{}:{}-{}:{}'.format(chrom,start,end,variant_type)


                        variants[str(no_segments)] = {
                            'callset_id': callset_id,
                            'digest': digest,
                            'start': [start],
                            'end': [end],
                            'reference_name': chrom,
                            'reference_bases': '.',
                            # 'alternate_bases': alternate_bases,
                            'variant_type': variant_type,
                            # 'cipos': cipos,
                            # 'ciend': ciend,
                            'info':{
                                'cnv_value': segvalue,
                                'cnv_length': get_attribute('svlen', seg['info'],'int'),                                   
                            },
                            'updated': str(datetime.datetime.utcnow()),
                            'variantset_id': variantset_id,
                            'biosample_id': biosample_id,
                            'mate_name':None
                            }

            except Exception as e:
                if log:
                    print('ERROR: Unable to process:{} {}'.format(sample['UID'], str(e)), file=log)











            ######################
            # Demo mode counter
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
    # click.echo()


    # click.echo()
    # for k, var in variants.items():
    #     numcall = len(var['calls'])
    #     if numcall > 1:
    #         no_variantMultSeg += 1
    #     else:
    #         no_variantOneSeg += 1

    # click.echo(str(no_sample_w_segments) + '\t samples have at least one segment')
    # click.echo(str(no_segments) + '\t segments(calls) found')
    # click.echo(str(no_variantOneSeg) + '\t segments are single events in a sample (' + str(no_variantOneSeg) +
    #            '/'+str(no_segments)+' = ' + str(round(no_variantOneSeg/no_segments*100, 2)) + '%)')
    # click.echo(str(no_segments-no_variantOneSeg) + '\t segments have companies (' + str(no_segments-no_variantOneSeg) +
    #            '/'+str(no_segments)+' = ' + str(round((no_segments-no_variantOneSeg)/no_segments*100, 2)) + '%)')
    click.echo(str(no_segments) + '\t variants created')
    # click.echo(str(no_variantOneSeg) + '\t variants are unique (' + str(no_variantOneSeg) +
    #            '/'+str(no_uniqueSegments)+' = ' + str(round(no_variantOneSeg/no_uniqueSegments*100, 2)) + '%)')
    # click.echo(str(no_variantMultSeg) + '\t variants have multiple calls (' + str(no_variantMultSeg) +
    #            '/'+str(no_uniqueSegments)+' = ' + str(round(no_variantMultSeg/no_uniqueSegments*100, 2)) + '%)')
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
                write_db(db_out[output_collection_individuals], individuals, output_collection_individuals)
                write_db(db_out[output_collection_biosamples], biosamples, output_collection_biosamples)
                write_db(db_out[output_collection_callsets], callsets, output_collection_callsets)
                write_db(db_out[output_collection_variants], variants, output_collection_variants)

                break

            else:
                print('invalid input')
        click.echo()

        # write variant set manually
        variant_set = {'id': 'AM_VS_CRCH36',
                'dataset_id': 'arraymap',
                'reference_set_id:': 'GRCh36',
                }
        db_out['variantsets'].remove()
        db_out['variantsets'].insert(variant_set)

    client.close()

# main
if __name__ == '__main__':
    cli()
