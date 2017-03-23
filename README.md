# Latest update:

### Script modification tips:
1. Please only use get_attribte() to get values, don't use simple[attribute].
2. get_attribute is impoved, please see the function comments for more.
3. Please try to assign values directly within the JSON block.


<br><br><br>
# Previous major updates:
## Update 4:
### arraymap2ga4gh.py:
1. gvnc.py & gbnv.py are now integrated into arraymap2ga4gh.py
2. most params' function remain the same, but with the names modified. 
3. Some new behaviors:
    * all the generated data is written to a new database different from the source.
    * user can suppress the database overwriting warning prompt.
    * user can use 2 filter params to manipulate with the input data.

### New read and write source controls:
1. the default input db is "arraymap"
2. the default output db is "arraymap_ga4gh"
3. user can change both sources and also collections names.
4. please see the help for a detailed list.

### New suppress parameter:
1. this script sometimes can run from hours, to make our life easier, it can be fully automatic now.
2. example: >python3 arraymap2ga4gh.py --dwa
3. this will suppress the warning prompt.
4. double check before you do this, as it will overwrite the database.

### New filtering function:
1. user can now provide a query by parameter or file.
2. the query must be of correct mongodb syntax.
3. when filtering through parameter, make sure to surround the query by "" and \ system symbol.
4. example: >python3 arraymap2ga4gh.py -f "{'ICDMORPHOLOGYCODE': {'\$regex':'\^[89]'}}"
5. when filtering through file, only the first line is used.
6. example: >python3 arraymap2ga4gh.py -ff f.txt
7. file has priority over param.




## Update 3:
### gbnc.py:
1. can generate individuals now.
2. related cli option "---collection_dst_individuals" is provided.
3. represents empty value as 'null' instead of ''.

### gvc.py:
generates pre-set "variant set".
<br><br><br>

## Update 2:
### general modifications:
1. variant, biosample and callset are all following the same naming convention.
2. they all have an unique "id" attribute generated as AM_V_"UID", AM_BS_"UID", and AM_CS_"UID".
3. "UID" is from data collection "samples" in database "arraymap".
4. all other attirbutes of them are exactly following the GA4GH schema.

### gvnc.py:
1. calls do not store biosample_id anymore, it can be retrieved through callset.
2. biosample_id of a variant is always generated now, instead of checking existence and generating when absent.

### gbnc.py:
1. new option:'-s', '--status',  default='-exclude'
2. it filters the data collection by STATUS value, param must be in format [+/-]keyword. "+" means to include, "-" means to exclude. 
3. it accepts regular expression, eg: "--status +include\\|\^NA" as an option from cmd.
4. biosample_id is always generated now, instead of checking existence and generating when absent.
5. characteristics of biosample is implemented. Right now, it simply captures ICD information.
6. duplicated "UID" (duplicated biosample_id) problem is temporarily resolved by only including simples with more than 50 attributes.
7. shortnames '-d' and '-l' for options '--demo' and '--log' are provided.

<br><br><br>


## Update 1:
### gbnc.py
New cli script to generate BIOSAMPLEs and CALLSETs.
Usage is very similar to gvnc.py except two destination collections are needed instead of one:
* '-dstb, --collection_dst_biosamples TEXT      The collection to write into, default is "biosamples"'
* '-dstc, --collection_dst_callsets TEXT        The collection to write into, default is "callsets"'

Not all relevent attributes in sample are copied to biosample, just have some for deomstration.

<br><br><br>

## Original release:
### gvnc.py:
New cli script gvnc.py is added.  

  This script checks through the given source db collection, generate new
  VARIANTs containing CALLs, and put them in the destination db collection.  

  >NOTE: Please manually create the destination collection to avoid
  accidental overwritting of existing collection.  

  >WARNING: This script will overwrite the destination db collection, check
  before running.  

  >Example: python3 gvnc.py --demo 1000 --dnw --log log.txt This will run for
  1000 valid samples, the result wont be written to the database, and any
  error or warning messages will be shown in log.txt.


#### Options:
*  `-db,     --dbname TEXT              The name of the database, default is "arraymap"`
*  `-src,    --collection_src TEXT      The collection to read from, default is "samples"`
*  `-dst,    --collection_dst TEXT      The collection to write into, default is "variants"`
*  `--demo   INTEGER RANGE              Only process a limited number of entries`
*  `--dnw                               Do Not Write to the db`
*  `--log    FILENAME                   Output errors and warnings to a log file`
*  `--help                              Show this message and exit.`


### Pre-requiremnt:
* sudo pip3 install pymongo
* sudo pip3 install click

<br><br><br>

## Proto-version:
### generate.py:
The old script version:

#### USAGE:
* `python generate.py`

#### Pre-requirement:
* `sudo pip install pymongo`

This script will scan the "samples", find all the "HG18 segment"s and put ones with identical type and locaation in one variant.
