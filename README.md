# Latest update:
## general modifications:
>1. variant, biosample and callset are all following the same naming convention.
2. they all have an unique "id" attribute generated as AM_V_"UID", AM_BS_"UID", and AM_CS_"UID".
3. "UID" is from data collection "samples" in database "arraymap".
4. all other attirbutes of them are exactly following the GA4GH schema.

## gvnc.py:
>1. calls do not store biosample_id anymore, it can be retrieved through callset.
2. biosample_id of a variant is always generated now, instead of checking existence and generating when absent.

## gbnc.py:
>1. new option:'-s', '--status',  default='-exclude'
2. it filters the data collection by STATUS value, param must be in format [+/-]keyword. "+" means to include, "-" means to exclude. 
3. it accepts regular expression, eg: "-include\\|\^NA" as an option from cmd.
4. biosample_id is always generated now, instead of checking existence and generating when absent.
5. characteristics of biosample is implemented. Right now, it simply captures ICD information.
6. duplicated "UID" (duplicated biosample_id) problem is temporarily resolved by only including simples with more than 50 attributes.
7. shortnames '-d' and '-l' for options '--demo' and '--log' are provided.


# Previous updates:
---
# gbnc.py
New cli script to generate BIOSAMPLEs and CALLSETs.
Usage is very similar to gvnc.py except two destination collections are needed instead of one:
* '-dstb, --collection_dst_biosamples TEXT      The collection to write into, default is "biosamples"'
* '-dstc, --collection_dst_callsets TEXT        The collection to write into, default is "callsets"'

Not all relevent attributes in sample are copied to biosample, just have some for deomstration.




# gvnc.py:
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


## Options:
*  `-db,     --dbname TEXT              The name of the database, default is "arraymap"`
*  `-src,    --collection_src TEXT      The collection to read from, default is "samples"`
*  `-dst,    --collection_dst TEXT      The collection to write into, default is "variants"`
*  `--demo   INTEGER RANGE              Only process a limited number of entries`
*  `--dnw                               Do Not Write to the db`
*  `--log    FILENAME                   Output errors and warnings to a log file`
*  `--help                              Show this message and exit.`


## Pre-requiremnt:
* sudo pip3 install pymongo
* sudo pip3 install click

***

# generate.py:
The old script version:

## USAGE:
* `python generate.py`

## Pre-requirement:
* `sudo pip install pymongo`

This script will scan the "samples", find all the "HG18 segment"s and put ones with identical type and locaation in one variant.
