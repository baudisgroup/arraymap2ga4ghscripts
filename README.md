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
*  `-db,     --dbname TEXT              The name of the database, default is arraymap`
*  `-src,    --collection_src TEXT      The collection to read from, default is samples`
*  `-dst,    --collection_dst TEXT      The collection to write into, default is myvariants`
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
