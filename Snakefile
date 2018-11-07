

## arraymap pipeline ##
# convert arraymap to ga4gh schema, and convert to hg38 
rule cnv2file:
    input:
        '/data/db/mongodb/arraymap_ga4gh.ns'
    output:
        '/Users/bogao/DataFiles/tmp/segments_36.txt'
    shell:
        'python arraymap2ga4gh.py --dna && '
        'python cnv2file.py'

rule liftover36to37:
    input:
        '/Users/bogao/DataFiles/tmp/segments_36.txt'
    output:
        '/Users/bogao/DataFiles/tmp/segments_37.txt'
    shell:
        'segment_liftover -i /Users/bogao/DataFiles/tmp/ -o /Users/bogao/DataFiles/tmp/ -c hg18ToHg19 -si segments_36.txt -so segments_37.txt -l /Users/bogao/DataFiles/Liftover\ archive/liftover/liftOver'

rule liftover36to38:
    input:
        '/Users/bogao/DataFiles/tmp/segments_36.txt'
    output:
        '/Users/bogao/DataFiles/tmp/segments_38.txt'
    shell:
        'segment_liftover -i /Users/bogao/DataFiles/tmp/ -o /Users/bogao/DataFiles/tmp/ -c hg18ToHg38 -si segments_36.txt -so segments_38.txt -l /Users/bogao/DataFiles/Liftover\ archive/liftover/liftOver'

rule file2db:
    input:
        '/Users/bogao/DataFiles/tmp/segments_37.txt',
        '/Users/bogao/DataFiles/tmp/segments_38.txt'
    output:
        '/Users/Shared/mongodump/arraymap/samples_web.bson'
    shell:
        'python arraymap_ga4gh_liftover.py &&'
        'sh dump_arraymap_ga4gh.sh'




## progenetix pipeline ##
## use -w (--latency-wait) 60

rule pg2file:
    input:
        '/data/db/mongodb/progenetix_ga4gh.ns'
    output:
        '/Users/bogao/DataFiles/tmp/segments_36_pg.txt'

    shell:
        'python arraymap2ga4gh.py --dna -dbin progenetix -dbout progenetix_ga4gh -f {{}} && '
        'python progenetix2files.py'



rule pg_liftover:
    input:
        '/Users/bogao/DataFiles/tmp/segments_36_pg.txt'
    output:
        '/Users/bogao/DataFiles/tmp/segments_38_pg.txt'
    shell:
        'segment_liftover -i /Users/bogao/DataFiles/tmp/ -o /Users/bogao/DataFiles/tmp/ -c hg18ToHg38 -si segments_36_pg.txt -so segments_38_pg.txt -l /Users/bogao/DataFiles/Liftover\ archive/liftover/liftOver'

rule pg_writedb:
    input:
        '/Users/bogao/DataFiles/tmp/segments_38_pg.txt'
    output:
        '/Users/Shared/mongodump/progenetix/samples_web.bson'
    shell:
        'python progenetix_ga4gh_liftover.py &&'
        'sh dump_progenetix_ga4gh.sh'