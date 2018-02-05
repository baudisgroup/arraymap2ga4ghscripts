rule build_arraymap2ga4gh:
    input:
        '/data/db/mongodb/arraymap.ns',
        'arraymap2ga4gh.py'
    # output:
    #     '/data/db/mongodb/arraymap_ga4gh.ns'
    shell:
        'python arraymap2ga4gh.py --dna'

rule cnv2file:
    input:
        '/data/db/mongodb/arraymap_ga4gh.ns'
    output:
        '/Users/bogao/DataFiles/tmp/segments_36.txt'
    shell:
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
    # output:
    #     '/data/db/mongodb/arraymap_ga4gh.ns'
    shell:
        'python arraymap_ga4gh_liftover.py'
