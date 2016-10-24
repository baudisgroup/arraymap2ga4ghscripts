USAGE: python3 generate.py

Pre-requirement: create Collection "myvariants" in DB "arraymap"

This script will scan the "samples", find all the "HG18 segment"s and put ones with identical localation in one variant.
identical localation is defined as with the same: Chromosome number, start postion, end postion and alternation type.