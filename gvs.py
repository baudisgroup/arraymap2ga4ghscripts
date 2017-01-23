from pymongo import MongoClient


client = MongoClient()
db = client.arraymap.variantsets
db.remove()

variant_set = {'id': 'AM_VS_HG18', 
				'dataset_id': 'arraymap',
				'reference_set_id': 'HG18',
				}

insert_id = db.insert(variant_set)
