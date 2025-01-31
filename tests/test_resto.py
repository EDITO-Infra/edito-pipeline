import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import logging
from typing import TYPE_CHECKING, List, Tuple
from datetime import datetime, date
import json
import pystac
import pandas as pd
# test the creation of a layer catalog using a given pipeline config

from metagisconfig2stac.resto import STACRestoManager

data_dir = os.path.join(os.path.dirname(__file__), '../data')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename=f'{data_dir}/push_to_resto.log')

targetstac = 'stac/catalog.json'
targetstac = 'stac/catalog.json'
pipeline_config = {
                "push_to_resto" : "True",
                "resto_instance" : "staging",
                "datadir" : data_dir,
                "stac_s3": os.path.splitext(targetstac)[0],
            }

stac_loc = os.path.join(data_dir, targetstac)
local_stac = pystac.Catalog.from_file(stac_loc)
resto_instance = pipeline_config['resto_instance']
resto = STACRestoManager(pipeline_config, local_stac=local_stac)

# # test get user features
feature_df = resto.client.get_all_user_features()
feature_df.to_csv(f'{data_dir}/{resto.client.resto_user}_all_features.csv')
print(feature_df)

responses = []

## snippet to test the post of a local stac to resto
# allvfcs = local_stac.get_children()

# catdata = local_stac.to_dict()

# for vfc in allvfcs:
#     vfcdata = vfc.to_dict()
#     print(vfcdata)
#     respcat = resto.post_to_child_catalog(vfcdata, catdata['id'])
#     responses.append(respcat)
#     for collection in vfc.get_collections():
#         colldata = collection.to_dict()
#         print(colldata['id'])
#         respcollcat = resto.post_collection_to_child_catalog(colldata, f"{catdata['id']}/{vfcdata['id']}")
#         respcollroot = resto.post_collection(colldata)
#         responses.append(respcollcat)
#         responses.append(respcollroot)
#         for item in collection.get_items():
#             itemdata = item.to_dict()
#             print(itemdata['id'])
#             resp3 = resto.post_item(itemdata)
#             responses.append(resp3)
    

# df = pd.DataFrame(responses)

# df.to_csv(f'{data_dir}/{resto_instance}_post_stac_responses.csv')


# snippet to delete a variable family catalog from local_stac.root_catalog
# responses = []
# vfcs = local_stac.get_children()
# for vfc in vfcs:
#     if 'bad_catalogid' in vfc.id:
#         print(f"Deleting {vfc.id}")
#        resp = resto.delete_catalog(f"{local_stac.id}/{vfc.id}")
#        print(resp)
#        responses.append(resp)
# df = pd.DataFrame(responses)
# df.to_csv(f'{data_dir}/{resto_instance}_delete_stac_responses.csv')