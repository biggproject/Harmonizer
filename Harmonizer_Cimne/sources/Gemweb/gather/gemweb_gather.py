import re
from datetime import datetime
import os
import sys

from utils.utils import log_string

sys.path.append(os.getcwd())
import utils
BATCH = 1000

def get_data(gemweb, data_type):
    # TODO: Read data from Gemweb
    #data = gemweb.gemweb.gemweb_query(data_type['endpoint'], category=data_type['category'])
    h_table_name = f"gemweb_{data_type['name']}_icaen"
    data = []
    for data_tmp in utils.hbase.get_hbase_data_batch(gemweb, h_table_name):
        dic_list = []
        for n_ens, x in data_tmp:
            item = dict()
            for k, v in x.items():
                k1 = re.sub("^info:", "", k.decode())
                item[k1] = v
            item.update({"id": n_ens})
            dic_list.append(item)
        data.extend(dic_list)
    return data

def update_static_data(data_type, mongo_conf, hbase_conf, connection, data_source, gemweb):
    log_string("getting_data", mongo=False)
    data = gemweb.gemweb.gemweb_query(data_type['endpoint'], category=data_type['category'])
    log_string("uploading_data", mongo=False)

    htable = get_HTable(hbase, "{}_{}_{}".format(data_source["hbase_name"], data_type['name'], user), {"info": {}})
    save_to_hbase(htable, data, [("info", "all")], row_fields=['id'], version=version)

    connection[data_type['name']]['version'] = version
    connection[data_type['name']]['inserted'] = len(data)
    connection[data_type['name']]['date'] = datetime.now()
    mongo = connection_mongo(mongo_conf)
    mongo[data_source['info']].replace_one({"_id": connection["_id"]}, connection)



