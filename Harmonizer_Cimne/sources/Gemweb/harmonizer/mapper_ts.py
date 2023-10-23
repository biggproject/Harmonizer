# import hashlib
# import json
#
# import pandas as pd
# from neo4j import GraphDatabase
# from rdflib import Namespace
#
# from utils import save_to_hbase
#
#
# def map_data(data, **kwargs):
#     freq = kwargs['freq']
#     namespace = kwargs['namespace']
#     user = kwargs['user']
#     config = kwargs['config']
#
#     hbase_conn2 = config['hbase_harmonized_data']
#     neo4j_connection = config['neo4j']
#
#     neo = GraphDatabase.driver(**neo4j_connection)
#     n = Namespace(namespace)
#     with neo.session() as ses:
#         source_id = ses.run(
#             f"""Match (o: ns0__Organization{{ns0__userId:'{user}'}})-[:ns0__hasSource]->(s:GemwebSource)
#             return id(s)""")
#         source_id = source_id.single().get("id(s)")
#     df = pd.DataFrame.from_records(data)
#
#     for gem_id, data_group in df.groupby("gem_id"):
#         # find device with ID imported from source
#         device_id = gem_id.decode()
#         data_group["ts"] = pd.to_datetime(data_group['measurement_ini'].apply(bytes.decode).apply(int), unit="s")
#         data_group.set_index("ts", inplace=True)
#         data_group.sort_index(inplace=True)
#         dt_ini = data_group.iloc[0].name
#         dt_end = data_group.iloc[-1].name
#         with neo.session() as session:
#             device_neo = session.run(f"""
#             MATCH (g)<-[:ns0__importedFromSource]-(d) WHERE id(g)={source_id} and d.uri =~ ".*#{device_id}-DEVICE-gemweb"
#             RETURN d""")
#             for d_neo in device_neo:
#                 prefix = (device_id + '~').encode("utf-8")
#                 list_id = f"{device_id}-DEVICE-gemweb-LIST-RAW-{freq}"
#                 list_uri = str(n[list_id])
#                 new_d_id = hashlib.sha256(list_uri.encode("utf-8"))
#                 new_d_id = new_d_id.hexdigest()
#                 session.run(f"""
#                     MATCH (device: ns0__Device {{uri:"{d_neo["d"].get("uri")}"}})
#                     MERGE (list: ns0__MeasurementList {{
#                         uri: "{list_uri}",
#                         ns0__measurementKey: "{new_d_id}",
#                         ns0__measurementUnit: "kWh",
#                         ns0__measurementFrequency: "{freq}",
#                         ns0__measurementReadingType: "real",
#                         ns0__measuredProperty: "{d_neo['d']['ns0__deviceType']}"
#                     }})<-[:ns0__hasMeasurementLists]-(device)
#                     SET
#                         list.ns0__measurementListStart = CASE
#                             WHEN list.n0s__measurementListStart < datetime("{dt_ini.tz_localize("UTC").to_pydatetime().isoformat()}")
#                                 THEN list.ns0__measurementListStart
#                                 ELSE datetime("{dt_ini.tz_localize("UTC").to_pydatetime().isoformat()}")
#                             END,
#  	                    list.ns0__measurementListEnd = CASE
#  	                    WHEN list.ns0__measurementListEnd > datetime("{dt_end.tz_localize("UTC").to_pydatetime().isoformat()}")
#  	                        THEN list.ns0__measurementListStart
#  	                        ELSE datetime("{dt_end.tz_localize("UTC").to_pydatetime().isoformat()}")
#  	                    END
#                     return list
#                 """)
#                 data_group['listKey'] = new_d_id
#                 device_table = f"data_{freq}_{user}_device"
#                 save_to_hbase(data_group.to_dict(orient="records"), device_table, hbase_conn2,
#                               [("info", ['measurement_end']), ("v", ['value'])], row_fields=['listKey', 'measurement_ini'])
#                 period_table = f"data_{freq}_{user}_period"
#                 save_to_hbase(data_group.to_dict(orient="records"), period_table, hbase_conn2,
#                               [("info", ['measurement_end']), ("v", ['value'])], row_fields=['measurement_ini', 'listKey'])
