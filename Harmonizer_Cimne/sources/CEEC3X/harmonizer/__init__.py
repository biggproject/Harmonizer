import argparse
import re
import utils
from utils.cache import Cache
from .mapper_static import harmonize_IdentificacionEdificio, harmonize_DatosGeneralesyGeometria, \
    harmonize_CondicionesFuncionamientoyOcupacion, harmonize_Demanda, harmonize_Consumo, harmonize_Emissions,\
    harmonize_Calificacion


def harmonize_command_line(arguments, config=None, settings=None):
    ap = argparse.ArgumentParser(description='Mapping of GPG data to neo4j.')
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    args = ap.parse_args(arguments)
    # IdentificacionEdificio
    hbase_conn = config['hbase_store_raw_data']
    data_harmonizer_map = {
        "IdentificacionEdificio": harmonize_IdentificacionEdificio,
        "DatosGeneralesyGeometria": harmonize_DatosGeneralesyGeometria,
        "CondicionesFuncionamientoyOcupacion": harmonize_CondicionesFuncionamientoyOcupacion,
        "Demanda": harmonize_Demanda,
        "Consumo": harmonize_Consumo,
        "EmisionesCO2": harmonize_Emissions,
        "Calificacion": harmonize_Calificacion
    }
    Cache.load_cache()
    for datatype, harmonizer in data_harmonizer_map.items():
        hbase_table = f"raw_CEEC3X_static_{datatype}__{args.user}"
        i = 0
        for data in utils.hbase.get_hbase_data_batch(hbase_conn, hbase_table, batch_size=1000):
            dic_list = []
            print("parsing hbase")
            for b_c, x in data:
                item = dict()
                for k, v in x.items():
                    k1 = re.sub("^info:", "", k.decode())
                    item[k1] = v
                item.update({
                    "building_organization_code": b_c.decode().split("~")[1],
                    "certificate_unique_code": b_c.decode().split("~")[0]}
                )
                dic_list.append(item)
            print("parsed. Mapping...")
            i += len(dic_list)
            print(i)
            harmonizer(dic_list, namespace=args.namespace, user=args.user, config=config)
