import xmltodict


def read_xml_certificate(cert_file):
    with open(cert_file, 'r') as f:
        data = f.read()
        # skip first xml line
        data = "\n".join([d for d in data.split("\n")[1:]])
    bs_data = xmltodict.parse(data)
    return bs_data['DatosEnergeticosDelEdificio']


