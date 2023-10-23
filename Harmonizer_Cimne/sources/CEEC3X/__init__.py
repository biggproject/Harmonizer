from .gather import gather
from .harmonizer import harmonize_command_line
from .harmonizer.mapper_static import harmonize_IdentificacionEdificio, harmonize_DatosGeneralesyGeometria,\
    harmonize_CondicionesFuncionamientoyOcupacion, harmonize_Demanda, harmonize_Consumo, harmonize_Emissions,\
    harmonize_Calificacion
from .. import SourcePlugin


class Plugin(SourcePlugin):
    source_name = "CEEC3X"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)

    def get_mapper(self, message):
        print(message["collection_type"])
        if message["collection_type"] == "IdentificacionEdificio":
            return harmonize_IdentificacionEdificio
        elif message["collection_type"] == "DatosGeneralesyGeometria":
            return harmonize_DatosGeneralesyGeometria
        elif message["collection_type"] == "CondicionesFuncionamientoyOcupacion":
            return harmonize_CondicionesFuncionamientoyOcupacion
        elif message["collection_type"] == "Demanda":
            return harmonize_Demanda
        elif message["collection_type"] == "Consumo":
            return harmonize_Consumo
        elif message["collection_type"] == "EmisionesCO2":
            return harmonize_Emissions
        elif message["collection_type"] == "Calificacion":
            return harmonize_Calificacion
        else:
            return None

    def get_kwargs(self, message):
        if message["collection_type"] in ["IdentificacionEdificio", "DatosGeneralesyGeometria",
                                          "CondicionesFuncionamientoyOcupacion", "Demanda", "Consumo",
                                          "EmisionesCO2", "Calificacion"]:
            return {
                "namespace": message['namespace'],
                "user": message['user'],
                "config": self.config
            }
        else:
            return {}

    def get_store_table(self, message):
        if message["collection_type"] in ["IdentificacionEdificio", "DatosGeneralesyGeometria",
                                          "CondicionesFuncionamientoyOcupacion", "Demanda", "Consumo",
                                          "EmisionesCO2", "Calificacion"]:
            return f"raw_{self.source_name}_static_{message['collection_type']}__{message['user']}"
