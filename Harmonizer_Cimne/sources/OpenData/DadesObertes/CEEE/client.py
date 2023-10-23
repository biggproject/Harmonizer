from sources.OpenData.DadesObertes.socrata import SocrataClient


class CEEE(SocrataClient):

    @property
    def url(self):
        return "analisi.transparenciacatalunya.cat"

    @property
    def dataset_id(self):
        return "j6ii-t3w2"

    @property
    def application_token(self):
        return "5PUy4W7VJav8SZYwypVqcnJ0Z"
