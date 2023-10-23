import pandas as pd
from sodapy import Socrata


class SocrataClient:

    def __init__(self):
        pass

    @property
    def client(self):
        return Socrata(self.url, self.application_token)

    @property
    def url(self):
        return None

    @property
    def dataset_id(self):
        return None

    @property
    def application_token(self):
        return None

    def query(self, limit=None, offset=None, **kwargs):
        results = self.client.get(self.dataset_id, limit=limit, offset=offset, **kwargs)
        return pd.DataFrame.from_records(results)
