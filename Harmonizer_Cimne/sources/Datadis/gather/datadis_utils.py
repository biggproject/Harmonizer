import ast

import settings
import utils
from neo4j import GraphDatabase

bigg = settings.namespace_mappings['bigg']

def decrypt_passwords(users, settings):
    for user in users:
        enc_dict = user['password']
        user['password'] = utils.security.decrypt(enc_dict, settings.secret_password)
    return users


def get_users(neo4j):
    driver = GraphDatabase.driver(**neo4j)
    with driver.session() as session:
        users = session.run(
            f"""Match (n:DatadisSource)<-[:hasSource]-(o:{bigg}__Organization)
            CALL{{With o Match (o)<-[:{bigg}__hasSubOrganization*0..]-(d:{bigg}__Organization) WHERE NOT (d)<-[:{bigg}__hasSubOrganization]-() return d}}
             return n.username AS username, n.Password AS password,
             d.userID AS user, split(d.uri,"#")[0]+'#'AS namespace
            """).data()
    return users
