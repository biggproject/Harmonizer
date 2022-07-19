#! /usr/bin/env python
# -*- coding: utf-8 -*-

#######################################
#--------------- IMPORTS -------------#
#######################################


import os 
import sys
import pathlib
import json
import os
from rdflib import Graph
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON


######################################
#----------- Functions --------------#
######################################


#========================================================================================================================================= 
#--start: Convert JSON to RDF---------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 
# Convert a JSON file into a RDF or JSONLD file thanks to a mapping file (RML)
def harmonizer_sparql(filename):


    # Specify the DBPedia endpoint
    g = Graph()
    result = g.parse(filename,format='n3')
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")

    # Default Query to add a source and the creation date
    query ="""
        prefix bigg: <http://bigg-project.eu/ontology#>
        prefix dc: <http://purl.org/dc/terms/> 

        CONSTRUCT {
            ?uri  a bigg:hamonizedData ;
                dc:created ?date ;
                dc:source "Hamonizer v1.0" .
            ?s ?p ?o .
        } WHERE {
            {
                SELECT ?date ?uri 
                WHERE {
                    BIND(NOW() AS ?date)
                    BIND(IRI(UUID()) AS ?uri)
                }
            }
            ?s ?p ?o .
        }
    """
    results = g.query(query)

    print('Results!')
    df_result = pd.DataFrame(results, columns = results.vars)
    print(df_result)

#========================================================================================================================================= 
#--end: Convert JSON to RDF---------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 


######################################
#--------------- Main ---------------#
######################################


if __name__ == "__main__":

    # Global Path
    path_data = os.getcwd()

    # Arguments
    args = ['ll', "data_output.ttl"] #sys.argv
    
    if len(args)!= 2:
        print('Error: wrong number of arguments')
        sys.exit()
    elif args[1].split(".")[1] != 'ttl':
        print('Error: a ttl file is waiting')
        sys.exit()
    elif os.path.exists(args[1]) == False : 
        print("Error: the file does not exists")
        sys.exit()
    else :
        filename = args[1]
        harmonizer_sparql(filename)

