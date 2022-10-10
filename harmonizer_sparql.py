#! /usr/bin/env python
# -*- coding: utf-8 -*-

#######################################
#--------------- IMPORTS -------------#
#######################################


import os
from random import gauss 
import sys
import pathlib
import json
import os
from rdflib import Graph
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON, N3, TURTLE, JSONLD, RDF


######################################
#----------- Functions --------------#
######################################


#========================================================================================================================================= 
#--start: Convert JSON to RDF---------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 
# Convert a JSON file into a RDF or JSONLD file thanks to a mapping file (RML)
def harmonizer_sparql(filename, destination_file):


    # Specify the DBPedia endpoint
    g = Graph().parse(filename,format='n3')
    #sparql = SPARQLWrapper("http://dbpedia.org/sparql")

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
    g+= results

    #print(g.serialize(format='ttl'))
    g.serialize(destination=destination_file, format='ttl')

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
    args = sys.argv #['Demo007', "data_output.ttl"] 
    
    if len(args)!= 4:
        print('Error: wrong number of arguments')
        sys.exit()
    elif args[2].split(".")[1] != 'ttl':
        print('Error: a ttl file is waiting')
        sys.exit()
    elif args[3].split(".")[1] != 'ttl':
        print('Error: a ttl file is waiting')
        sys.exit()
    elif os.path.exists(os.path.join(path_data, 'data', args[1], args[2])) == False : 
        print("Error: the file does not exists")
        sys.exit()
    else :
        demo_folder = args[1]
        filename = args[2]
        destination = args[3]
        output_file = os.path.join(path_data, 'data', demo_folder, filename)
        destination_filename = os.path.join(path_data, 'data', demo_folder, destination)
        harmonizer_sparql(output_file, destination_filename)

