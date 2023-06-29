#! /usr/bin/env python
# -*- coding: utf-8 -*-

#######################################
#--------------- IMPORTS -------------#
#######################################


import os 
import sys
import pathlib
import os
import argparse
import glob
from rdflib import Graph
import re


######################################
#----------- Functions --------------#
######################################


#========================================================================================================================================= 
#--start: Check Arguments ----------------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 
# Check the arguments (input json, mapping rml,sparql query file, output ttl) of the the command line
def check_arguments():

    convert_activated = False
    sparql_activated  = False
    query_files = []

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help="Input file", required=True)
    parser.add_argument('--mapping', help="Input mapping file to convert")
    parser.add_argument('--sparql',  nargs='*', help="Sparql file to interrogate the ttl file")
    parser.add_argument('--output', help="Name of the wanted output file", default="output.ttl")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print('Error, wrong input file')
        sys.exit()

    if args.mapping != None:
        if not os.path.exists(args.mapping):
            print('Error, wrong mapping file')
            sys.exit()
        elif args.input.split('.')[-1] == 'json' :
            print('Activation of the conversion')
            convert_activated = True
        else :
            print('Mapping spécifié mais pas de json en input : pas de conversion')
            convert_activated = False

    if args.sparql != None:
        for i in args.sparql:
            if not os.path.exists(i):
                print('Error, wrong sparql file')
                sys.exit()
            else :
                query_files.append(i)
                print('Activation of the request')
                sparql_activated = True
        
    return args.input, args.mapping, args.output, query_files, convert_activated, sparql_activated
#========================================================================================================================================= 
#--end: Check Arguments ------------------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 


#========================================================================================================================================= 
#--start: Replace Json source into mapping file ------------------------------------------------------------------------------------------
#========================================================================================================================================= 
# Find all sources file name into the rml file to replace it with the corresponding Input file
def correct_RML_inputfile(mapping_file, input_file, mapping_updated_file):

    #reg = re.compile(r'rml:source\s+\"([^"]+\.(json|csv))\"\s*;')
    with open(mapping_file) as rml:
        data = rml.read()

    data = data.replace('__SOURCE__',input_file)

    with open(mapping_updated_file,'w') as f:
        f.write(data)
#========================================================================================================================================= 
#--end: Replace Json source into mapping file --------------------------------------------------------------------------------------------
#========================================================================================================================================= 


#========================================================================================================================================= 
#--start: Convert JSON to RDF-------------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 
# Convert a JSON file into a RDF or JSONLD file thanks to a mapping file (RML)
def harmonizer(rml_lib, input_file, mapping_file, output_file):
    # HARMONIZE DATA 
    output_format = output_file.split('.')[-1]
    if output_format.lower() == 'jsonld':
        os.system("java -jar {} -m {} -s jsonld -o {}".format(rml_lib, mapping_file,output_file))
    
    elif output_format.lower() == 'ttl':
        os.system("java -jar {} -m {} -o {}".format(rml_lib, mapping_file,output_file))
        g = Graph()
        ttl = pathlib.Path(output_file).read_text()
        g.parse(data=ttl, format="turtle")
        g.serialize(destination=output_file,format="turtle")
        
    else:
        print('Error, wrong format')
        sys.exit()
#========================================================================================================================================= 
#--end: Convert JSON to RDF---------------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 


#========================================================================================================================================= 
#--start: Query ttl file ---------------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 
# Adding multiple queries to the harmonizer tool, to add / modify the json file converted into a ttl one
def harmonizer_sparql(ttl_filename,path_query_files, destination_file):
    # Specify the DBPedia endpoint
    g = Graph().parse(ttl_filename,format='n3')
    
    for file in path_query_files:
        f = open(file, 'r')
        query = f.read()
        f.close()
        #sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        g.update(query)
        

    g.serialize(destination=destination_file, format='ttl')
#========================================================================================================================================= 
#--end: Query ttl file -----------------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 



######################################
#--------------- Main ---------------#
######################################


if __name__ == "__main__":

    rml_lib = glob.glob('*.jar')[0]

    # Arguments
    input_file, mapping_file, output_file, query_files, convert_activated, sparql_activated = check_arguments()
    mapping_updated_file = mapping_file.split('.rml')[0] + "_updated.rml"

    correct_RML_inputfile(mapping_file, input_file, mapping_updated_file)


    if convert_activated and sparql_activated == False: 
        print('Harmonizer without queries')
        # Convert data to RDF
        harmonizer(rml_lib, input_file, mapping_updated_file, output_file)
    
    elif convert_activated and sparql_activated :
        # Convert data to RDF
        harmonizer(rml_lib, input_file, mapping_updated_file, 'tmp_output.ttl')
        harmonizer_sparql('tmp_output.ttl', query_files, output_file)
        os.system('del tmp_output.ttl')

    elif convert_activated == False and sparql_activated :
        # Add Sparql Estage
        if input_file.split('.')[-1]== 'ttl':
            harmonizer_sparql(input_file, query_files, output_file)
        else :
            print('Error : wrong format of the input file')
            