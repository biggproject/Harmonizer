#! /usr/bin/env python
# -*- coding: utf-8 -*-

#######################################
#--------------- IMPORTS -------------#
#######################################


import os 
import sys
import wget
import pathlib
import json
import os
from rdflib import Graph


######################################
#----------- Functions --------------#
######################################


#========================================================================================================================================= 
#--start: Convert JSON to RDF---------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 
# Convert a JSON file into a RDF or JSONLD file thanks to a mapping file (RML)
def harmonizer(json_file, mapping_file, output_format, output_file):

    #parsed_data = json.loads(pathlib.Path(json_file).read_text())
    #output_file = json_file.split('.')[0] + '_output.' + output_format.lower()

    # HARMONIZE DATA 
    if output_format.lower() == 'jsonld':
        os.system("java -jar rml.jar -m {} -s jsonld -o {}".format(mapping_file,output_file))
        
    elif output_format.lower() == 'ttl':
        os.system("java -jar rml.jar -m {} -o {}".format(mapping_file,output_file))
        g = Graph()
        ttl = pathlib.Path(output_file).read_text()
        g.parse(data=ttl, format="turtle")
        g.serialize(destination=output_file,format="turtle")
        
    else:
        print('Error, wrong format')
        sys.exit()
#========================================================================================================================================= 
#--end: Convert JSON to RDF---------------------------------------------------------------------------------------------------------
#========================================================================================================================================= 


######################################
#--------------- Main ---------------#
######################################


if __name__ == "__main__":

    # Global Path
    current_path = os.getcwd()
    folder_data = 'Demo007'
    path_data = os.path.join(current_path,'data', folder_data)

    # Arguments
    args = sys.argv

    if len(args) != 4:
        print('Error in arguments')
        sys.exit()
    else:
        # File name
        JSON_data = args[1]
        RML_file = args[2]
        output_format = args[3] 

        path_output = os.path.join(path_data,JSON_data.split('.')[0] + '_output.' + output_format.lower())
        path_Json_data = os.path.join(path_data, JSON_data)
        path_RML_file = os.path.join(path_data, RML_file)
        
        # Checks
        if not os.path.exists(path_Json_data):
            print('Error, wrong JSON file')
            sys.exit()
        elif not os.path.exists(path_Json_data):
            print('Error, wrong Mapping file')
            sys.exit()
        else:
            # Convert data to RDF
            harmonizer(path_Json_data,path_RML_file,output_format,path_output)



