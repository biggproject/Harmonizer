## BiGG Harmonizer Project 

**Done by the CSTB**

The aim of the BiGG Harmonizer tool is to convert data from JSON to **RDF** (Turtle).  
The BiGG harmonizer tool is developped in Python, and it uses 2 modules to convert and to align data from Json file.  

To execute the module, you will have to complete the following command :  
python harmonizer.py --input [input file json or ttl] --mapping [mapping file rml] --sparql [query files] --output [output file ttl]

The creation of the RML mapping file need to be completed by an adjustment of the file to use the Python module. 
Indeed, the name of the source need to be modified as '\_\_SOURCE__' in the Matey Yarrml rules before to export the RML mapping file. 
