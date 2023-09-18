## BiGG Ontop Demo

**Done by the CSTB**

The objective of this application is to demonstrate the ONTOP library which allows you to implement the transition from a relational data model to a graph data model.
Ontop is an open source OBDA( ONTOLOGY BASED DATA ACCESS) framewok supports all of the main W3C recommandations: OWL, R2RML, SPARQL, SWRL etc...
on the one hand, on the other hand it supports  the major of relational database that  implements SQL 99 like MYSQL, PROSTRESQL, ORACLE, MS SQL SERVER etc...

The implementation of this application is based on a POSTGRESQL Database. We use a simple building measurement data model to illustrate the implementation (cf building_measurement.png). 

The Framework has four keys concepts:

1. The Mapping file: building_measurement.obda
2. The Ontology file: building_measurement.ttl
3. The Data Source file : building_measurement.properties
4. The Query file: building_measurement.sql and building_measurement_data.sql
