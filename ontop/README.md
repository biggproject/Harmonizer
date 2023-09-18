## BiGG Ontop Demo

**Done by the CSTB**
***OBJECTIVE***
The objective of this application is to demonstrate the ONTOP library which allows you to implement the transition from a relational data model to a graph data model.
Ontop is an open source OBDA( ONTOLOGY BASED DATA ACCESS) framewok supports all of the main W3C recommandations: OWL, R2RML, SPARQL, SWRL etc...
on the one hand, on the other hand it supports  the major of relational database that  implements SQL 99 like MYSQL, PROSTRESQL, ORACLE, MS SQL SERVER etc...

The implementation of this application is based on a POSTGRESQL Database. We use a simple building measurement data model to illustrate the implementation (cf building_measurement.png). 

The Framework has four keys concepts:

1. The Mapping: Implemented in the building_measurement.obda  file in our case
2. The Ontology : Implemented in the building_measurement.ttl file in our case
3. The Data Source  : Implemented in the building_measurement.properties in our case
4. The Query : building_measurement.sql and building_measurement_data.sql in our case

***Description***