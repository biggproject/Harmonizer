## Bigg Ontop Demo- Done By CSTB

***OBJECTIVE***

The objective of this application is to demonstrate the ONTOP library which allows you to implement the transition from a relational data model to a graph data model.
Ontop is an open source OBDA( ONTOLOGY BASED DATA ACCESS) framework supports all of the main W3C recommandations: OWL, R2RML, SPARQL, SWRL etc...
on the one hand, on the other hand it supports  the major of relational database that  implements SQL 99 like MYSQL, PROSTRESQL, ORACLE, MS SQL SERVER etc...

The implementation of this application is based on a POSTGRESQL Database. We use a simple building measurement data model to illustrate the implementation (cf building_measurement.png). 

The Framework has four keys concepts:

  1. The Mapping: Implemented in the building_measurement.obda  file in our case. He materializes the link between the  relational model and rdf mapping. Each mapping declaration contains:
     1. Target : RDF Mapping -> Example: bigg:building_measurement/Company/{id} a bigg:Company ; bigg:name {name}^^xsd:string . 
     2. Source : The SQL query -> Example: SELECT * FROM "company"
  
  2. The Ontology : Implemented in the building_measurement.ttl file in our case. The file contains all the classes definition of our model.
  3. The Data Source  : Implemented in the building_measurement.properties in our case. This file contains the credentials  for the JDBC connection:
     1. The Database URL jdbc\:postgresql\://localhost\:5432/building_measurement
     2. The Database Password : jdbc.password=XXXX
     3. The Database Password : jdbc.user=XXXXX
     4. The JDBC Driver:  jdbc.driver=org.postgresql.Driver
  4. The Query : building_measurement.sql and building_measurement_data.sql in our case. These 2 SQL files contains all the DDL (Data Definition Language), DML (Data Manipulation Language) and DCL (Data Control Language)   that are necessary to build the relational database.

***Description***

The description below shows the implementation of our relational data model. The model contains these tables:

  1. Company
  2. Site
  3. Building
  4. Zone
  5. Building_Space
  6. Sensor
     
Each table has two mains fields: a name, an identifier (the primary key). The Site table contains a foreign key reference towards the Company, Building  references the Site, Zone references Building,  Building_Space references Zone and Sensor references Building_Space.

***User Guide***
In order to run this demo,  you should have these prerequisites:

  1. Install Ontop Protégé Bundle : https://sourceforge.net/projects/ontop4obda/files
  2. Download the Postgresql JDBC Driver (42.5.4 or higher): https://jdbc.postgresql.org/download/postgresql-42.5.4.jar
  3. Have access to a Postgres Database Server. You can use another RDMS (Mysql or something else) if you want, or use the h2 Protégé built-in database.

 
A. Run the Ontop Protégé by executing the run.sh script (Linux Platform) 

     1.  ~/ontop-protege-bundle-linux-5.0.2/Protege-5.6.1$./run.sh 

B. Configure the Data Source properties:

     1. Copy the JDBC driver in your  working directory: ~/jdbc/postgresql-42.5.4.jar
     2. Go to Protege Web Interface: File -> Preferences -> JDBC Drivers
     3. Browse to select the driver  Class
     4. Go to Window -> Tabs -> Onto Mappings -> Connector Parameters
          a. Connection Url : jdbc\:postgresql\://localhost\:5432/building_measurement
          b. Database Username: XXXX
          c. Database Password: XXXX
          d. JDBC Driver Class: org.postgresql.Driver

C . Load the TTL File

     1. Go to file -> Open -> building_measurement.ttl
     2. Start the reasoner
          a. Go to Reasoner -> Start Reasoner
     3.  Go to Window -> Tabs -> Onto Mappings
          a. Double click on mapping Editor to edit a triple to see target and source:
          
          For Example
          Target: bigg:building_measurement/Company/{id} a bigg:Company ; bigg:name {name}^^xsd:string . 
          Source: SELECT * FROM "company"
     4. Excute the Sql Query to see the result.