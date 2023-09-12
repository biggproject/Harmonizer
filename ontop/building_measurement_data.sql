-- Company datas --
INSERT INTO company(id,name,created_date,last_updated) values(1,'CSTB', now(), now());
-- Site datas --
INSERT INTO site(id,name,created_date,last_updated, company_id) values(1,'Sophia Antipolis', now(), now(),1);
INSERT INTO site(id,name,created_date,last_updated, company_id) values(2,'Grenoble', now(), now(),1);
INSERT INTO site(id,name,created_date,last_updated, company_id) values(3,'Paris', now(), now(),1);
INSERT INTO site(id,name,created_date,last_updated, company_id) values(4,'Nantes', now(), now(),1);

-- Building datas --
INSERT INTO building(id,name,created_date,last_updated, site_id) values(1,'BA', now(), now(),1);
INSERT INTO building(id,name,created_date,last_updated, site_id) values(2,'BB', now(), now(),1);
INSERT INTO building(id,name,created_date,last_updated, site_id) values(3,'BC', now(), now(),1);

INSERT INTO building(id,name,created_date,last_updated, site_id) values(4,'BA', now(), now(),2);
INSERT INTO building(id,name,created_date,last_updated, site_id) values(5,'BB', now(), now(),2);
INSERT INTO building(id,name,created_date,last_updated, site_id) values(6,'BC', now(), now(),2);

INSERT INTO building(id,name,created_date,last_updated, site_id) values(7,'BA', now(), now(),3);
INSERT INTO building(id,name,created_date,last_updated, site_id) values(8,'BB', now(), now(),3);
INSERT INTO building(id,name,created_date,last_updated, site_id) values(9,'BC', now(), now(),3);

-- Zone datas --
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(1,'ZA', now(), now(),1);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(2,'ZB', now(), now(),1);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(3,'ZC', now(), now(),1);

INSERT INTO zone(id,name,created_date,last_updated, building_id) values(4,'ZA', now(), now(),2);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(5,'ZB', now(), now(),2);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(6,'ZC', now(), now(),2);

INSERT INTO zone(id,name,created_date,last_updated, building_id) values(7,'ZA', now(), now(),3);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(8,'ZB', now(), now(),3);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(9,'ZC', now(), now(),3);

INSERT INTO zone(id,name,created_date,last_updated, building_id) values(10,'ZA', now(), now(),4);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(11,'ZB', now(), now(),4);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(12,'ZC', now(), now(),4);

INSERT INTO zone(id,name,created_date,last_updated, building_id) values(13,'ZA', now(), now(),5);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(14,'ZB', now(), now(),5);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(15,'ZC', now(), now(),5);

INSERT INTO zone(id,name,created_date,last_updated, building_id) values(16,'ZA', now(), now(),6);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(17,'ZB', now(), now(),6);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(18,'ZC', now(), now(),6);

INSERT INTO zone(id,name,created_date,last_updated, building_id) values(19,'ZA', now(), now(),7);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(20,'ZB', now(), now(),7);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(21,'ZC', now(), now(),7);

INSERT INTO zone(id,name,created_date,last_updated, building_id) values(22,'ZA', now(), now(),8);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(23,'ZB', now(), now(),8);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(24,'ZC', now(), now(),8);

INSERT INTO zone(id,name,created_date,last_updated, building_id) values(25,'ZA', now(), now(),9);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(26,'ZB', now(), now(),9);
INSERT INTO zone(id,name,created_date,last_updated, building_id) values(27,'ZC', now(), now(),9);

-- Building Space datas --
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(1,'BSA', now(), now(),1);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(2,'BSB', now(), now(),1);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(3,'BSA', now(), now(),2);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(4,'BSB', now(), now(),2);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(5,'BSA', now(), now(),3);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(6,'BSB', now(), now(),3);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(7,'BSA', now(), now(),4);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(8,'BSB', now(), now(),4);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(9,'BSA', now(), now(),5);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(10,'BSB', now(), now(),5);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(11,'BSA', now(), now(),6);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(12,'BSB', now(), now(),6);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(13,'BSA', now(), now(),7);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(14,'BSB', now(), now(),7);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(15,'BSA', now(), now(),8);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(16,'BSB', now(), now(),8);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(17,'BSA', now(), now(),9);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(18,'BSB', now(), now(),9);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(19,'BSA', now(), now(),10);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(20,'BSB', now(), now(),10);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(21,'BSA', now(), now(),11);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(22,'BSB', now(), now(),11);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(23,'BSA', now(), now(),12);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(24,'BSB', now(), now(),12);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(25,'BSA', now(), now(),13);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(26,'BSB', now(), now(),13);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(27,'BSA', now(), now(),14);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(28,'BSB', now(), now(),14);

INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(29,'BSA', now(), now(),15);
INSERT INTO building_space(id,name,created_date,last_updated, zone_id) values(30,'BSB', now(), now(),15);

-- Sensor datas --
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(1,'Sensor A', 'measurement_1',now(), now(),1);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(2,'Sensor B', 'measurement_2',now(), now(),1);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(3,'Sensor A', 'measurement_1',now(), now(),2);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(4,'Sensor B', 'measurement_2',now(), now(),2);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(5,'Sensor A', 'measurement_1',now(), now(),3);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(6,'Sensor B', 'measurement_2',now(), now(),3);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(7,'Sensor A', 'measurement_1',now(), now(),4);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(8,'Sensor B', 'measurement_2',now(), now(),4);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(9,'Sensor A', 'measurement_1',now(), now(),5);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(10,'Sensor B', 'measurement_2',now(), now(),5);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(11,'Sensor A', 'measurement_1',now(), now(),6);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(12,'Sensor B', 'measurement_2',now(), now(),6);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(13,'Sensor A', 'measurement_1',now(), now(),7);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(14,'Sensor B', 'measurement_2',now(), now(),7);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(15,'Sensor A', 'measurement_1',now(), now(),8);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(16,'Sensor B', 'measurement_2',now(), now(),8);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(17,'Sensor A', 'measurement_1',now(), now(),9);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(18,'Sensor B', 'measurement_2',now(), now(),9);

INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(19,'Sensor A', 'measurement_1',now(), now(),10);
INSERT INTO sensor(id,name,measurement,created_date,last_updated, building_space_id) values(20,'Sensor B', 'measurement_2',now(), now(),10);