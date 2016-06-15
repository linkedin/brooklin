CREATE DATABASE testdb1;
USE testdb1;

CREATE TABLE testTable1 (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512)
);

-- Two transaction with 4 insert events each in TestTable1
INSERT INTO testTable1(name, description)
VALUES ("name1","description1"),
       ("name2","description2"),
       ("name3","description3"),
       ("name4","description4");

INSERT INTO testTable1(name, description)
VALUES ("name5","description5"),
       ("name6","description6"),
       ("name7","description7"),
       ("name8","description8");


CREATE TABLE testTable2 (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512)
);

-- One transaction with 6 insert events in testTable2
INSERT INTO testTable2(name, description)
VALUES ("name1","description1"),
       ("name2","description2"),
       ("name3","description3"),
       ("name4","description4"),
       ("name5","description5"),
       ("name6","description6");


-- One transaction with 1 insert event in testTable1
UPDATE testTable1 set name="newname4" where name="name4";

-- One transaction with 1 delete event in testTable2
delete from testTable2 where name = "name6"; 