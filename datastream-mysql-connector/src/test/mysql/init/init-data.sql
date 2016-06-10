CREATE DATABASE test1;
USE test1;

CREATE TABLE testTable1 (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512)
);

INSERT INTO testTable1(name, description)
VALUES ("name1","description1"),
       ("name2","description2"),
       ("name3","description3"),
       ("name4","description4"),
       ("name5","description5"),
       ("name6","description6"),
       ("name7","description7"),
       ("name8","description8");