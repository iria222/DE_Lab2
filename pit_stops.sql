
CREATE DATABASE IF NOT EXISTS pit_stops_db;
USE pit_stops_db;

CREATE TABLE IF NOT EXISTS driver (
  driver_id INT AUTO_INCREMENT PRIMARY KEY,
  driver_name VARCHAR(255),
  driver_surname VARCHAR(255),
  driver_nationality VARCHAR(255),
  date_of_birth DATETIME
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS race (
  race_id INT AUTO_INCREMENT PRIMARY KEY,
  year INT,
  month INT,
  day INT,
  race_name VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pit_stops (
  pit_stops_id INT AUTO_INCREMENT PRIMARY KEY,
  driver_id INT,
  race_id INT,

  stop_number INT,
  lap_number INT,
  stop_time TIME,
  stop_duration INT,

  FOREIGN KEY(driver_id) REFERENCES driver(driver_id),
  foreign key(race_id) references race(race_id)
) ENGINE=InnoDB;



SELECT * from pit_stops;
SELECT * from driver;
SELECT * from race;






