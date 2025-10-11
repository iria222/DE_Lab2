
CREATE DATABASE IF NOT EXISTS formula1_db;
USE formula1_db;

CREATE TABLE IF NOT EXISTS driver (
  driver_id INT AUTO_INCREMENT PRIMARY KEY,
  driver_name VARCHAR(255),
  driver_surname VARCHAR(255),
  driver_nationality VARCHAR(255),
  date_of_birth DATETIME
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS constructor (
  constructor_id INT AUTO_INCREMENT PRIMARY KEY,
  constructor_name VARCHAR(255),
  constructor_nationality VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS race (
  race_id INT AUTO_INCREMENT PRIMARY KEY,
  year INT,
  month INT,
  day INT,
  race_name VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS circuit (
  circuit_id INT AUTO_INCREMENT PRIMARY KEY,
  circuit_name VARCHAR(255),
  circuit_location VARCHAR(255),
  circuit_country VARCHAR(255),
  longitude FLOAT,
  latitude FLOAT,
  altitude FLOAT
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS qualifying (
  qualifying_id INT AUTO_INCREMENT PRIMARY KEY,
  circuit_id INT,
  constructor_id INT,
  race_id INT,
  driver_id INT,


  q1 VARCHAR(20),
  q2 VARCHAR(20),
  q3 VARCHAR(20),
  
  foreign key(circuit_id) references circuit(circuit_id),
  foreign key(constructor_id) references constructor(constructor_id),
  foreign key(race_id) references race(race_id),
  foreign key(driver_id) references driver(driver_id)

) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pit_stops (
  pit_stops_id INT AUTO_INCREMENT PRIMARY KEY,
  driver_id INT,
  race_id INT,
  constructor_id INT,

  stop_number INT,
  lap_number INT,
  stop_time TIME,
  stop_duration INT,

  FOREIGN KEY(driver_id) REFERENCES driver(driver_id),
  foreign key(constructor_id) references constructor(constructor_id),
  foreign key(race_id) references race(race_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS status (
  status_id INT AUTO_INCREMENT PRIMARY KEY,
  status VARCHAR(255)

) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS results (
  result_id INT AUTO_INCREMENT PRIMARY KEY,
  circuit_id INT,
  constructor_id INT,
  race_id INT,
  driver_id INT,
  status_id INT,

  car_number INT,
  starting_position INT,
  final_position INT,
  position_order INT,
  points INT,
  laps INT,
  
  foreign key(circuit_id) references circuit(circuit_id),
  foreign key(constructor_id) references constructor(constructor_id),
  foreign key(race_id) references race(race_id),
  foreign key(driver_id) references driver(driver_id),
  foreign key(status_id) references status(status_id)


) ENGINE=InnoDB;
