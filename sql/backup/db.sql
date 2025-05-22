CREATE DATABASE eReserveData;

USE eReserveData;

Create TABLE ResourceData (
    id INT PRIMARY KEY IDENTITY(1,1),
	title NVARCHAR(100),
	res_description NVARCHAR(500),
	access_count INT,
	student_count INT,
	resource_id NVARCHAR(100)
);