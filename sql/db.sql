-- ----------------------------------------
-- Create Database
-- ----------------------------------------

CREATE DATABASE eReserveData;
GO

-- ----------------------------------------
-- Use Database
-- ----------------------------------------

USE eReserveData;
GO

-- ----------------------------------------
-- Table: IntegrationUser
-- ----------------------------------------

CREATE TABLE IntegrationUser (
    ereserve_id INT PRIMARY KEY NOT NULL,
    identifier NVARCHAR(255) NOT NULL,
    roles NVARCHAR(255) NOT NULL,
    first_name NVARCHAR(255) NOT NULL,
    last_name NVARCHAR(255) NOT NULL,
    email NVARCHAR(100) NOT NULL,
    lti_consumer_user_id NVARCHAR(255) NOT NULL,
    lti_lis_person_sourcedid NVARCHAR(255) NOT NULL,
    created_at DATETIME,
    updated_at DATETIME
);
GO

-- ----------------------------------------
-- Table: School
-- ----------------------------------------

CREATE TABLE School (
    ereserve_id INT PRIMARY KEY NOT NULL,
    name NVARCHAR(255) NOT NULL
);
GO

-- ----------------------------------------
-- Table: Unit
-- ----------------------------------------

CREATE TABLE Unit (
    ereserve_id INT PRIMARY KEY NOT NULL,
    code NVARCHAR(50) NOT NULL,
    name NVARCHAR(255) NOT NULL
);
GO

-- ----------------------------------------
-- Table: TeachingSession
-- ----------------------------------------

CREATE TABLE TeachingSession (
    ereserve_id INT PRIMARY KEY NOT NULL,
    name NVARCHAR(255),
    start_date DATE,
    end_date DATE,
    archived BIT DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME
);
GO

-- ----------------------------------------
-- Table: Reading
-- ----------------------------------------

CREATE TABLE Reading (
    ereserve_id INT PRIMARY KEY NOT NULL,
    reading_title NVARCHAR(200),
    genre NVARCHAR(100),
    source_document_title NVARCHAR(100),
    article_number NVARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME
);
GO

-- ----------------------------------------
-- Table: ReadingList
-- ----------------------------------------

CREATE TABLE ReadingList (
    ereserve_id INT PRIMARY KEY NOT NULL,
    unit_id INT,
    teaching_session_id INT,
    name NVARCHAR(255) NOT NULL,
    duration NVARCHAR(50),
    start_date DATE,
    end_date DATE,
    hidden BIT DEFAULT 0,
    usage_count BIGINT,
    item_count BIGINT,
    approved_item_count BIGINT,
    deleted BIT DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME,
    CONSTRAINT FK_ReadingList_Unit FOREIGN KEY (unit_id) REFERENCES Unit (ereserve_id),
    CONSTRAINT FK_ReadingList_TeachingSession FOREIGN KEY (teaching_session_id) REFERENCES TeachingSession (ereserve_id)
);
GO

-- ----------------------------------------
-- Table: ReadingListUsage
-- ----------------------------------------

CREATE TABLE ReadingListUsage (
    ereserve_id INT PRIMARY KEY NOT NULL,
    list_id INT NOT NULL,
    integration_user_id INT NOT NULL,
    item_usage_count BIGINT DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME,
    CONSTRAINT FK_ReadingListusage_List FOREIGN KEY (list_id) REFERENCES ReadingList (ereserve_id),
    CONSTRAINT FK_ReadingListusage_IntegrationUser FOREIGN KEY (integration_user_id) REFERENCES IntegrationUser (ereserve_id)
);
GO

-- ----------------------------------------
-- Table: ReadingListItem
-- ----------------------------------------

CREATE TABLE ReadingListItem (
    ereserve_id INT PRIMARY KEY NOT NULL,
    list_id INT NOT NULL,
    reading_id INT NOT NULL,
    status NVARCHAR(50),
    hidden BIT DEFAULT 0,
    reading_utilisations_count BIGINT DEFAULT 0,
    reading_importance NVARCHAR(100),
    usage_count BIGINT DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME,
    CONSTRAINT FK_ReadingListItem_ReadingList FOREIGN KEY (list_id) REFERENCES ReadingList (ereserve_id),
    CONSTRAINT FK_ReadingListItem_Reading FOREIGN KEY (reading_id) REFERENCES Reading (ereserve_id)
);
GO

-- ----------------------------------------
-- Table: UnitOffering
-- ----------------------------------------

CREATE TABLE UnitOffering (
    ereserve_id INT PRIMARY KEY NOT NULL,
    unit_id INT NOT NULL,
    reading_list_id INT NOT NULL,
    source_unit_code NVARCHAR(100),
    source_unit_name NVARCHAR(255),
    source_unit_offering NVARCHAR(100),
    result NVARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME,
    CONSTRAINT FK_UnitOffering_Unit FOREIGN KEY (unit_id) REFERENCES Unit (ereserve_id),
    CONSTRAINT FK_UnitOffering_ReadingList FOREIGN KEY (reading_list_id) REFERENCES ReadingList (ereserve_id)
);
GO

-- ----------------------------------------
-- Table: ReadingListItemUsage
-- ----------------------------------------

CREATE TABLE ReadingListItemUsage (
    ereserve_id INT PRIMARY KEY NOT NULL,
    item_id INT NOT NULL,
    list_usage_id INT NOT NULL,
    integration_user_id INT NOT NULL,
    utilisation_count BIGINT DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME,
    CONSTRAINT FK_ReadingListItemusage_ListItem FOREIGN KEY (item_id) REFERENCES ReadingListItem (ereserve_id),
    CONSTRAINT FK_ReadingListItemusage_ListUsage FOREIGN KEY (list_usage_id) REFERENCES ReadingListUsage (ereserve_id),
    CONSTRAINT FK_ReadingListItemusage_IntegrationUser FOREIGN KEY (integration_user_id) REFERENCES IntegrationUser (ereserve_id)
);
GO

-- ----------------------------------------
-- Table: ReadingUtilisation
-- ----------------------------------------

CREATE TABLE ReadingUtilisation (
    ereserve_id INT PRIMARY KEY NOT NULL,
    integration_user_id INT NOT NULL,
    item_id INT NOT NULL,
    item_usage_id INT NOT NULL,
    created_at DATETIME,
    updated_at DATETIME,
    CONSTRAINT FK_ReadingUtilisation_ListItem FOREIGN KEY (item_id) REFERENCES ReadingListItem (ereserve_id),
    CONSTRAINT FK_ReadingUtilisation_ListItemUsage FOREIGN KEY (item_usage_id) REFERENCES ReadingListItemUsage (ereserve_id),
    CONSTRAINT FK_ReadingUtilisation_IntegrationUser FOREIGN KEY (integration_user_id) REFERENCES IntegrationUser (ereserve_id)
);
GO
