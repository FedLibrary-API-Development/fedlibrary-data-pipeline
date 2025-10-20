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
    identifier NVARCHAR(255),
    roles NVARCHAR(255),
    first_name NVARCHAR(255),
    last_name NVARCHAR(255),
    email NVARCHAR(100),
    lti_consumer_user_id NVARCHAR(255),
    lti_lis_person_sourcedid NVARCHAR(255),
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
    code NVARCHAR(100) NOT NULL,
    name NVARCHAR(255) NOT NULL,
    school_id INT,
    CONSTRAINT FK_Unit_School FOREIGN KEY (school_id) REFERENCES School (ereserve_id)
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
    updated_at DATETIME,
    code VARCHAR(10)
);
GO

-- ----------------------------------------
-- Table: Reading
-- ----------------------------------------

CREATE TABLE Reading (
    ereserve_id INT PRIMARY KEY NOT NULL,
    reading_title NVARCHAR(1000),
    genre NVARCHAR(100),
    source_document_title NVARCHAR(500),
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
    list_id INT,
    integration_user_id INT,
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
    list_id INT,
    reading_id INT,
    deleted BIT DEFAULT 0,
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
    unit_id INT,
    reading_list_id INT,
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
    item_id INT,
    list_usage_id INT,
    integration_user_id INT,
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

-- ----------------------------------------
-- Table: FedUnit
-- ----------------------------------------

CREATE TABLE FedUnit(
    uc_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    unit_id INT,
    unit_code VARCHAR(50),
    is_false BIT DEFAULT 0,
    num_extracted INT,
    FOREIGN KEY (unit_id) REFERENCES Unit (ereserve_id)
);
GO

-- ----------------------------------------
-- Table: PipelineRunHistory
-- ----------------------------------------

CREATE TABLE PipelineRunHistory(
    run_id INT IDENTITY(1,1) PRIMARY KEY,
    run_start_time DATETIME NOT NULL,
    run_end_time DATETIME NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('IN_PROGRESS', 'SUCCESS', 'FAILED')),
    is_initial_load BIT NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT GETDATE(),
    INDEX IX_PipelineRunHistory_Status (status),
    INDEX IX_PipelineRunHistory_StartTime (run_start_time DESC)
);
GO