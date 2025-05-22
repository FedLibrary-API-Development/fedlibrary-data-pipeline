-- Ensure you're using the correct database
USE eReserveData;
GO

-- Delete from dependent (child) tables first to avoid FK constraint errors
DELETE FROM dbo.ReadingUtilisation;
DELETE FROM dbo.ReadingListItemUsage;
DELETE FROM dbo.ReadingListUsage;
DELETE FROM dbo.UnitOffering;
DELETE FROM dbo.ReadingListItem;
DELETE FROM dbo.ReadingList;
DELETE FROM dbo.Unit;
DELETE FROM dbo.Reading;
DELETE FROM dbo.TeachingSession;
DELETE FROM dbo.School;
DELETE FROM dbo.IntegrationUser;
