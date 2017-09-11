IF OBJECT_ID('tempdb..#TEMP') IS NOT NULL
	DROP TABLE #TEMP

CREATE TABLE #TEMP(
	ID INT IDENTITY(1,1) PRIMARY KEY
	, TABLE_NAME VARCHAR(50)
)


INSERT INTO #TEMP (TABLE_NAME)
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE '%DIM%'

DECLARE @START INT = 1
DECLARE @END INT = (SELECT MAX(ID) FROM #TEMP)
DECLARE @SQL VARCHAR(MAX)
DECLARE @MIN INT
DECLARE @MAX  INT
DECLARE @SQL_2 VARCHAR(MAX)


WHILE(@START <= @END)
BEGIN

	DECLARE @TABLE_NAME VARCHAR(50) = (SELECT TABLE_NAME FROM #TEMP WHERE ID = @START)

	--CREATE TABLE #TABLE_NAME
	IF OBJECT_ID('tempdb..#SQL') IS NOT NULL
	DROP TABLE #SQL

	SELECT    ORDINAL_POSITION AS ID
			, COLUMN_NAME
			, DATA_TYPE
			, CHARACTER_MAXIMUM_LENGTH 
			, CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN COLUMN_NAME + '_ ' +  DATA_TYPE
				ELSE (COLUMN_NAME + '_ ' +  DATA_TYPE + '(' + CONVERT(VARCHAR(3), CHARACTER_MAXIMUM_LENGTH) + ')')
				END AS SQL
	INTO #SQL
	FROM INFORMATION_SCHEMA.COLUMNS 
	WHERE TABLE_NAME = @TABLE_NAME


	SET @MAX = (SELECT MAX(ID) FROM #SQL)
	SET @MIN = 1

	SET @SQL_2 =  'INSERT INTO #' + @TABLE_NAME + '( '
	SET @SQL = 'CREATE TABLE #' + @TABLE_NAME + '( '

		WHILE (@MIN <= @MAX)
		BEGIN
			SET @SQL = @SQL + ', ' +  (SELECT SQL FROM #SQL WHERE ID = @MIN)
			SET @SQL_2 = @SQL_2 + ', ' + (SELECT COLUMN_NAME FROM #SQL WHERE ID = @MIN) + '_'

			SET @MIN = @MIN + 1
			IF @MIN > @MAX
				BREAK
		END
	-- DONE---


	SET @SQL =  @SQL + ') '
	SET @SQL =  REPLACE (@SQL, '( ,', '(')

	SET @SQL_2 = @SQL_2 + ') ' + ' SELECT * FROM ' + @TABLE_NAME
	SET @SQL_2 = REPLACE(@SQL_2, '( ,', '(')

	PRINT(@SQL)
	PRINT(@SQL_2)


	SET @START = @START + 1
	IF @START = @END
		BREAK
END


insert into OPENROWSET('Microsoft.ACE.OLEDB.15.0', 
    'Excel 15.0;Database=D:\testing.xlsx;', 
    'SELECT * FROM [DimBranch$]') 
select * 
from PVCom..DimBranch


Exec sp_configure 'show advanced options', 1;
RECONFIGURE;
GO

Exec sp_configure 'Ad Hoc Distributed Queries', 1;
RECONFIGURE;
GO

EXEC master.dbo.sp_MSset_oledb_prop N'Microsoft.ACE.OLEDB.12.0' , N'AllowInProcess' , 1; 
GO
EXEC master.dbo.sp_MSset_oledb_prop N'Microsoft.ACE.OLEDB.12.0' , N'DynamicParameters' , 1;
GO

Insert into OPENDATASOURCE('Microsoft.ACE.OLEDB.12.0','Data Source=D:\testing.xls;Extended Properties=Excel 10.0')...[DimBranch$]
select * 
from PVCom..DimBranch

