USE [master]
GO

EXEC sp_configure 'Show Advanced Options', 1
RECONFIGURE
GO

EXEC sp_configure 'Ad Hoc Distributed Queries', 1
RECONFIGURE
GO

EXEC sp_MSSet_oledb_prop N'Microsoft.ACE.OLEDB.12.0', N'AllowInProcess', 1
GO

EXEC sp_MSSet_oledb_prop N'Microsoft.ACE.OLEDB.12.0', N'DynamicParameters', 1 
GO


SELECT * --INTO #TargetTableName 
FROM OPENROWSET('Microsoft.ACE.OLEDB.12.0',
				'Excel 12.0;Database=E:\xln20170417.xlsx;HDR=YES',
                'SELECT * FROM [XLN20170417$]')
