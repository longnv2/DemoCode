   SELECT
    der.session_id
    ,est.TEXT AS QueryText
    ,der.status
    ,der.blocking_session_id
    ,der.cpu_time
    ,der.total_elapsed_time
FROM sys.dm_exec_requests AS der
CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS est


--CLEAR CACHE SQLSERVER
CHECKPOINT; 
GO 
DBCC DROPCLEANBUFFERS; 
GO
