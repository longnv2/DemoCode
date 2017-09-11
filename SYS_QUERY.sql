declare @s table(spid smallint,login_time datetime,last_batch datetime,[status] nchar(30),loginame nchar(128),[text] text)

declare @sql_handle binary(20),@spid smallint;
declare c1 cursor for select sql_handle,spid from master..sysprocesses where spid >50;
open c1;
fetch next from c1 into @sql_handle,@spid; 
while (@@FETCH_STATUS =0) 
begin 
		insert into @s
	select spid,login_time,last_batch,[status],loginame,a.text
	from ::fn_get_sql(@sql_handle) a, master..sysprocesses b
	where b.spid = @spid
	fetch next from c1 into @sql_handle,@spid
end 
close c1
deallocate c1;

select * from @s order by last_batch desc


SELECT deqs.last_execution_time AS [Time]
	, dest.TEXT AS [Query] 
	, *
FROM sys.dm_exec_query_stats AS deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
where convert(varchar(8), deqs.last_execution_time, 112) = '20170607'
ORDER BY deqs.last_execution_time DESC

----------------------------------------------------
USE master
go
SELECT sdest.DatabaseName 
    ,sdes.session_id
    ,sdes.[host_name]
    ,sdes.[program_name]
    ,sdes.client_interface_name
    ,sdes.login_name
    ,sdes.login_time
    ,sdes.nt_domain
    ,sdes.nt_user_name
    ,sdec.client_net_address
    ,sdec.local_net_address
    ,sdest.ObjName
    ,sdest.Query
FROM sys.dm_exec_sessions AS sdes
INNER JOIN sys.dm_exec_connections AS sdec ON sdec.session_id = sdes.session_id
CROSS APPLY (
    SELECT db_name(dbid) AS DatabaseName
        ,object_id(objectid) AS ObjName
        ,ISNULL((
                SELECT TEXT AS [processing-instruction(definition)]
                FROM sys.dm_exec_sql_text(sdec.most_recent_sql_handle)
                FOR XML PATH('')
                    ,TYPE
                ), '') AS Query

    FROM sys.dm_exec_sql_text(sdec.most_recent_sql_handle)
    ) sdest
where sdes.session_id <> @@SPID 
--and sdes.nt_user_name = '' -- Put the username here !
ORDER BY sdes.last_request_start_time
------------------------------------------------------------------------------

SELECT spid, 
       loginame, 
       status, 
       cmd, 
       program_name, 
       hostname, 
       login_time, 
       last_batch, 
       (SELECT text 
        FROM   sys.Dm_exec_sql_text(sql_handle))AS SQLH 
		, *
FROM   master.dbo.sysprocesses WITH (nolock) 
WHERE  loginame <> 'sa' 
       AND program_name LIKE '%Studio%' 

----------------------------------------------------------

SELECT        SQLTEXT.text, STATS.last_execution_time, *
FROM          sys.dm_exec_query_stats STATS
CROSS APPLY   sys.dm_exec_sql_text(STATS.sql_handle) AS SQLTEXT
--WHERE         STATS.last_execution_time > GETDATE()-1
ORDER BY      STATS.last_execution_time DESC


SELECT      c.session_id, s.host_name, s.login_name, s.status, st.text, s.login_time, s.program_name, *
FROM        sys.dm_exec_connections c
INNER JOIN  sys.dm_exec_sessions s ON c.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(most_recent_sql_handle) AS st
ORDER BY    c.session_id

-----------------------------------------------------------

SELECT * FROM ::fn_trace_getinfo(default) 

SELECT    TextData
		,  DatabaseID
		, TransactionID
		, NTDomainName
		, HostName
		, ApplicationName
		, LoginName
		, StartTime
		, EndTime
		, CPU
		, ServerName
		, EventClass
		, DatabaseName
		, DBUserName
		, SessionLoginName
		, PlanHandle
FROM fn_trace_gettable('C:\Program Files\Microsoft SQL Server\MSSQL13.PVB_REPORT\MSSQL\Log\log_22.trc', default)
where convert(varchar(8), starttime, 112) = '20170607'-- and LoginName <> 'sa'
order by StartTime desc


SELECT  d.plan_handle ,
        d.sql_handle ,
        e.text
FROM    sys.dm_exec_query_stats d
        CROSS APPLY sys.dm_exec_sql_text(d.plan_handle) AS e

------------------------------------------------------------------------------

SELECT
    deqs.last_execution_time AS [Time], 
    dest.TEXT AS [Query],
	deqs.sql_handle as dsql

	 , *
 FROM  sys.dm_exec_query_stats AS deqs
 CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
 --left join sys.sysprocesses AS a
 --on a.sql_handle = deqs.sql_handle
	--CROSS APPLY master.dbo.sysprocesses AS
ORDER BY Time DESC

select * FROM   sys.sysprocesses order by login_time desc



SELECT * 
FROM 
   sys.dm_exec_sessions s
   LEFT  JOIN sys.dm_exec_connections c
        ON  s.session_id = c.session_id
   LEFT JOIN sys.dm_db_task_space_usage tsu
        ON  tsu.session_id = s.session_id
   LEFT JOIN sys.dm_os_tasks t
        ON  t.session_id = tsu.session_id
        AND t.request_id = tsu.request_id
   LEFT JOIN sys.dm_exec_requests r
        ON  r.session_id = tsu.session_id
        AND r.request_id = tsu.request_id
   OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) TSQL

   SELECT *
   FROM  sys.dm_exec_query_stats

