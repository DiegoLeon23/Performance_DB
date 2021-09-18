--Crear la BD para el demo 
CREATE DATABASE DBPartition
go
use DBPartition
go

--Crear la tabla de carga
CREATE TABLE tbVisitors
(id INT IDENTITY(1,1) PRIMARY KEY
, name VARCHAR(50)
, date DATE)
GO


-- Poblando la tabla Global
INSERT INTO tbVisitors(name,date)
VALUES(newid(),
convert(date,convert(varchar(15),'2013-' + 
convert(varchar(5), (convert(int,rand()*12))+1) + '-' +
convert(varchar(5),(convert(int,rand()*27))+1))))
GO 10000 ---10000 registros

---Setup de la tabla particionada 
---Crear los filegroups
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_ENE
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_FEB
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_MAR
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_ABR
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_MAY
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_JUN
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_JUL
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_AGO
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_SET
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_OCT
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_NOV
ALTER DATABASE DBPartition ADD FILEGROUP FG_2013_DIC
ALTER DATABASE DBPartition ADD FILEGROUP FG_OTROS
GO
-- VER LOS FILEGROUPS
SP_HELPFILEGROUP
GO
-- VER LOS FILES ASOCIADOS A LOS FILEGROUPS
SP_HELPFILE

CREATE PARTITION FUNCTION VisitasPorMes(date)
AS
RANGE RIGHT FOR VALUES 
('20130101', '20130201', '20130301',
 '20130401', '20130501', '20130601',
 '20130701', '20130801', '20130901',
 '20131001', '20131101', '20131201',
 '20140101')
GO

-- Crear los esquemas de particion
CREATE PARTITION SCHEME VisitantesFileGroup
AS
PARTITION VisitasPorMes TO
( [PRIMARY],
  [FG_2013_ENE],[FG_2013_FEB],[FG_2013_MAR],
  [FG_2013_ABR],[FG_2013_MAY],[FG_2013_JUN],
  [FG_2013_JUL],[FG_2013_AGO],[FG_2013_SET],
  [FG_2013_OCT],[FG_2013_NOV],[FG_2013_DIC],
  [FG_OTROS] )
 GO

   -- Crear la tabla Particionada
CREATE TABLE Visitantes(
	[id] [int] IDENTITY(1,1) NOT NULL,
	[name] [varchar](50) NULL,
	[date] date NULL
) ON VisitantesFileGroup([date])

select * from tbVisitors
select * from Visitantes

-- Insertar los datos de la tabla Global a la tabla con particion *MOVE*
INSERT INTO Visitantes(name, [date])
SELECT name, [date] FROM tbVisitors
GO

-- Ver los datos de las Particiones de la tabla

SELECT
  OBJECT_SCHEMA_NAME(pstats.object_id) AS SchemaName
  ,OBJECT_NAME(pstats.object_id) AS TableName
  ,ps.name AS PartitionSchemeName
  ,ds.name AS PartitionFilegroupName
  ,pf.name AS PartitionFunctionName
  ,CASE pf.boundary_value_on_right WHEN 0 THEN 'Range Left' ELSE 'Range Right' END AS PartitionFunctionRange
  ,CASE pf.boundary_value_on_right WHEN 0 THEN 'Upper Boundary' ELSE 'Lower Boundary' END AS PartitionBoundary
  ,prv.value AS PartitionBoundaryValue
  ,c.name AS PartitionKey
  ,CASE 
    WHEN pf.boundary_value_on_right = 0 
    THEN c.name + ' > ' + CAST(ISNULL(LAG(prv.value) OVER(PARTITION BY pstats.object_id ORDER BY pstats.object_id, pstats.partition_number), 'Infinity') AS VARCHAR(100)) + ' and ' + c.name + ' <= ' + CAST(ISNULL(prv.value, 'Infinity') AS VARCHAR(100)) 
    ELSE c.name + ' >= ' + CAST(ISNULL(prv.value, 'Infinity') AS VARCHAR(100))  + ' and ' + c.name + ' < ' + CAST(ISNULL(LEAD(prv.value) OVER(PARTITION BY pstats.object_id ORDER BY pstats.object_id, pstats.partition_number), 'Infinity') AS VARCHAR(100))
  END AS PartitionRange
  ,pstats.partition_number AS PartitionNumber
  ,pstats.row_count AS PartitionRowCount
  ,p.data_compression_desc AS DataCompression
FROM sys.dm_db_partition_stats AS pstats
INNER JOIN sys.partitions AS p ON pstats.partition_id = p.partition_id
INNER JOIN sys.destination_data_spaces AS dds ON pstats.partition_number = dds.destination_id
INNER JOIN sys.data_spaces AS ds ON dds.data_space_id = ds.data_space_id
INNER JOIN sys.partition_schemes AS ps ON dds.partition_scheme_id = ps.data_space_id
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.indexes AS i ON pstats.object_id = i.object_id AND pstats.index_id = i.index_id AND dds.partition_scheme_id = i.data_space_id AND i.type <= 1 /* Heap or Clustered Index */
INNER JOIN sys.index_columns AS ic ON i.index_id = ic.index_id AND i.object_id = ic.object_id AND ic.partition_ordinal > 0
INNER JOIN sys.columns AS c ON pstats.object_id = c.object_id AND ic.column_id = c.column_id
LEFT JOIN sys.partition_range_values AS prv ON pf.function_id = prv.function_id AND pstats.partition_number = (CASE pf.boundary_value_on_right WHEN 0 THEN prv.boundary_id ELSE (prv.boundary_id+1) END)
WHERE pstats.object_id = OBJECT_ID('Visitantes')
ORDER BY TableName, PartitionNumber;

-- Comparar Performance
SET STATISTICS IO, TIME ON
GO
SELECT name, date FROM Visitantes WHERE [date] = '2013-01-15'
SELECT name, date FROM tbVisitors WHERE [date] = '2013-01-15'
GO
SET STATISTICS IO, TIME OFF

--/////////////////////////////////////////////////// TEMP

----------------------------------------------------------
-- Lecturas Tabla Temporal
----------------------------------------------------------
select 
  DB_NAME(mf.database_id) ,sum(fs.num_of_reads) as total_reads
from sys.master_files mf
cross apply sys.dm_io_virtual_file_stats(mf.database_id,NULL) fs
where mf.database_id = 2 and mf.type_desc = 'ROWS' group by mf.database_id
GO
CREATE TABLE #temp1 (col1 int)
insert into #temp1 select database_id from sys.databases
DBCC DROPCLEANBUFFERS --limpieza de buffer (obligar que lo que habia en memoria se vaya)
select count(*) from #temp1
DROP TABLE #temp1 
GO
select DB_NAME(mf.database_id) ,sum(fs.num_of_reads) as total_reads
from sys.master_files mf
cross apply sys.dm_io_virtual_file_stats(mf.database_id,NULL) fs
where mf.database_id = 2 and mf.type_desc = 'ROWS' group by mf.database_id
GO

----------------------------------------------------------
-- Lecturas de Variable
----------------------------------------------------------
select 
  DB_NAME(mf.database_id),sum(fs.num_of_reads) as total_reads
from sys.master_files mf
cross apply sys.dm_io_virtual_file_stats(mf.database_id,NULL) fs
where mf.database_id = 2 and mf.type_desc = 'ROWS' group by mf.database_id
GO
DECLARE @temp1 TABLE (col1 int)
insert into @temp1 select database_id from sys.databases
DBCC DROPCLEANBUFFERS
select count(*) from @temp1
GO
select 
  DB_NAME(mf.database_id) ,sum(fs.num_of_reads) as total_reads
from sys.master_files mf
cross apply sys.dm_io_virtual_file_stats(mf.database_id,NULL) fs
where mf.database_id = 2 and mf.type_desc = 'ROWS' group by mf.database_id
GO

