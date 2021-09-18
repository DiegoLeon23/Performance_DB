---Conociendo un poco más de los indices
---Uso de la base de datos AdventureWorksDW2016 

---Creamos un indice no clustered columnstore
CREATE NONCLUSTERED COLUMNSTORE INDEX CSI_FactInternetSales ON dbo.FactInternetSales 
(
 ProductKey,
 OrderDateKey,
 DueDateKey,
 ShipDateKey,
 CustomerKey,
 PromotionKey,
 CurrencyKey,
 SalesTerritoryKey,
 SalesOrderNumber,
 SalesOrderLineNumber,
 TotalProductCost,
 SalesAmount
)
---visualizamos las estadisticas de entrada y salida
----1. Tomando el indice creado
set statistics io on
SELECT F.SalesOrderNumber, F.OrderDateKey, F.CustomerKey, F.ProductKey, F.SalesAmount
FROM dbo.FactInternetSales AS F
INNER JOIN dbo.DimProduct AS D1 ON F.ProductKey = D1.ProductKey
INNER JOIN dbo.DimCustomer AS D2 ON F.CustomerKey = D2.CustomerKey

----2. Ignorando el indice creado
SELECT F.SalesOrderNumber, F.OrderDateKey, F.CustomerKey, F.ProductKey, F.SalesAmount
FROM dbo.FactInternetSales AS F
INNER JOIN dbo.DimProduct AS D1 ON F.ProductKey = D1.ProductKey
INNER JOIN dbo.DimCustomer AS D2 ON F.CustomerKey = D2.CustomerKey
OPTION (IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX)

--- OJO: NO puedo poner un indice COLUMNSTORE si es que mi tabla se encuentra en memoria

-- Revisando el uso de los indices
SELECT OBJECT_NAME(IX.OBJECT_ID) Table_Name
     ,IX.name AS Index_Name
     ,IX.type_desc Index_Type
     ,SUM(PS.[used_page_count]) * 8 IndexSizeKB
     ,IXUS.user_seeks AS NumOfSeeks -- numero de veces que el indice es usado
     ,IXUS.user_scans AS NumOfScans -- numero de veces que las páginas hoja en el indice son escaneadas
     ,IXUS.user_lookups AS NumOfLookups -- indica el numero de veces que un indice agrupado es usado por el indice no agrupado para buscar la fila entera 
     ,IXUS.user_updates AS NumOfUpdates -- muestra el numero de veces que la información del indice es modificada
     ,IXUS.last_user_seek AS LastSeek
     ,IXUS.last_user_scan AS LastScan
     ,IXUS.last_user_lookup AS LastLookup
     ,IXUS.last_user_update AS LastUpdate
FROM sys.indexes IX
INNER JOIN sys.dm_db_index_usage_stats IXUS ON IXUS.index_id = IX.index_id AND IXUS.OBJECT_ID = IX.OBJECT_ID
INNER JOIN sys.dm_db_partition_stats PS on PS.object_id=IX.object_id
WHERE OBJECTPROPERTY(IX.OBJECT_ID,'IsUserTable') = 1
GROUP BY OBJECT_NAME(IX.OBJECT_ID) ,IX.name ,IX.type_desc ,IXUS.user_seeks ,IXUS.user_scans 
,IXUS.user_lookups,IXUS.user_updates ,IXUS.last_user_seek ,IXUS.last_user_scan 
,IXUS.last_user_lookup ,IXUS.last_user_update

--- Fragmentacion

SELECT  OBJECT_NAME(IDX.OBJECT_ID) AS Table_Name, 
IDX.name AS Index_Name, 
IDXPS.index_type_desc AS Index_Type, 
IDXPS.avg_fragmentation_in_percent  Fragmentation_Percentage
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) IDXPS 
INNER JOIN sys.indexes IDX  ON IDX.object_id = IDXPS.object_id 
AND IDX.index_id = IDXPS.index_id 
ORDER BY Fragmentation_Percentage DESC

CREATE NONCLUSTERED INDEX IX_ProductVendor_VendorID   
    ON Purchasing.ProductVendor (BusinessEntityID);   
GO

set statistics io on
select * from [Purchasing].[ProductVendor]
where BusinessEntityID = 1678