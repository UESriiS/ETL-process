CREATE DATABASE WWI_DW_a2_work_3;
GO

USE WWI_DW_a2_work_3
GO


--Create Dimension city table
CREATE TABLE dbo.DimLocations(
	LocationKey INT NOT NULL,
	CityName NVARCHAR(50) NULL,
	StateProvCode NVARCHAR(5) NULL,
	StateProvName NVARCHAR(50) NULL,
	CountryName NVARCHAR(60) NULL,
	CountryFormalName NVARCHAR(60) NULL,
    CONSTRAINT PK_DimCities PRIMARY KEY CLUSTERED ( LocationKey )
);



--Create Dimension customer table
CREATE TABLE dbo.DimCustomers(
	CustomerKey INT NOT NULL,
	CustomerName NVARCHAR(100) NULL,
	CustomerCategoryName NVARCHAR(50) NULL,
	DeliveryCityName NVARCHAR(50) NULL,
	DeliveryStateProvCode NVARCHAR(5) NULL,
	DeliveryCountryName NVARCHAR(50) NULL,
	PostalCityName NVARCHAR(50) NULL,
	PostalStateProvCode NVARCHAR(5) NULL,
	PostalCountryName NVARCHAR(50) NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL,
    CONSTRAINT PK_DimCustomers PRIMARY KEY CLUSTERED ( CustomerKey )
);



--Create Dimension Product table
CREATE TABLE dbo.DimProducts(
	ProductKey INT NOT NULL,
	ProductName NVARCHAR(100) NULL,
	ProductColour NVARCHAR(20) NULL,
	ProductBrand NVARCHAR(50) NULL,
	ProductSize NVARCHAR(20) NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL,
    CONSTRAINT PK_DimProducts PRIMARY KEY CLUSTERED ( ProductKey )
);



--Create Dimension SalePeople table
CREATE TABLE dbo.DimSalesPeople(
	SalespersonKey INT NOT NULL,
	FullName NVARCHAR(50) NULL,
	PreferredName NVARCHAR(50) NULL,
	LogonName NVARCHAR(50) NULL,
	PhoneNumber NVARCHAR(20) NULL,
	FaxNumber NVARCHAR(20) NULL,
	EmailAddress NVARCHAR(256) NULL,
    CONSTRAINT PK_DimSalesPeople PRIMARY KEY CLUSTERED (SalespersonKey )
);


--Create Dimension Date table
CREATE TABLE dbo.DimDate(
	DateKey DATE NOT NULL,
	DateValue DATE NOT NULL,
	CYear SMALLINT NOT NULL,
	CQtr TINYINT NOT NULL,
	CMonth TINYINT NOT NULL,
	Day TINYINT NOT NULL,
	StartOfMonth DATE NOT NULL,
	EndOfMonth DATE NOT NULL,
	MonthName VARCHAR(9) NOT NULL,
	DayOfWeekName VARCHAR(9) NOT NULL,
    CONSTRAINT PK_DimDate PRIMARY KEY CLUSTERED ( DateKey )
);



--Create Dimension FactOrders table
CREATE TABLE dbo.FactOrders(
	CustomerKey INT NOT NULL,
	LocationKey INT NOT NULL,
	ProductKey INT NOT NULL,
	SalespersonKey INT NOT NULL,
	DateKey DATE NOT NULL,
	SupplierKey INT NOT NULL,
	Quantity INT NOT NULL,
	UnitPrice DECIMAL(18, 2) NOT NULL,
	TaxRate DECIMAL(18, 3) NOT NULL,
	TotalBeforeTax DECIMAL(18, 2) NOT NULL,
	TotalAfterTax DECIMAL(18, 2) NOT NULL,

    CONSTRAINT FK_FactOrders_DimCities FOREIGN KEY(LocationKey) REFERENCES dbo.DimLocations (LocationKey),
    CONSTRAINT FK_FactOrders_DimCustomers FOREIGN KEY(CustomerKey) REFERENCES dbo.DimCustomers (CustomerKey),
    CONSTRAINT FK_FactOrders_DimDate FOREIGN KEY(DateKey) REFERENCES dbo.DimDate (DateKey),
    CONSTRAINT FK_FactOrders_DimProducts FOREIGN KEY(ProductKey) REFERENCES dbo.DimProducts (ProductKey),
    CONSTRAINT FK_FactOrders_DimSalesPeople FOREIGN KEY(SalespersonKey) REFERENCES dbo.DimSalesPeople (SalespersonKey),
	CONSTRAINT FK_FactOrders_DimSupplier FOREIGN KEY(SupplierKey) REFERENCES dbo.DimSuppliers (SupplierKey)
);
GO
CREATE TABLE dbo.FactOrderss(
CustomerKey INT NOT NULL,
	LocationKey INT NOT NULL,
	ProductKey INT NOT NULL,
	SalespersonKey INT NOT NULL,
	DateKey DATE NOT NULL,
	SupplierKey INT NOT NULL,
	Quantity INT NOT NULL,
	UnitPrice DECIMAL(18, 2) NOT NULL,
	TaxRate DECIMAL(18, 3) NOT NULL,
	TotalBeforeTax DECIMAL(18, 2) NOT NULL,
	TotalAfterTax DECIMAL(18, 2) NOT NULL,
)




--Create DateLoad table
CREATE PROCEDURE dbo.DimDate_Load 
    @DateValue DATE
AS
BEGIN;
    INSERT INTO dbo.DimDate
    SELECT DATEFROMPARTS(YEAR(@DateValue), MONTH(@DateValue), DAY(@DateValue)),
           @DateValue,
           YEAR(@DateValue),
           MONTH(@DateValue),
           DAY(@DateValue),
           DATEPART(qq,@DateValue),
           DATEADD(DAY,1,EOMONTH(@DateValue,-1)),
           EOMONTH(@DateValue),
           DATENAME(mm,@DateValue),
           DATENAME(dw,@DateValue);
END





------------------------------ Requirement 1 ------------------------------
--Create Dimension Supplier table
create table DimSuppliers(
	SupplierKey int not null,
	SupplierName varchar(30) not null,
	SupplierCategoryName varchar(30) not null,
	FullName varchar(20) not null,
	PhoneNumber nvarchar(50) null,
	FaxNumber int not null,
	WebsiteURL varchar(50) not null,
	StartDate date not null,
	EndDate date not null
	
	constraint PK_Supplier_Key PRIMARY KEY Clustered (SupplierKey)
);


GO

ALTER TABLE DimSuppliers ALTER COLUMN PhoneNumber nvarchar(20) null;




------------------------------ Requirement 2 ------------------------------
--Create Dimension Load table
CREATE PROCEDURE dbo.LoadDateDimension
    @StartDate DATE = '2012-01-01',
    @EndDate DATE = '2021-12-31'
AS
BEGIN
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        EXEC dbo.DimDate_Load @CurrentDate;
        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END

EXEC dbo.LoadDateDimension;

select * from dbo.DimDate;



------------------------------ Requirement 3 ------------------------------

--Get the sum of the total sales and order it in desc sequence
--Create a compelling warehouse query
SELECT 
    c.CustomerName,
    ci.CityName,
    s.FullName,
    p.ProductName,
    d.DateValue,
    SUM(o.Quantity * o.UnitPrice) AS TotalSales
FROM 
    dbo.FactOrders o
    INNER JOIN dbo.DimCustomers c ON o.CustomerKey = c.CustomerKey
    INNER JOIN dbo.DimSalesPeople s ON o.SalespersonKey = s.SalespersonKey
    INNER JOIN dbo.DimProducts p ON o.ProductKey = p.ProductKey
    INNER JOIN dbo.DimDate d ON o.DateKey = d.DateKey
	INNER JOIN dbo.DimLocations ci ON ci.LocationKey = o.LocationKey
GROUP BY 
    c.CustomerName,
    ci.CityName,
    s.FullName,
    p.ProductName,
    d.DateValue
ORDER BY 
    TotalSales DESC;



------------------------------ Requirement 4 ------------------------------



--Create customer stage table
CREATE TABLE stage_Customer(
	CustomerKey INT NOT NULL,
	LocationKey INT NOT NULL,
	CustomerName NVARCHAR(200) NULL,
	CustomerCategoryName NVARCHAR(200) NULL,
	DeliveryCityName NVARCHAR(200) NULL,
	DeliveryStateProvCode NVARCHAR(200) NULL,
	DeliveryStateProvName NVARCHAR(200) NULL,
	DeliveryCountryName NVARCHAR(200) NULL,
	DeliveryFormalName NVARCHAR(200) NULL,
	PostalCityName NVARCHAR(200) NULL,
	PostalStateProvCode NVARCHAR(200) NULL,
	PostalStateProvName NVARCHAR(200) NULL,
	PostalCountryName NVARCHAR(200) NULL,
	PostalFormalName NVARCHAR (200) NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL,
)



--Create customer extract query 
CREATE PROCEDURE CustomerQuery
AS
BEGIN
	INSERT INTO stage_Customer(
	CustomerKey,
	LocationKey,
	CustomerName,
	CustomerCategoryName,
	DeliveryCityName, 
	DeliveryStateProvCode, 
	DeliveryStateProvName, 
	DeliveryCountryName, 
	DeliveryFormalName, 
	PostalCityName, 
	PostalStateProvCode, 
	PostalStateProvName, 
	PostalCountryName, 
	PostalFormalName, 
	StartDate, 
	EndDate 
	)
	
	SELECT 
		c.CustomerID,
		ci.[CityID],
		c.CustomerName,
		cc.CustomerCategoryName,
		ci.CityName,
		sp.StateProvinceCode,
		sp.[StateProvinceName],
		coun.CountryName,
		coun.[FormalName],
		ci.CityName,
		sp.StateProvinceCode,
		sp.[StateProvinceName],
		coun.CountryName,
		coun.[FormalName],
		c.ValidFrom,
		c.ValidTo

	FROM WideWorldImporters.Sales.Customers c
	JOIN WideWorldImporters.Sales.CustomerCategories cc ON c.CustomerCategoryID = cc.CustomerCategoryID
	JOIN WideWorldImporters.Application.Cities ci ON c.DeliveryCityID = ci.CityID
	JOIN WideWorldImporters.Application.StateProvinces sp ON sp.StateProvinceID = ci.StateProvinceID
	JOIN WideWorldImporters.Application.Countries coun ON coun.CountryID = sp.CountryID

END
EXEC CustomerQuery;
select * from stage_Customer




--Create Product Stage table
CREATE TABLE stage_Products(
	ProductKey INT NOT NULL,
	ProductName NVARCHAR(100) NULL,
	ProductColour NVARCHAR(20) NULL,
	ProductBrand NVARCHAR(50) NULL,
	ProductSize NVARCHAR(20) NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL,
)
--Create product extract query procedure
CREATE PROCEDURE ProductsQuery 
AS 
BEGIN
	INSERT INTO stage_Products(
		ProductKey,
		ProductName,
		ProductColour,
		ProductBrand,
		ProductSize,
		StartDate,
		EndDate
	)
	SELECT 
		si.StockItemID,
		si.StockItemName,
		co.ColorName,
		si.Brand,
		si.Size,
		si.ValidFrom,
		si.ValidTo
	FROM
		WideWorldImporters.Warehouse.StockItems si
		JOIN WideWorldImporters.Warehouse.Colors co ON si.ColorID = co.ColorID

END

EXEC ProductsQuery;
select * from stage_Products


--Create sales people stage table
CREATE TABLE Stage_SalesPeople(
	SalespersonKey INT NOT NULL,
	FullName NVARCHAR(50) NULL,
	PreferredName NVARCHAR(50) NULL,
	LogonName NVARCHAR(50) NULL,
	PhoneNumber NVARCHAR(20) NULL,
	FaxNumber NVARCHAR(20) NULL,
	EmailAddress NVARCHAR(256) NULL,
)

--Create sales people extract query procedure
CREATE PROCEDURE SalesPeopleQuery
AS
BEGIN
	INSERT INTO Stage_SalesPeople(
		SalespersonKey,
		FullName,
		PreferredName,
		LogonName,
		PhoneNumber,
		FaxNumber,
		EmailAddress
	)
	SELECT 
		p.PersonID,
		p.FullName,
		p.PreferredName,
		p.LogonName,
		p.PhoneNumber,
		p.FaxNumber,
		p.EmailAddress
	FROM 
		WideWorldImporters.Application.People p
	WHERE 
		p.IsSalesperson = 1;
END


EXEC SalesPeopleQuery;
select * from Stage_SalesPeople






--Create stage order table
CREATE TABLE dbo.Stage_Orders(
	CustomerKey INT NOT NULL,
	LocationKey INT NOT NULL,
	ProductKey INT NOT NULL,
	SalespersonKey INT NOT NULL,
	DateKey DATE NOT NULL,
	SupplierKey INT NOT NULL,
	Quantity INT NOT NULL,
	UnitPrice DECIMAL(18, 2) NOT NULL,
	TaxRate DECIMAL(18, 3) NOT NULL,
	TotalBeforeTax DECIMAL(18, 2) NOT NULL,
	TotalAfterTax DECIMAL(18, 2) NOT NULL,
);


--create order extract procedure
CREATE PROCEDURE Orders_Extract
	@OrderDate DATE
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	DECLARE @RowCt INT;
	
	TRUNCATE TABLE Stage_Orders;
	INSERT INTO Stage_Orders(
		CustomerKey,
		LocationKey,
		ProductKey,
		SalespersonKey,
		DateKey,
		SupplierKey,
		Quantity,
		UnitPrice,
		TaxRate,
		TotalBeforeTax,
		TotalAfterTax
	)
	SELECT
		o.CustomerID,
		c.DeliveryCityID,
		ol.StockItemID,
		o.SalespersonPersonID,
		o.OrderDate,
		si.SupplierID,
		ol.Quantity,
		ol.UnitPrice,
		ol.TaxRate,
		(ol.Quantity * ol.UnitPrice),
		((ol.Quantity * ol.UnitPrice) * (1 + ol.TaxRate))
	FROM WideWorldImporters.Sales.Orders o
	INNER JOIN WideWorldImporters.Sales.OrderLines ol
		ON o.OrderID = ol.OrderID
	INNER JOIN WideWorldImporters.Warehouse.StockItems si
		ON ol.StockItemID = si.StockItemID
	INNER JOIN WideWorldImporters.Sales.Customers c
		ON c.CustomerID = o.CustomerID
		WHERE CAST(o.OrderDate AS DATE) = @OrderDate;

	
	SET @RowCt = @@ROWCOUNT;
	IF @RowCt = 0 
	BEGIN
		THROW 50001, 'No records found. Check with source system.', 1;
	END;
END;
GO


EXEC Orders_Extract '2013-01-01';
select * from Stage_Orders;







--create supplier stage table
CREATE TABLE Stage_Supplier(
	SupplierKey int not null,
	SupplierName varchar(30),
	SupplierCategoryName varchar(50),
	FullName varchar(20) not null,
	PhoneNumber varchar(20) not null,
	FaxNumber int not null,
	WebsiteURL varchar(50) not null,
	StartDate date not null,
	EndDate date not null
	)




-- create supplier extract query 
CREATE PROCEDURE Supplier_Query
AS
BEGIN
	INSERT INTO Stage_Supplier(
		SupplierKey,
		SupplierName,
		SupplierCategoryName,
		FullName,
		PhoneNumber,
		FaxNumber,
		WebsiteURL,
		StartDate,
		EndDate
	)
	SELECT 
		s.SupplierID,
		s.SupplierName,
		sc.SupplierCategoryName,
		s.SupplierName,
		s.PhoneNumber,
		s.FaxNumber,
		s.WebsiteURL,
		s.ValidFrom,
		s.ValidTo
	FROM
		WideWorldImporters.Purchasing.Suppliers s
		JOIN WideWorldImporters.Purchasing.SupplierCategories sc ON s.SupplierCategoryID = sc.SupplierCategoryID
END


ALTER TABLE Stage_Supplier
ALTER COLUMN PhoneNumber nvarchar(20)

ALTER TABLE Stage_Supplier
ALTER COLUMN FaxNumber nvarchar(20)


ALTER TABLE Stage_Supplier
ALTER COLUMN FullName nvarchar(50)

EXEC  Supplier_Query;
select * from Stage_Supplier



------------------------------ Requirement 5 ------------------------------

--Create sequence key
CREATE SEQUENCE dbo.CustomerKey
AS INT
START WITH 1
INCREMENT BY 1;



CREATE SEQUENCE dbo.CustomerKeyy
AS INT
START WITH 1
INCREMENT BY 1;


CREATE SEQUENCE dbo.ProductKey
    START WITH 1
    INCREMENT BY 1;



CREATE SEQUENCE dbo.SalespersonKey
    START WITH 1
    INCREMENT BY 1;



CREATE SEQUENCE dbo.SupplierKey
    START WITH 1
    INCREMENT BY 1;

CREATE SEQUENCE dbo.LocationKey
    START WITH 1
    INCREMENT BY 1;

select * from PreLoadFactOrders



--create preload city table
CREATE TABLE dbo.PreLoadLocation(
	LocationKey INT NOT NULL,
	CityName NVARCHAR(50) NULL,
	StateProvCode NVARCHAR(5) NULL,
	StateProvName NVARCHAR(50) NULL,
	CountryName NVARCHAR(60) NULL,
	CountryFormalName NVARCHAR(60) NULL,
);


--create city transformation procedure
CREATE PROCEDURE dbo.transformLocation
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	TRUNCATE TABLE dbo.PreLoadLocation
	BEGIN TRANSACTION;
	INSERT INTO dbo.PreLoadLocation 
	SELECT NEXT VALUE FOR dbo.LocationKey AS LocationKey,
		sl.[DeliveryCityName],
		sl.[DeliveryStateProvCode],
		sl.[DeliveryStateProvName],
		sl.[DeliveryCountryName],
		sl.[DeliveryFormalName]
	FROM dbo.stage_Customer sl
	WHERE NOT EXISTS ( SELECT 1
	FROM dbo.DimLocations dl
	WHERE sl.[DeliveryCityName] = dl.CityName
		AND sl.[DeliveryStateProvCode] = dl.StateProvCode
		AND sl.[DeliveryStateProvName] = dl.StateProvName
		AND sl.[DeliveryCountryName] = dl.CountryName
		AND sl.[DeliveryFormalName] = dl.CountryFormalName
		);
		
	INSERT INTO dbo.PreLoadlocation /* Column list excluded for brevity */
	SELECT 
		sl.LocationKey,
		sl.[DeliveryCityName],
		sl.[DeliveryStateProvCode],
		sl.[DeliveryStateProvName],
		sl.[DeliveryCountryName],
		sl.[DeliveryFormalName]	
	FROM dbo.stage_Customer sl
		JOIN dbo.DimLocations dl
	ON sl.DeliveryCityName = dl.CityName
		AND sl.DeliveryStateProvCode = dl.StateProvCode
		AND sl.DeliveryStateProvName = dl.StateProvName
		AND sl.DeliveryCountryName = dl.CountryName
		AND sl.DeliveryFormalName = dl.CountryFormalName;

	COMMIT TRANSACTION;
	END;

	exec dbo.transformLocation
	select * from PreLoadLocation


--create customer preload table
CREATE TABLE dbo.PreLoadCustomers(
    CustomerKey INT NOT NULL,
    CustomerName NVARCHAR(100) NULL,
    CustomerCategoryName NVARCHAR(50) NULL,
    DeliveryCityName NVARCHAR(50) NULL,
    DeliveryStateProvCode NVARCHAR(5) NULL,
    DeliveryCountryName NVARCHAR(50) NULL,
    PostalCityName NVARCHAR(50) NULL,
    PostalStateProvCode NVARCHAR(5) NULL,
    PostalCountryName NVARCHAR(50) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL
);

select * from dbo.PreLoadCustomers
select * from [dbo].[DimCustomers]
select * from stage_Customer




--create customer transformation procedure 
CREATE PROCEDURE dbo.transformCustomer
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	TRUNCATE TABLE dbo.PreLoadCustomers;
	
	DECLARE @StartDate DATE = GETDATE();
	DECLARE @EndDate DATE = DATEADD(dd,-1,GETDATE());

	BEGIN TRANSACTION;

	--UPDATE records
	INSERT INTO dbo.PreLoadCustomers
	SELECT NEXT VALUE FOR dbo.CustomerKey AS CustomerKey,
		stg.CustomerName,
		stg.CustomerCategoryName,
		stg.DeliveryCityName,
		stg.DeliveryStateProvCode,
		stg.DeliveryCountryName,
		stg.PostalCityName,
		stg.PostalStateProvCode,
		stg.PostalCountryName,
		@StartDate,
		NULL

	FROM dbo.stage_Customer stg
	JOIN dbo.DimCustomers cu
		ON stg.CustomerName = cu.CustomerName AND cu.EndDate is NULL

	WHERE stg.CustomerCategoryName <> cu.CustomerCategoryName
	OR stg.DeliveryCityName <> cu.DeliveryCityName
	OR stg.DeliveryStateProvCode <> cu.DeliveryStateProvCode
	OR stg.DeliveryCountryName <> cu.DeliveryCountryName
	OR stg.PostalCityName <> cu.PostalCityName
	OR stg.PostalStateProvCode <> cu.PostalStateProvCode
	OR stg.PostalCountryName <> cu.PostalCountryName;
	
	--CREATE records, expire the unnecessary records
	INSERT INTO  dbo.PreLoadCustomers
	SELECT cu.CustomerKey,
	cu.CustomerName,
	cu.CustomerCategoryName,
	cu.DeliveryCityName,
	cu.DeliveryStateProvCode,
	cu.DeliveryCountryName,
	cu.PostalCityName,
	cu.PostalStateProvCode,
	cu.PostalCountryName,
	cu.StartDate,
	CASE
		WHEN preL.CustomerName IS NULL THEN NULL
		ELSE @EndDate
		END AS EndDate
	FROM dbo.DimCustomers cu
	LEFT JOIN dbo.PreLoadCustomers preL
	ON preL.CustomerName = cu.CustomerName
	AND cu.EndDate IS NULL;

	--Create New Records
	INSERT INTO dbo.PreLoadCustomers
	SELECT NEXT VALUE FOR dbo.CustomerKey AS CustomerKey,
	stg.CustomerName,
	stg.CustomerCategoryName,
	stg.DeliveryCityName,
	stg.DeliveryStateProvCode,
	stg.DeliveryCountryName,
	stg.PostalCityName,
	stg.DeliveryStateProvCode,
	stg.PostalCountryName,
	@StartDate,	
	NULL
	FROM dbo.stage_Customer stg
	WHERE NOT EXISTS ( SELECT 1 FROM dbo.DimCustomers cu WHERE stg.CustomerName = cu.CustomerName );

	--Expire Missing Records
	INSERT INTO dbo.PreLoadCustomers
	SELECT cu.CustomerKey,
	cu.CustomerName,
	cu.CustomerCategoryName,
	cu.DeliveryCityName,
	cu.DeliveryStateProvCode,
	cu.DeliveryCountryName,
	cu.PostalCityName,
	cu.PostalStateProvCode,
	cu.PostalCountryName,
	cu.StartDate,
	@EndDate
	FROM [dbo].[DimCustomers] cu
	WHERE NOT EXISTS ( SELECT 1 FROM dbo.stage_Customer stg WHERE stg.CustomerName = cu.CustomerName )
	AND cu.EndDate IS NULL;
	COMMIT TRANSACTION;

END;
GO

execute dbo.transformCustomer
select * from PreLoadCustomers





--create products preload table
CREATE TABLE dbo.PreLoadProducts(
    ProductKey INT NOT NULL,
    ProductName NVARCHAR(100) NULL,
    ProductColour NVARCHAR(20) NULL,
	ProductBrand NVARCHAR (50) NULL,
	ProductSize NVARCHAR (50) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL
);

--create products transformation procedure
CREATE PROCEDURE dbo.transformProducts
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	TRUNCATE TABLE dbo.PreLoadProducts;

	DECLARE @StartDate DATE = GETDATE();
	DECLARE @EndDate DATE = DATEADD(dd,-1,GETDATE());

	BEGIN TRANSACTION;

	--add updated records
	INSERT INTO dbo.PreLoadProducts 
	SELECT NEXT VALUE FOR dbo.ProductKey AS ProductKey,
		spd.ProductName,
		spd.[ProductColour],
		spd.[ProductBrand],
		spd.[ProductSize],
		@StartDate,
	    NULL
	FROM dbo.stage_Products spd
		JOIN dbo.DimProducts dpd
	ON spd.ProductName = dpd.ProductName AND dpd.EndDate IS NULL
	WHERE spd.ProductColour <> dpd.ProductColour
		OR spd.ProductBrand <> dpd.ProductBrand
		OR spd.ProductSize <> dpd.ProductSize

	--add existing record, expire as necessary
	INSERT INTO dbo.PreLoadProducts/* Column list excluded for brevity */
	SELECT 
	    dpd.[ProductKey],
		dpd.[ProductName],
		dpd.[ProductColour],
		dpd.[ProductBrand],
		dpd.[ProductSize],
		dpd.[StartDate],
		CASE
			WHEN plp.ProductName IS NULL THEN NULL
			ELSE @EndDate
		END AS EndDate
	FROM dbo.DimProducts dpd	
	LEFT JOIN dbo.PreLoadProducts plp
		ON plp.ProductName = dpd.ProductName
		AND dpd.EndDate IS NULL;

	--Create New Records
	INSERT INTO dbo.PreLoadProducts /* Column list excluded for brevity */
	SELECT NEXT VALUE FOR dbo.ProductKey AS ProductKey,
		spd.[ProductName],
		spd.[ProductColour],
		spd.[ProductBrand],
		spd.[ProductSize],
		@StartDate,
		NULL
	FROM dbo.stage_Products spd
	WHERE NOT EXISTS ( SELECT 1 FROM dbo.DimProducts dpd WHERE spd.ProductName = dpd.ProductName);


	--Expire Missing Records
	INSERT INTO dbo.PreLoadProducts /* Column list excluded for brevity */
	SELECT dpd.[ProductKey],
		dpd.[ProductName],
		dpd.[ProductColour],
		dpd.[ProductBrand],
		dpd.[ProductSize],
		dpd.[StartDate],
		@EndDate
	FROM dbo.DimProducts dpd
	WHERE NOT EXISTS ( SELECT 1 FROM dbo.stage_Products spd WHERE spd.ProductName = dpd.ProductName )
		AND dpd.EndDate IS NULL;
	COMMIT TRANSACTION;
END;

execute dbo.transformProducts
select * from PreLoadProducts



--create sales people pre load table 
CREATE TABLE dbo.PreLoadSalesPeople(
    SalespersonKey INT NOT NULL,
    FullName NVARCHAR(50) NULL,
    PreferredName NVARCHAR(50) NULL,
    LogonName NVARCHAR(50) NULL,
    PhoneNumber NVARCHAR(20) NULL,
    FaxNumber NVARCHAR(20) NULL,
    EmailAddress NVARCHAR(256) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL
);

--create sales people transformation procedure
CREATE PROCEDURE dbo.TransformSalesPeople
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    TRUNCATE TABLE dbo.PreLoadSalesPeople;

    BEGIN TRANSACTION;
    INSERT INTO dbo.PreLoadSalesPeople
    SELECT NEXT VALUE FOR dbo.SalespersonKey AS SalespersonKey,
        ssperson.[FullName],
        ssperson.[PreferredName],
        ssperson.[LogonName],
        ssperson.[PhoneNumber],
        ssperson.[FaxNumber],
        ssperson.[EmailAddress],
        GETDATE(),
        NULL -- default value for EndDate
    FROM dbo.Stage_SalesPeople ssperson
    WHERE NOT EXISTS (SELECT 1
                      FROM dbo.DimSalesPeople dsperson
                      WHERE ssperson.FullName = dsperson.FullName);

    INSERT INTO dbo.PreLoadSalesPeople (SalespersonKey, FullName, PreferredName, LogonName, PhoneNumber, FaxNumber, EmailAddress, StartDate, EndDate)
    SELECT dsperson.SalespersonKey,
        dsperson.FullName,
        dsperson.[PreferredName],
        dsperson.[LogonName],
        dsperson.[PhoneNumber],
        dsperson.[FaxNumber],
        dsperson.[EmailAddress],
        GETDATE(),
        NULL -- default value for EndDate
    FROM dbo.Stage_SalesPeople ssperson
    JOIN dbo.DimSalesPeople dsperson ON ssperson.FullName = dsperson.FullName;

    COMMIT TRANSACTION;
END;

execute dbo.TransformSalesPeople
select * from PreLoadSalesPeople



--create supplier preload table
CREATE TABLE dbo.PreLoadSuppliers(
    SupplierKey INT NOT NULL,
    SupplierName NVARCHAR(100) NULL,
    SupplierCategoryName NVARCHAR(50) NULL,
	FullName VARCHAR(50) NULL,
    PhoneNumber NVARCHAR(20) NULL,
    FaxNumber NVARCHAR(20) NULL,
    WebsiteURL NVARCHAR(256) NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL
);


--create suppliers transformation procedure
CREATE PROCEDURE dbo.transformSuppliers
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	TRUNCATE TABLE dbo.PreLoadSuppliers;

	DECLARE @StartDate DATE = GETDATE();
	DECLARE @EndDate DATE = DATEADD(dd,-1,GETDATE());

	BEGIN TRANSACTION;

	--add updated records
	INSERT INTO dbo.PreLoadSuppliers (SupplierKey, SupplierName, SupplierCategoryName, FullName, PhoneNumber, FaxNumber, WebsiteURL, StartDate, EndDate)
	SELECT NEXT VALUE FOR dbo.SupplierKey AS SupplierKey,
	    ssp.SupplierName,
		ssp.[SupplierCategoryName],
		ssp.[FullName],
		ssp.[PhoneNumber],
		ssp.[FaxNumber],
		ssp.[WebsiteURL],
		@StartDate,
	    NULL
	FROM dbo.Stage_Supplier ssp
		JOIN dbo.DimSuppliers dsp
	ON ssp.SupplierCategoryName = dsp.SupplierCategoryName AND ssp.EndDate IS NULL
	WHERE ssp.FullName <> dsp.FullName
		OR ssp.PhoneNumber <> dsp.PhoneNumber
		OR ssp.FaxNumber <> dsp.FaxNumber
		OR ssp.WebsiteURL <> dsp.WebsiteURL;

	--add existing record, expire as necessary
	INSERT INTO dbo.PreLoadSuppliers (SupplierKey, SupplierName, SupplierCategoryName, FullName, PhoneNumber, FaxNumber, WebsiteURL, EndDate)
	SELECT 
	    dsp.[SupplierKey],
		dsp.[SupplierName],
		dsp.[SupplierCategoryName],
		dsp.[FullName],
		dsp.[PhoneNumber],
		dsp.[FaxNumber],
		dsp.[WebsiteURL],
		CASE
			WHEN pls.SupplierName IS NULL THEN NULL
			ELSE @EndDate
		END AS EndDate
	FROM dbo.DimSuppliers dsp	
	LEFT JOIN dbo.PreLoadSuppliers pls
		ON pls.SupplierName = dsp.SupplierName
		AND dsp.EndDate IS NULL;

	--Create New Records
	INSERT INTO dbo.PreLoadSuppliers (SupplierKey, SupplierName, SupplierCategoryName, FullName, PhoneNumber, FaxNumber, WebsiteURL, StartDate, EndDate)
	SELECT NEXT VALUE FOR dbo.SupplierKey AS SupplierKey,
		ssp.[SupplierName],
		ssp.[SupplierCategoryName],
		ssp.[FullName],
		ssp.[PhoneNumber],
		ssp.[FaxNumber],
		ssp.[WebsiteURL],
		@StartDate,
		NULL
	FROM dbo.Stage_Supplier ssp
	WHERE NOT EXISTS ( SELECT 1 FROM dbo.DimSuppliers dsp WHERE dsp.SupplierName = ssp.SupplierName);

	--Expire Missing Records
	INSERT INTO dbo.PreLoadSuppliers (SupplierKey, SupplierName, SupplierCategoryName, FullName, PhoneNumber, FaxNumber, WebsiteURL, EndDate)
	SELECT dsp.[SupplierKey],
		dsp.[SupplierName],
		dsp.[SupplierCategoryName],
		dsp.[FullName],
		dsp.[PhoneNumber],
		dsp.[FaxNumber],
		dsp.[WebsiteURL],
		@EndDate
	FROM dbo.DimSuppliers dsp
	WHERE NOT EXISTS ( SELECT 1 FROM dbo.Stage_Supplier ssp WHERE ssp.SupplierName = dsp.SupplierName )
		AND dsp.EndDate IS NULL;
	COMMIT TRANSACTION;
END;

execute dbo.transformSuppliers



--create preload fact orders table
CREATE TABLE dbo.PreLoadFactOrders(
	CustomerKey INT NOT NULL,
	LocationKey INT NOT NULL,
	ProductKey INT NOT NULL,
	SalespersonKey INT NOT NULL,
	DateKey DATE NOT NULL, 
	SupplierKey INT NOT NULL,
	Quantity INT NOT NULL,
	UnitPrice DECIMAL(18, 2) NOT NULL,
	TaxRate DECIMAL(18, 3) NOT NULL,
	TotalBeforeTax DECIMAL(18, 2) NOT NULL,
	TotalAfterTax DECIMAL(18, 2) NOT NULL
);




--create fact order transformation procedure
CREATE PROCEDURE dbo.FactOrders_Load10
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    TRUNCATE TABLE dbo.PreLoadFactOrders;

	ALTER SEQUENCE dbo.CustomerKey RESTART WITH 1;

    DECLARE @StartDate DATE = GETDATE();
    DECLARE @EndDate DATE = DATEADD(day,-1,GETDATE());

    BEGIN TRANSACTION;
    --add updated records
    INSERT INTO dbo.PreLoadFactOrders(CustomerKey,LocationKey,ProductKey,SalespersonKey,DateKey,SupplierKey,Quantity,UnitPrice,TaxRate,TotalBeforeTax,TotalAfterTax)
    SELECT NEXT VALUE FOR dbo.CustomerKeyy AS CustomerKey,
	SELECT NEXT VALUE FOR dbo.LocationKey AS LocationKey,
	SELECT NEXT VALUE FOR dbo.ProductKey AS ProductKey,
	SELECT NEXT VALUE FOR dbo.SalespersonKey AS SalespersonKey,
	SELECT NEXT VALUE FOR dbo.DateKey AS DateKey,
	SELECT NEXT VALUE FOR dbo.SupplierKey AS SupplierKey,
        so.[Quantity],
        so.[UnitPrice],
        so.[TaxRate],
        so.[TotalBeforeTax],
        so.[TotalAfterTax]
    FROM dbo.Stage_Orders so
	JOIN dbo.stage_Customer c ON c.CustomerKey = so.CustomerKey
    JOIN dbo.FactOrders fo
        ON so.LocationKey = fo.LocationKey
    WHERE so.ProductKey <> fo.ProductKey
        OR so.SalespersonKey <> fo.SalespersonKey
        OR so.DateKey <> fo.DateKey
        OR so.SupplierKey <> fo.SupplierKey
        OR so.Quantity <> fo.Quantity
        OR so.UnitPrice <> fo.UnitPrice
        OR so.TaxRate <> fo.TaxRate
        OR so.TotalBeforeTax <> fo.TotalBeforeTax
        OR so.TotalAfterTax <> fo.TotalAfterTax;

    --add existing record, expire as necessary
    INSERT INTO dbo.PreLoadFactOrders(CustomerKey,LocationKey,ProductKey,SalespersonKey,DateKey,SupplierKey,Quantity,UnitPrice,TaxRate,TotalBeforeTax,TotalAfterTax)
    SELECT 
        plc.CustomerKey,
        fo.LocationKey,
        fo.[ProductKey],
        fo.[SalespersonKey],
        fo.[DateKey],
        fo.[SupplierKey],
        fo.[Quantity],
        fo.[UnitPrice],
        fo.[TaxRate],
        fo.[TotalBeforeTax],
        fo.[TotalAfterTax]
    FROM dbo.FactOrders fo
	LEFT JOIN dbo.PreLoadCustomers plc
		ON plc.CustomerKey = fo.CustomerKey
    LEFT JOIN dbo.PreLoadFactOrders plfo
        ON plfo.LocationKey = fo.LocationKey
    WHERE plfo.LocationKey IS NULL;

    --Create New Records
    INSERT INTO dbo.PreLoadFactOrders(CustomerKey,LocationKey,ProductKey,SalespersonKey,DateKey,SupplierKey,Quantity,UnitPrice,TaxRate,TotalBeforeTax,TotalAfterTax)
    SELECT NEXT VALUE FOR dbo.CustomerKey AS CustomerKey,
        so.[LocationKey],
        so.[ProductKey],
        so.[SalespersonKey],
        so.[DateKey],
        so.[SupplierKey],
        so.[Quantity],
        so.[UnitPrice],
        so.[TaxRate],
        so.[TotalBeforeTax],
        so.[TotalAfterTax]
    FROM dbo.Stage_Orders so
    WHERE NOT EXISTS (SELECT 1 FROM dbo.FactOrders fo WHERE fo.LocationKey = so.LocationKey);

    --Expire Missing Records
    DELETE FROM dbo.PreLoadFactOrders
    WHERE NOT EXISTS (SELECT 1 FROM dbo.Stage_Orders so WHERE so.LocationKey = dbo.PreLoadFactOrders.LocationKey);

    COMMIT TRANSACTION;
END;


execute dbo.FactOrders_Load10
select * from dbo.PreLoadFactOrders


------------------------------ Requirement 6 ------------------------------
--create city load procedure
CREATE PROCEDURE dbo.Locations_Load
AS
BEGIN;
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRANSACTION;
DELETE dl
FROM dbo.DimLocations dl
JOIN dbo.PreLoadLocation pl
ON dl.LocationKey = pl.LocationKey;
INSERT INTO dbo.DimLocations 
SELECT * 
FROM dbo.PreLoadLocation;
COMMIT TRANSACTION;
END;

exec dbo.Locations_Load
select * from dbo.DimLocations




--create customer load procedure
CREATE PROCEDURE dbo.Customers_Load
AS
BEGIN;
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRANSACTION;
DELETE dimcust
FROM dbo.DimCustomers dimcust
JOIN dbo.PreLoadCustomers custpre
ON dimcust.CustomerKey = custpre.CustomerKey;
INSERT INTO dbo.DimCustomers 
SELECT * 
FROM dbo.PreLoadCustomers;
COMMIT TRANSACTION;
END;

execute dbo.Customers_Load
select * from dbo.DimCustomers



--create product load procedure
CREATE PROCEDURE dbo.Product_Load
AS
BEGIN;
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRANSACTION;
DELETE dpro
FROM dbo.DimProducts dpro
JOIN dbo.PreLoadProducts prepro
ON dpro.ProductKey = prepro.ProductKey;
INSERT INTO dbo.DimProducts 
SELECT *
FROM dbo.PreLoadProducts;
COMMIT TRANSACTION;
END;

execute dbo.Product_Load
select * from dbo.DimProducts


--create supplier load procedure
CREATE PROCEDURE dbo.Suppliers_Load
AS
BEGIN;
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRANSACTION;
DELETE dsupplier
FROM dbo.DimSuppliers dsupplier	
JOIN dbo.PreLoadSuppliers preSupplier
ON dsupplier.SupplierKey = preSupplier.SupplierKey;
INSERT INTO dbo.DimSuppliers 
SELECT * 
FROM dbo.PreLoadSuppliers;
COMMIT TRANSACTION;
END;

ALTER TABLE dbo.DimSuppliers ALTER column FaxNumber nvarchar(50) null;
ALTER TABLE dbo.DimSuppliers ALTER column EndDate DATE null;
ALTER TABLE dbo.DimSuppliers ALTER column PhoneNumber nvarchar(200) null;
ALTER TABLE dbo.DimSuppliers ALTER column FullName nvarchar(200) null;

execute dbo.Suppliers_Load
select * from dbo.DimSuppliers






--Create sales people load procedure

ALTER TABLE dbo.DimSalesPeople
ADD StartDate DATE,
    EndDate DATE;

CREATE PROCEDURE dbo.SalesPeople_Load_2
AS
BEGIN;
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRANSACTION;
DELETE dsales
FROM dbo.DimSalesPeople dsales
JOIN dbo.PreLoadSalesPeople preSales
ON dsales.SalespersonKey = preSales.SalespersonKey;
INSERT INTO dbo.DimSalesPeople
SELECT *
FROM dbo.PreLoadSalesPeople;
COMMIT TRANSACTION;
END;

select * from [dbo].[PreLoadSalesPeople]
select * from [dbo].[DimSalesPeople]


exec dbo.SalesPeople_Load_2
select * from DimSalesPeople



--create load orders procedure
CREATE PROCEDURE dbo.Orders_Load_42
AS
BEGIN;

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	INSERT INTO dbo.FactOrders
	SELECT * 
	FROM dbo.PreLoadFactOrders
	WHERE CustomerKey IN (SELECT CustomerKey FROM dbo.DimCustomers)
	OR ProductKey IN (SELECT ProductKey FROM dbo.DimProducts)
	OR LocationKey IN (SELECT LocationKey FROM dbo.DimLocations)
END;
GO





exec dbo.Orders_Load_42
select * from dbo.FactOrders


select * from DimCustomers
select * from PreLoadFactOrders

select * from DimProducts

select * from DimLocations

------------------------------ Requirement 7 ------------------------------

--get information from 2013-1-1 to 2013-1-4
DECLARE @Date DATE = '2013-01-01';

WHILE @Date <= '2013-01-04'
BEGIN
    EXEC dbo.Orders_Extract @OrderDate = @Date;
    EXEC dbo.FactOrders_Load10;
    EXEC dbo.Orders_Load_31;
    SET @Date = DATEADD(DAY, 1, @Date);
END

SELECT
    cust.CustomerName,
    loc.CityName,
    sp.PreferredName,
    prod.ProductName,
    supp.SupplierName,
    dt.DateValue,
    SUM(fact.Quantity) AS TotalQuantity,
    SUM(fact.TotalBeforeTax) AS TotalSalesAmount
FROM dbo.FactOrderss AS fact
left JOIN dbo.DimCustomers AS cust ON fact.CustomerKey = cust.CustomerKey
left JOIN dbo.DimLocations AS loc ON fact.LocationKey = loc.LocationKey
left JOIN dbo.DimSalesPeople AS sp ON fact.SalespersonKey = sp.SalespersonKey
left JOIN dbo.DimProducts AS prod ON fact.ProductKey = prod.ProductKey
left JOIN dbo.DimSuppliers AS supp ON fact.SupplierKey = supp.SupplierKey
left JOIN dbo.DimDate AS dt ON fact.DateKey = dt.DateKey
GROUP BY
    cust.CustomerName,
    loc.CityName,
    sp.PreferredName,
    prod.ProductName,
    supp.SupplierName,
    dt.DateValue
ORDER BY
    dt.DateValue,
    TotalSalesAmount DESC;

	select * from dbo.FactOrderss