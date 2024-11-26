DROP DATABASE IF EXISTS EcommerceFootballStore

CREATE DATABASE EcommerceFootballStore

USE EcommerceFootballStore

CREATE TABLE DimCategory(
	CategoryID int IDENTITY(1,1) PRIMARY KEY,
	CategoryName varchar(50) NOT NULL,
	CategoryCode char(3) NOT NULL
	)

CREATE TABLE DimSubcategory(
	SubcategoryID int IDENTITY(1,1) PRIMARY KEY,
	CategoryID int FOREIGN KEY REFERENCES DimCategory(CategoryID),
	SubcategoryName varchar(50) NOT NULL,
	SubcategoryCode char(3) NOT NULL,
	ProductCount int DEFAULT 0
	)

CREATE TABLE DimProduct(
	ProductID int IDENTITY(1,1) PRIMARY KEY,
	ProductName varchar(50) NOT NULL,
	SubcategoryID int FOREIGN KEY REFERENCES DimSubcategory(SubcategoryID),
	Producer varchar(50) NOT NULL,
	UnitPrice decimal(10,2) NOT NULL,
	StartTime datetime2 GENERATED ALWAYS AS ROW START HIDDEN,
    EndTime datetime2 GENERATED ALWAYS AS ROW END HIDDEN,
    PERIOD FOR SYSTEM_TIME (StartTime, EndTime)
	)
	WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.DimProductHistory))

CREATE TABLE DimDelivery(
	DeliveryID int IDENTITY(1,1) PRIMARY KEY,
	Supplier varchar(50) NOT NULL,
	DeliveryPrice decimal(10,2) NOT NULL
	)

CREATE TABLE DimPromotion(
	PromotionID int IDENTITY(1,1) PRIMARY KEY,
	Description varchar(100) NOT NULL,
	Discount decimal(10,2),
	StartDate date NOT NULL,
	EndDate date NOT NULL,
	Scope varchar(50) NOT NULL
	)

CREATE TABLE DimCustomer(
	CustomerID int IDENTITY(150,1) PRIMARY KEY,
	FirstName varchar(50) NOT NULL,
	LastName varchar(50) NOT NULL,
	BirthDate date NOT NULL,
	Gender char(1) NOT NULL,
	Kids int,
	MartialStatus varchar(20) NOT NULL,
	DeliveryAddress varchar(100) NOT NULL,
	CorrespondenceAddress varchar(100) NOT NULL,
	StartTime datetime2 GENERATED ALWAYS AS ROW START HIDDEN,
    EndTime datetime2 GENERATED ALWAYS AS ROW END HIDDEN,
    PERIOD FOR SYSTEM_TIME (StartTime, EndTime),
	CONSTRAINT CheckAge CHECK (DATEDIFF(YEAR,BirthDate,GETDATE()) >= 18),
	CONSTRAINT CheckGender CHECK (Gender = 'M' OR Gender = 'F'),
	CONSTRAINT CheckMartialStatus CHECK (MartialStatus IN ('single','married','divorced'))
	)
	WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.DimCustomerHistory))

CREATE TABLE SalesOnline(
	OrderID int IDENTITY(10000,1) PRIMARY KEY,
	OrderDate datetime DEFAULT CURRENT_TIMESTAMP,
	DeliveryDate date,
	CustomerID int NULL FOREIGN KEY REFERENCES DimCustomer(CustomerID),
	DeliveryID int FOREIGN KEY REFERENCES DimDelivery(DeliveryID),
	Invoice bit DEFAULT 0
	)

CREATE TABLE OrderDetails(
	PositionID int IDENTITY(1,1),
	OrderID int FOREIGN KEY REFERENCES SalesOnline(OrderID),
	ProductID int FOREIGN KEY REFERENCES DimProduct(ProductID),
	Quantity int NOT NULL,
	PromotionID int NULL FOREIGN KEY REFERENCES DimPromotion(PromotionID)
	)

CREATE TABLE Invoices(
	InvoiceNo varchar(50) PRIMARY KEY,
	PositionCount int NOT NULL,
	ProductCount int NOT NULL,
	InvoiceDate datetime NOT NULL,
	Customer varchar(101),
	PersonalInvoice bit NOT NULL
	)

GO
CREATE OR ALTER VIEW vSales
AS
(
	SELECT s.OrderID, s.OrderDate, s.DeliveryDate, c.DeliveryAddress,
		CASE WHEN  CONCAT_WS(' ', c.FirstName, c.LastName) = '' THEN 'purchase without registration' ELSE CONCAT_WS(' ', c.FirstName, c.LastName) END AS 'Customer', 
		CAST(SUM(p.UnitPrice * o.Quantity * (CASE WHEN pr.Discount IS NOT NULL THEN 1-pr.Discount ELSE 1 END)) AS decimal(10,2)) AS 'SalesAmount', 
		MAX(d.DeliveryPrice) AS 'DeliveryPrice',
		CAST(SUM(p.UnitPrice * o.Quantity * (CASE WHEN pr.Discount IS NOT NULL THEN 1-pr.Discount ELSE 1 END)) + MAX(d.DeliveryPrice) AS decimal(10,2)) AS 'TotalAmount',
		COUNT(*) AS 'PositionCount'
	FROM SalesOnline s
	JOIN OrderDetails o  ON s.OrderID = o.OrderID
	JOIN DimProduct p ON o.ProductID = p.ProductID
	JOIN DimDelivery d ON s.DeliveryID = d.DeliveryID
	LEFT JOIN DimPromotion pr ON o.PromotionID = pr.PromotionID
	LEFT JOIN DimCustomer c ON s.CustomerID = c.CustomerID
	GROUP BY s.OrderID, s.OrderDate, s.DeliveryDate, c.DeliveryAddress, CONCAT_WS(' ', c.FirstName, c.LastName)
)
