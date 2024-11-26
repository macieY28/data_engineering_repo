--triggers insert/delete products

SELECT * FROM DimSubcategory WHERE SubcategoryID IN (5,6)

INSERT INTO DimProduct (ProductName, SubcategoryID, Producer, UnitPrice)
VALUES
	('Umbro Tocco Ili', 6, 'Umbro', 199.99),
	('Macron LPFA', 5, 'Macron', 249.99)

SELECT * FROM DimSubcategory WHERE SubcategoryID IN (5,6)

DELETE FROM DimProduct WHERE ProductName IN ('Umbro Tocco Ili', 'Macron LPFA')

SELECT * FROM DimSubcategory WHERE SubcategoryID IN (5,6)

--adding sales and promotion assign

INSERT INTO DimCustomer (FirstName, LastName, BirthDate, Gender, Kids, MartialStatus, DeliveryAddress, CorrespondenceAddress)
VALUES
	('Maciej', 'Gruszczyñski', '1995-11-10', 'M', 0, 'single', 'Poziomkowa 1, 61-215 Poznañ', 'Poziomkowa 1, 61-215 Poznañ')

DECLARE @AddedCustomer int = SCOPE_IDENTITY()
EXEC AddSales
	@CustomerID = @AddedCustomer, @DeliveryMethod = 'DPD', @Products = 'Real Madrid Home Kit,Puma King Platinum', @Quantities = '1,2'

SELECT TOP 1 * FROM SalesOnline ORDER BY OrderDate DESC
SELECT TOP 2 * FROM OrderDetails ORDER BY OrderID DESC

INSERT INTO DimPromotion (Description, Discount, StartDate, EndDate, Scope) VALUES
('New Balance 24h promo', 0.17, CAST(GETDATE() AS date), CAST(GETDATE() AS date), 'New Balance')

EXEC AddSales
	@CustomerID = NULL, @DeliveryMethod = 'InPost', @Products = 'New Balance Accelerate Training Shorts', @Quantities = '1'

SELECT TOP 1 * FROM SalesOnline ORDER BY OrderDate DESC
SELECT TOP 1 * FROM OrderDetails ORDER BY OrderID DESC


--procedure add invoice

SELECT TOP 1 * FROM Invoices ORDER BY InvoiceDate

EXEC AddInvoice @OrderID = 10254

SELECT TOP 1 * FROM Invoices ORDER BY InvoiceDate

--view vSales

SELECT * FROM vSales ORDER BY OrderID

--system versioning

UPDATE DimProduct SET UnitPrice = 42.99 WHERE ProductID = 14

SELECT * FROM DimProductHistory

UPDATE DimCustomer SET CorrespondenceAddress = 'ul. Aleja Grunwaldzka 180/15, 80-200 Gdañœk' WHERE CustomerID = 158

SELECT * FROM DimCustomerHistory


 
