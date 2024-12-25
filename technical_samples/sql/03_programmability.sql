USE EcommerceFootballStore

-- trigger updating product count in subcategory after adding new product/products
GO
CREATE OR ALTER TRIGGER InsertProductCount ON DimProduct
AFTER INSERT
AS
BEGIN
	DECLARE @Subcategory int
	DECLARE crs_inserted CURSOR FOR
	SELECT SubcategoryID FROM inserted

	OPEN crs_inserted
	FETCH NEXT FROM crs_inserted INTO @Subcategory
	WHILE @@FETCH_STATUS = 0
		BEGIN
		UPDATE DimSubcategory
		SET ProductCount = ProductCount + 1
		WHERE SubcategoryID = @Subcategory
		FETCH NEXT FROM crs_inserted INTO @Subcategory
		END
END;

-- trigger updating product count in subccategory after deleting product/products

GO
CREATE OR ALTER TRIGGER DeleteProductCount ON DimProduct
AFTER DELETE
AS
BEGIN
	DECLARE @Subcategory int
	DECLARE crs_deleted CURSOR FOR
	SELECT SubcategoryID FROM deleted

	OPEN crs_deleted
	FETCH NEXT FROM crs_deleted INTO @Subcategory
	WHILE @@FETCH_STATUS = 0
		BEGIN
		UPDATE DimSubcategory
		SET ProductCount = ProductCount - 1
		WHERE SubcategoryID = @Subcategory
		FETCH NEXT FROM crs_deleted INTO @Subcategory
		END
END;

--procedure to register new online sales

GO
CREATE OR ALTER PROCEDURE AddSales
	@CustomerID int = NULL,
	@DeliveryMethod varchar(50),
	@Products varchar(1049),
	@Quantities varchar(59)
AS
	BEGIN
	SET NOCOUNT ON
	
	BEGIN TRY
	BEGIN TRANSACTION

	IF (SELECT COUNT(value) FROM STRING_SPLIT(@Products,',')) != (SELECT COUNT(value) FROM STRING_SPLIT(@Quantities,','))
		BEGIN
			PRINT 'Number of products must be equal to number of quantities'
			RETURN
		END
	ELSE
		BEGIN
			DECLARE @DeliveryID int
			DECLARE @OrderID int
			DECLARE @ProductIDs table(ProductID int, ordinal int)
			DECLARE @Qts table(Quantity int, ordinal int)

			SELECT @DeliveryID = DeliveryID FROM DimDelivery WHERE Supplier = @DeliveryMethod
	
			INSERT INTO SalesOnline (OrderDate, DeliveryDate, CustomerID, DeliveryID, Invoice)
			VALUES
				(GETDATE(), NULL, @CustomerID, @DeliveryID, 0)

			SELECT @OrderID = SCOPE_IDENTITY()
			
			INSERT INTO @ProductIDs(ProductID, ordinal)
			SELECT p.ProductID, ROW_NUMBER() OVER(ORDER BY p.ProductID) FROM STRING_SPLIT(@Products,',') s JOIN DimProduct p ON s.value = p.ProductName

			INSERT INTO @Qts(Quantity, ordinal)
			SELECT value, ROW_NUMBER() OVER(ORDER BY value)  FROM STRING_SPLIT(@Quantities,',')

			INSERT INTO OrderDetails (OrderID, ProductID, Quantity)
			SELECT @OrderID, p.ProductID, q.Quantity
			FROM @ProductIDs p
			JOIN @Qts q ON p.ordinal = q.ordinal

		END
	COMMIT

	END TRY
	BEGIN CATCH
		ROLLBACK;
		THROW
	END CATCH
	END
GO

--procedure adding new invoice

GO
CREATE OR ALTER PROCEDURE AddInvoice
	@OrderID int
AS
	BEGIN
	DECLARE @OrderDate datetime
	DECLARE @PositionCount int
	DECLARE @ProductCount int
	DECLARE @Customer varchar(101)
	DECLARE @PersonalInvoice bit

	SELECT @OrderDate = OrderDate FROM SalesOnline WHERE OrderID = @OrderID

	SELECT @PositionCount = COUNT(*), @ProductCount = SUM(Quantity) FROM OrderDetails WHERE OrderID = @OrderID

	SELECT @Customer = CONCAT_WS(' ',FirstName, LastName) FROM DimCustomer c
	JOIN SalesOnline o ON o.CustomerID = c.CustomerID WHERE o.OrderID = @OrderID

	IF (SELECT CustomerID FROM SalesOnline WHERE OrderID = @OrderID) IS NULL
		SET @PersonalInvoice = 0
	ELSE
		SET @PersonalInvoice = 1

	INSERT INTO Invoices(InvoiceNo, PositionCount, ProductCount, InvoiceDate, Customer, PersonalInvoice)
	VALUES
	(CONCAT_WS('/','FV',@OrderID,YEAR(@OrderDate),MONTH(@OrderDate),DAY(@OrderDate)),
	@PositionCount, @ProductCount, GETDATE(), @Customer, @PersonalInvoice)

	UPDATE SalesOnline SET Invoice = 1 WHERE OrderID = @OrderID

	END

-- function assigning promotion to order position

GO
CREATE OR ALTER FUNCTION PromoCalculate(
	@CustomerID int,
	@ProductID int
	)
RETURNS int AS
BEGIN
	DECLARE @PromotionID int
	DECLARE @Producer varchar(50)

	SET @PromotionID = NULL
	SELECT @Producer = MAX(Producer) FROM DimProduct WHERE ProductID = @ProductID
	
	IF (SELECT COUNT(CustomerID) FROM SalesOnline WHERE CustomerID = @CustomerID) = 1
		SET @PromotionID = 1
	ELSE IF (SELECT Scope FROM DimPromotion WHERE StartDate <= CAST(GETDATE() AS date) AND EndDate >= CAST(GETDATE() AS date) AND Scope = @Producer) IS NOT NULL
		SELECT @PromotionID = MAX(PromotionID) FROM DimPromotion WHERE  StartDate <= CAST(GETDATE() AS date) AND EndDate >= CAST(GETDATE() AS date) AND Scope = @Producer
	ELSE 
		SELECT @PromotionID = MAX(PromotionID) FROM DimPromotion WHERE StartDate <= CAST(GETDATE() AS date) AND EndDate >= CAST(GETDATE() as date) AND PromotionID !=1 AND Scope = 'Global'

	RETURN @PromotionID
END


--trigger applying promotion assing after adding new sale
GO

CREATE OR ALTER TRIGGER PromoAssign ON OrderDetails
AFTER INSERT
AS
BEGIN

	DECLARE @ProductID int
	DECLARE @CustomerID int
	DECLARE @PositionID int
	DECLARE @OrderID int
	DECLARE @PromotionID int
	DECLARE crs_od_inserted CURSOR LOCAL FOR 
	SELECT ProductID, PositionID, OrderID FROM inserted

	OPEN crs_od_inserted
	FETCH NEXT FROM crs_od_inserted INTO @ProductID, @PositionID, @OrderID
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			SELECT @CustomerID = CustomerID FROM SalesOnline WHERE OrderID = @OrderID
			SET @PromotionID = dbo.PromoCalculate(@CustomerID,@ProductID)
			UPDATE OrderDetails SET PromotionID = @PromotionID WHERE PositionID = @PositionID
			FETCH NEXT FROM crs_od_inserted INTO @ProductID, @PositionID, @OrderID
		END
	CLOSE crs_od_inserted
	DEALLOCATE crs_od_inserted
END;
