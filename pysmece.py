from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import *

conf = SparkConf().setAppName("YourAppName") \
                  .set("spark.executor.memory", "2g") \
                  .set("spark.driver.memory", "2g")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

customer_df = spark.read.option("header", True).csv("data/customers.csv").alias("c")
orders_df = spark.read.option("header", True).csv("data/orders.csv").alias("o")
order_details_df = spark.read.option("header", True).csv("data/orderdetails.csv").alias("od")
product_df = spark.read.option("header", True).csv("data/products.csv").alias("p")

"""
SELECT 
    o.OrderID,
    o.OrderDate,
    o.TotalAmount,
    c.CustomerName,
    c.CustomerEmail
FROM 
    Orders o
JOIN 
    Customers c
ON 
    o.CustomerID = c.CustomerID;
"""
def listAllOrdersWithCustomerDetails():
    return customer_df.join(orders_df, col("c.CustomerID") == col("o.CustomerID")).select(
       col("o.OrderID"), col("o.OrderDate"), col("o.TotalAmount"), col("c.CustomerName"), col("c.CustomerEmail")
    )

#listAllOrdersWithCustomerDetails().show()

"""
SELECT 
    od.OrderID,
    p.ProductName,
    p.ProductCategory,
    od.Quantity,
    p.Price,
    (od.Quantity * p.Price) AS TotalPrice
FROM 
    OrderDetails od
JOIN 
    Products p
ON 
    od.ProductID = p.ProductID;
"""
def listOrderDetailsWithProductInformation():
    return order_details_df.join(product_df, col("od.ProductID") == col("p.ProductID")).withColumn("TotalPrice", col("od.Quantity") * col("p.Price")).select(
        col("od.OrderID"), col("p.ProductName"), col("p.ProductCategory"), col("od.Quantity"), col("p.Price"), col("TotalPrice")
    )

#listOrderDetailsWithProductInformation().show()

"""
SELECT 
    c.CustomerID,
    c.CustomerName,
    SUM(o.TotalAmount) AS TotalSpent
FROM 
    Orders o
JOIN 
    Customers c
ON 
    o.CustomerID = c.CustomerID
GROUP BY 
    c.CustomerID, 
    c.CustomerName;
"""
def totalSpendingByEachCustomer():
    df_join = orders_df.join(customer_df, col("c.CustomerID") == col("o.CustomerID")).select(
        col("c.CustomerID"), col("c.CustomerName"), col("o.TotalAmount")
    )

    return df_join.groupBy(col("c.CustomerID"), col("c.CustomerName")).agg(sum(col("o.TotalAmount")).alias("TotalSpent"))
    
 
    """return orders_df.join(customer_df, col("o.CustomerID") == col("c.CustomerID")).withColumn("TotalSpent", sum(col("o.TotalAmount"))).select(
        col("c.CustomerID"), col("c.CustomerName"), col("TotalSpent")
    ).groupBy(col("c.CustomerID"), col("c.CustomerName"))"""


#totalSpendingByEachCustomer().show()

"""
SELECT 
    p.ProductID,
    p.ProductName,
    SUM(od.Quantity) AS TotalQuantitySold,
    SUM(od.Quantity * p.Price) AS TotalSales
FROM 
    OrderDetails od
JOIN 
    Products p
ON 
    od.ProductID = p.ProductID
GROUP BY 
    p.ProductID, 
    p.ProductName;
"""
def totalSalesOfEachProduct():
    df_joined = product_df.join(order_details_df, col("od.ProductID") == col("p.ProductID")).select(
        col("p.ProductID"), col("p.ProductName"), col("od.Quantity"), col("p.Price")
    )

    return df_joined.groupBy(col("p.ProductID"), col("p.ProductName")).agg(
        sum(col("od.Quantity")).alias("TotalQuantitySold"),
        sum((col("od.Quantity") * col("p.Price"))).alias("TotalSales")
    ).select(
        col("p.ProductID"), col("p.ProductName"), col("TotalQuantitySold"), col("TotalSales")
    )

#totalSalesOfEachProduct().show()
"""
SELECT 
    c.CustomerID,
    c.CustomerName,
    COUNT(o.OrderID) AS TotalOrders,
    SUM(o.TotalAmount) AS TotalSpent
FROM 
    Orders o
JOIN 
    Customers c
ON 
    o.CustomerID = c.CustomerID
GROUP BY 
    c.CustomerID, 
    c.CustomerName;
"""
def totalOrdersAndAmountByCustomer():
    df_joined = orders_df.join(customer_df, col("o.CustomerID") == col("c.CustomerID")).select(
        col("c.CustomerID"), col("c.CustomerName"), col("o.OrderID"), col("o.TotalAmount")
    )

    return df_joined.groupBy(col("c.CustomerID"), col("c.CustomerName")).agg(
        count(col("o.OrderID")).alias("TotalOrders"),
        sum(col("o.TotalAmount")).alias("TotalSpent")
    )

#totalOrdersAndAmountByCustomer().show()

"""
SELECT 
    c.CustomerID,
    c.CustomerName,
    AVG(o.TotalAmount) AS AverageOrderAmount
FROM 
    Orders o
JOIN 
    Customers c
ON 
    o.CustomerID = c.CustomerID
GROUP BY 
    c.CustomerID, 
    c.CustomerName;
"""
def avgOrderAmountPerCustomer():
    df_joined = orders_df.join(customer_df, col("c.CustomerID") == col("o.CustomerID")).select(
        col("c.CustomerID"), col("c.CustomerName"), col("o.TotalAmount")
    )

    return df_joined.groupBy(col("c.CustomerID"), col("c.CustomerName")).agg(
        avg(col("o.TotalAmount")).alias("AverageOrderAmount")
    )

#avgOrderAmountPerCustomer().show()

"""
SELECT 
    p.ProductID,
    p.ProductName
FROM 
    Products p
LEFT JOIN 
    OrderDetails od
ON 
    p.ProductID = od.ProductID
WHERE 
    od.ProductID IS NULL;
"""
def listOfProductsNotSold():
    return product_df.join(order_details_df, col("p.ProductID") == col("od.ProductID"), "left").select(
        col("p.ProductID"), col("p.ProductName")
    ).filter("od.ProductID is not null")

#listOfProductsNotSold().show()

"""
SELECT * 
FROM Orders 
WHERE OrderDate BETWEEN '2023-08-01' AND '2023-08-05';
"""
def ordersInSpecificDateRange(rangeLow: str, rangeHigh: str):
    return orders_df.select("o.*").filter(col("o.OrderDate").between(rangeLow, rangeHigh))

#ordersInSpecificDateRange('2023-08-01', '2023-08-05').show()

