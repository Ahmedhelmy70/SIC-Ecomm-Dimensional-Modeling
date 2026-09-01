-- Load Customers
LOAD DATA LOCAL INFILE '/media/sf_Samsung/Project_1/Dataset/olist_customers_dataset.csv'
INTO TABLE raw_customers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load Products
LOAD DATA LOCAL INFILE '/media/sf_Samsung/Project_1/Dataset/olist_products_dataset.csv'
INTO TABLE raw_products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load Orders
LOAD DATA LOCAL INFILE '/media/sf_Samsung/Project_1/Dataset/olist_orders_dataset.csv'
INTO TABLE raw_orders
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load Order Items
LOAD DATA LOCAL INFILE '/media/sf_Samsung/Project_1/Dataset/olist_order_items_dataset.csv'
INTO TABLE raw_order_items
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load Order Payments
LOAD DATA LOCAL INFILE '/media/sf_Samsung/Project_1/Dataset/olist_order_payments_dataset.csv'
INTO TABLE raw_order_payments
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
