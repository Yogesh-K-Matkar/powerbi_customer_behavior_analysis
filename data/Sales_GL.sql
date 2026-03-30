USE ReportDemo



/*
 CREATE TABLE customers (
     customer_id INTEGER PRIMARY KEY NOT NULL,
     name Varchar(50) NOT NULL,
     country Varchar(50) NOT NULL,
     signup_date Date NOT NULL
 );

 CREATE TABLE orders (
     order_id INTEGER PRIMARY KEY NOT NULL,
     customer_id INTEGER NOT NULL,            
     order_date DATE NOT NULL,
     product_category VARCHAR(50) NOT NULL,
     order_amount FLOAT NOT NULL,
     payment_method VARCHAR(20) NOT NULL,
     FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
 );
*/

/*
 INSERT INTO customers (name, country, signup_date) 
 VALUES ('Aisha','India', '2024-01-10'),
        ('Rahul', 'India', '2024-02-05'),
        ('Sophia', 'USA', '2024-01-20'),
        ('Liam', 'UK', '2024-03-01'),
        ('Mei', 'Singapore', '2024-02-15');                                

 INSERT INTO orders (customer_id, order_date, product_category, order_amount, payment_method)
 VALUES (1, '2024-03-01', 'Books', 1200, 'CARD'),
        (1, '2024-03-10', 'Electronics', 8500, 'UPI'),
        (2, '2024-03-05', 'Groceries', 1500, 'COD'),
        (2, '2024-03-18', 'Books', 700, 'CARD'),
        (3, '2024-03-12', 'Electronics', 9000, 'CARD'),
        (3, '2024-03-25', 'Fashion', 3000, 'CARD'),
        (4, '2024-03-07', 'Books', 900, 'UPI'),
        (4, '2024-03-21', 'Groceries', 2200, 'CARD'),
        (5, '2024-03-15', 'Electronics', 7500, 'UPI'),
        (5, '2024-03-28', 'Books', 1100, 'CARD');

INSERT INTO orders (customer_id, order_date, product_category, order_amount, payment_method)
 VALUES		(1, '2024-04-01', 'Books', 2000, 'CARD');
 */

SELECT * FROM CUSTOMERS
SELECT * FROM ORDERS



SELECT C.country,SUM(O.order_amount) order_amount
FROM customers C
INNER JOIN orders O on C.customer_id=O.customer_id
GROUP BY C.country
ORDER BY order_amount desc


SELECT order_date,count(order_id) order_count_datewise
FROM orders
GROUP BY order_date