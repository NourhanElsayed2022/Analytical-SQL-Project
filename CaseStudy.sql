-- 1/1
WITH quantity_sum AS (
SELECT  stockcode,  SUM(quantity) OVER(PARTITION BY stockcode  ) AS sum_quantity    -- sum of quantity for each stock code
FROM tableRetail)
SELECT DISTINCT  stockcode, sum_quantity
FROM quantity_sum
WHERE sum_quantity = (SELECT MAX(sum_quantity) FROM quantity_sum);     -- filter to include only rows with maximum sum of quantity
---------------------------------------------------------------
-- 1/2
WITH quantity_sum AS (
    SELECT 
        TO_CHAR(TO_DATE(invoicedate ,'MM/DD/YYYY HH24:MI'),'MM') AS mon,     -- extract month from invoice date
        stockcode,
        SUM(quantity) OVER(PARTITION BY TO_DATE(invoicedate ,'MM/DD/YYYY HH24:MI'), stockcode) AS sum_quantity     -- sum of quantity for each stock code in each month
    FROM  
        tableRetail
)
SELECT   mon,   stockcode,  sum_quantity AS max_sum_quantity_per_month
FROM  quantity_sum
WHERE   (mon, sum_quantity) IN (
 SELECT  mon, MAX(sum_quantity) AS max_sum_quantity_per_month   --  get Maximum sum of quantity for each month
 FROM  quantity_sum
GROUP BY  mon
    )
ORDER BY mon;
--------------------------------------------------------------------------------------
-- 1/3
SELECT DISTINCT
        TO_CHAR(TO_DATE(invoicedate ,'MM/DD/YYYY HH24:MI'),'HH24') AS Hours,     -- extract the hour  from the invoice date
       ROUND(AVG(quantity*price) OVER(PARTITION BY   TO_CHAR(TO_DATE(invoicedate ,'MM/DD/YYYY HH24:MI'),'HH24')),2)||'$' AS avg_sales  -- calculate the average sales for each hour
FROM  
        tableRetail
ORDER BY Hours;
------------------------------------------------------------------------------------
-- 1/4
SELECT DISTINCT
        TO_CHAR(TO_DATE(invoicedate ,'MM/DD/YYYY HH24:MI'),'MM') AS mon,      -- extract month from invoice date
        SUM(quantity*price) OVER(PARTITION BY  TO_CHAR(TO_DATE(invoicedate ,'MM/DD/YYYY HH24:MI'),'MM'))||'$' AS TOTAL_seles_per_month    -- calculate the total sales for each month
    FROM  
        tableRetail    
        ORDER BY MON;    
------------------------------------------------------------------------------------
-- 1/5
SELECT DISTINCT stockcode , ROUND(AVG(price) OVER(PARTITION BY stockcode),2)||'$' AS avg_price_for_product    -- calculate the average price for each product 
      FROM  
        tableRetail;
--------------------------------------------------------------------------------------
-- 1/6
WITH customer_sales AS (
    SELECT customer_id, SUM(price * quantity) ||'$' AS total_sales   -- calculate the total sales for each customer
    FROM  tableRetail
    GROUP BY customer_id
)
SELECT 
    customer_id, total_sales
FROM (
    SELECT customer_id,  total_sales, DENSE_RANK() OVER (ORDER BY total_sales DESC) AS sales_rank  -- assign a  rank to each customer based on total sales
    FROM customer_sales
)
WHERE sales_rank <= 10;     -- filter the result to include only the top 10 customers
-------------------------------------------------------------------------------------------------------
-- 2
WITH cus_data AS(
SELECT DISTINCT  customer_id , round ( ( SELECT MAX( TO_DATE (invoicedate,'MM/DD/YYYY HH24:MI') ) FROM tableretail ) - FIRST_VALUE(TO_DATE(invoicedate ,'MM/DD/YYYY HH24:MI') IGNORE NULLS) 
OVER(PARTITION BY customer_id ORDER BY TO_DATE(invoicedate ,'MM/DD/YYYY HH24:MI') DESC ) )AS recency ,   -- get the date for the last transaction for each customer 
COUNT(DISTINCT invoicedate ) OVER(PARTITION BY  customer_id ) AS frequency,  -- get the number of times the customer has bought from the store
SUM(price * quantity) OVER(PARTITION BY  customer_id ) AS monetary     -- the amount each customer has paid for our products 
FROM tableRetail
),
cus_score AS(
SELECT customer_id, recency,  frequency,  monetary, NTILE(5) OVER(ORDER BY recency desc ) AS r_score,  -- divide the recency into 5 groups
NTILE(5) OVER(ORDER BY frequency) AS f_score,      -- divide the frequency into 5 groups
NTILE(5) OVER(ORDER BY monetary) AS m_score    -- divide the monetary into 5 groups
FROM cus_data
    )
SELECT customer_id, recency,  frequency,  monetary, r_score, ROUND( (f_score+m_score)/2) AS fm_score , CASE      -- select the category based on these cases
                                                                                                                   WHEN r_score =5 and ROUND( (f_score+m_score)/2)  =5 THEN 'Champions'
                                                                                                                  WHEN r_score =4 and ROUND( (f_score+m_score)/2)  =5 THEN 'Champions'
                                                                                                                  WHEN r_score =5 and ROUND( (f_score+m_score)/2)  =4 THEN 'Champions'
                                                                                                                  WHEN r_score =5 and ROUND( (f_score+m_score)/2)  =2 THEN 'Potential Loyalists'
                                                                                                                  WHEN r_score =4 and  ROUND( (f_score+m_score)/2)  =2 THEN 'Potential Loyalists'
                                                                                                                  WHEN r_score =4 and  ROUND( (f_score+m_score)/2)  =3 THEN 'Potential Loyalists'
                                                                                                                  WHEN r_score =3 and  ROUND( (f_score+m_score)/2)  =3 THEN 'Potential Loyalists'
                                                                                                                  WHEN r_score =5 and ROUND( (f_score+m_score)/2)  =3 THEN 'Loyal Customers'
                                                                                                                  WHEN r_score =4 and  ROUND( (f_score+m_score)/2)  =4 THEN 'Loyal Customers'
                                                                                                                  WHEN r_score =3 and  ROUND( (f_score+m_score)/2)  =5 THEN 'Loyal Customers'
                                                                                                                  WHEN r_score =3 and  ROUND( (f_score+m_score)/2)  =4 THEN 'Loyal Customers'
                                                                                                                  WHEN r_score =5 and  ROUND( (f_score+m_score)/2)  =1 THEN 'Recent Customers'
                                                                                                                  WHEN r_score =4 and  ROUND( (f_score+m_score)/2)  =1 THEN 'Promising'
                                                                                                                  WHEN r_score =3 and  ROUND( (f_score+m_score)/2)  =1 THEN 'Promising'
                                                                                                                  WHEN r_score =3 and  ROUND( (f_score+m_score)/2)  =2 THEN 'Customers Needing Attention'
                                                                                                                  WHEN r_score =2 and  ROUND( (f_score+m_score)/2)  =3 THEN 'Customers Needing Attention'
                                                                                                                  WHEN r_score =2 and  ROUND( (f_score+m_score)/2)  =2 THEN 'Customers Needing Attention'
                                                                                                                  WHEN r_score =2 and  ROUND( (f_score+m_score)/2)  =5 THEN 'At Risk'
                                                                                                                  WHEN r_score =2 and  ROUND( (f_score+m_score)/2)  =4 THEN 'At Risk'
                                                                                                                  WHEN r_score =1 and  ROUND( (f_score+m_score)/2)  =3 THEN 'At Risk' 
                                                                                                                  WHEN r_score =1 and  ROUND( (f_score+m_score)/2)  =5 THEN 'Cant Lose Them'
                                                                                                                  WHEN r_score =1 and  ROUND( (f_score+m_score)/2)  =4 THEN 'Cant Lose Them'
                                                                                                                  WHEN r_score =1 and  ROUND( (f_score+m_score)/2)  =2 THEN 'Hibernating'
                                                                                                                  WHEN r_score =2 and  ROUND( (f_score+m_score)/2)  =1 THEN 'Hibernating'
                                                                                                                  WHEN r_score =1 and  ROUND( (f_score+m_score)/2)  =1 THEN 'Lost'
                                                                                                                  END AS group_name 
FROM cus_score
;
-----------------------------------------------------------------------------------------------------
-- 3/A
CREATE TABLE transactions ( Cust_Id number ,
                                           Calendar_Dt date ,
                                           Amt_LE  float );
                                           -----------------------
WITH gap_tab AS (
    SELECT 
        Cust_Id,  Calendar_Dt,
        LAG(Calendar_Dt, 1) OVER(PARTITION BY Cust_Id ORDER BY Calendar_Dt) AS pre_date,   -- get the previous date for each one
        Calendar_Dt - LAG(Calendar_Dt, 1) OVER(PARTITION BY Cust_Id ORDER BY Calendar_Dt) AS gap   -- calculate the difference or the gap between the Calendar_Dt and the pre_date"previous one"
    FROM 
        transactions
),
group_tab AS(
SELECT 
    Cust_Id, Calendar_Dt, pre_date, gap,
      SUM(CASE WHEN gap = 1 or gap is null THEN 0 ELSE 1 END) OVER(PARTITION BY Cust_Id ORDER BY Calendar_Dt) -- here we divide the consecutive days into groups
         AS grp
FROM 
    gap_tab),
 count_tab AS(
SELECT  Cust_Id, Calendar_Dt, pre_date, gap, grp ,COUNT(*) OVER(PARTITION BY Cust_Id , grp) AS cont     -- count the number of days in each group of days 
FROM group_tab
)
SELECT DISTINCT Cust_Id, MAX(cont) OVER(PARTITION BY Cust_Id) AS max_consecutive_days   -- calculate the max  consecutive days for each customer
FROM count_tab
ORDER BY Cust_Id
------------------------------------------------------------------------------------------------------- 
-- 3/B
WITH spend_tab AS(                                           
SELECT Cust_Id ,Calendar_Dt ,Amt_LE ,SUM(Amt_LE) OVER(PARTITION BY Cust_Id ORDER BY Calendar_Dt ) AS TOTAL_SPEND  -- calculate the total spend for each customer
FROM transactions
),
sh_spend AS(
-- Here we exclude the additional days of where customer exceed  250
SELECT Cust_Id,Calendar_Dt,Amt_LE,max(TOTAL_SPEND) over (partition by Cust_Id) as max_spend   -- calculate the maximum total spend for each customer
 FROM spend_tab 
 WHERE TOTAL_SPEND - Amt_LE < 250    -- filters the transactions to include only those where the difference between the total spend and the current transaction amount is less than 250
),
spend_days AS(
-- Here  we count of  calendar_dates(days) for each customer where their maximum spend is greater than or equal to 250.
SELECT  distinct  Cust_Id , COUNT(Calendar_Dt) OVER(PARTITION BY Cust_Id) AS TOTAL_DAYS  --calculate the number of  days for each customer
FROM sh_spend
where max_spend >=250          -- filters the customers based on their maximum spend being greater than or equal to 250
ORDER BY Cust_Id
)
SELECT ROUND(AVG(TOTAL_DAYS)) ||' Days'AS AVG_DAYS FROM spend_days ;  -- calculate the avg days that each customer take to reach a spent 250
