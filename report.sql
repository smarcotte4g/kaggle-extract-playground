SELECT 
    dp.product_line AS product_category,
    dd.year,
    dd.month,
    SUM(fs.total) AS total_sales,
    AVG(fs.gross_income) AS avg_gross_income,
    -- Ensures ranking resets for each month of the year
    RANK() OVER (PARTITION BY dd.year, dd.month ORDER BY SUM(fs.total) DESC) AS sales_rank
FROM 
    fact_sales fs
JOIN 
    dim_date dd ON fs.date_id = dd.date_id
JOIN 
    dim_product dp ON fs.product_id = dp.product_id
GROUP BY 
    dp.product_line, dd.year, dd.month
ORDER BY 
    dd.year, dd.month, sales_rank;