-- Name: Rabbia Munir
-- Data Analytics Bootcamp
-- Capstone Project 2 Maven Fuzzy Factory
-- Date: 18-05-2025


-- SALES ANALYSIS 
-- no. of sales, total revenue & total margin generated
CREATE VIEW SalesAnalysis AS
SELECT 
	   YEAR(created_at) AS year,
	   MONTH(created_at) AS month,
       COUNT(DISTINCT order_id) AS NumberofSales,
       SUM(price_usd) AS TotalRevenue,
       SUM(price_usd - cogs_usd) AS TotalProfit
FROM order_items
GROUP BY 1,2;


-- How much revenue each product is generating?
CREATE VIEW SalesAnalysis_eachproduct AS
SELECT oi.product_id,
       product_name,
	   COUNT(order_id) AS orders,
       SUM(price_usd) AS TotalRevenue,
       SUM(price_usd - cogs_usd) AS TotalProfit,
       AVG(price_usd) AS AverageOrderValue
FROM order_items oi
join products p
on oi.product_id = p.product_id
GROUP BY 1, 2
ORDER BY TotalProfit DESC;


 -- Query: Total Refund Amount (USD) by Product
Create view refundamount_by_product as
SELECT 
	p.product_id,
    p.product_name,
    SUM(oir.refund_amount_usd) AS total_refund_amount_usd
FROM order_item_refunds oir
JOIN order_items oi
  ON oir.order_item_id = oi.order_item_id
JOIN products p
  ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_refund_amount_usd DESC;


-- where the bulk of our website sessions are coming from? 
-- breakdown by UTM source, campaign and referring domain
CREATE VIEW session_traffic_breakdown AS
With ranks as (
select  year(created_at) as years, 
extract(month from created_at) as month_no, monthname(created_at) as month_name, 
utm_source , utm_campaign, http_referer, count(website_session_id) as session_count 
from website_sessions ws 
group by years,month_no, month_name, utm_source, utm_campaign, http_referer
order by years, month_no , session_count desc
)
select years, month_no, month_name , utm_source, utm_campaign, http_referer, session_count,
rank() over(partition by years,month_no order by session_count desc) as ranks
from ranks;

-- Monthly session and order trends for top traffic source.
CREATE VIEW session_traffic_breakdown AS
WITH RANK2 AS (SELECT  YEAR(ws.created_at) as years, 
EXTRACT(MONTH FROM ws.created_at) as month_no, 
MONTHNAME(ws.created_at) AS month_name, 
utm_source, utm_campaign, http_referer, device_type, 
COUNT(ws.website_session_id) AS session_count, COUNT(DISTINCT order_id) AS Orders_placed
FROM website_sessions ws 
LEFT JOIN orders o
ON ws.website_session_id = o.website_session_id 
GROUP BY years,month_no, month_name, utm_source, utm_campaign, http_referer, device_type
ORDER BY years,month_no),
RANK3 AS
(SELECT years, month_no, month_name , utm_source, utm_campaign, http_referer, device_type, Session_count, Orders_placed,
DENSE_RANK() OVER (PARTITION BY years,month_no ORDER BY Session_count DESC) AS ranks
FROM RANK2)
SELECT years, month_no, month_name, utm_source, utm_campaign, http_referer, device_type, Session_count, Orders_placed
FROM RANK3
GROUP BY years, month_no, month_name, utm_source, utm_campaign, http_referer, device_type, Session_count, Orders_placed;

-- top traffic source
WITH ranks AS (
SELECT  
YEAR(created_at) as years, 
EXTRACT(MONTH FROM created_at) as month_no, 
MONTHNAME(created_at) AS month_name, 
utm_source, utm_campaign, http_referer,
COUNT(website_session_id) AS session_count 
FROM website_sessions ws 
GROUP BY years,month_no, month_name, utm_source, utm_campaign, http_referer
ORDER BY years, month_no , session_count DESC
),
RAN AS (SELECT years, month_no, month_name , utm_source, utm_campaign, http_referer, session_count,
RANK() OVER(PARTITION BY years,month_no ORDER BY session_count DESC) AS ranks
FROM ranks)
SELECT utm_source,utm_campaign, http_referer, COUNT(ranks) AS Bulk_of_utm_source
FROM RAN
WHERE ranks BETWEEN 1 AND 3
GROUP BY utm_source, utm_campaign, http_referer
ORDER BY Bulk_of_utm_source DESC;




-- Session to order conversion rate by month,
CREATE VIEW session_to_order_conv_rate AS
SELECT 
year(ws.created_at),
month(ws.created_at),
monthname(ws.created_at),
COUNT(DISTINCT ws.website_session_id) AS total_sessions,
COUNT(DISTINCT o.order_id) AS total_orders,
ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT ws.website_session_id) * 100, 2) AS conversion_rate
FROM website_sessions ws
LEFT JOIN orders o 
ON ws.website_session_id = o.website_session_id
GROUP BY year(ws.created_at), month(ws.created_at),  monthname(ws.created_at)
ORDER BY year(ws.created_at), month(ws.created_at);


-- Quarterly increase in CVR
CREATE VIEW Quarterlyinc_dec_CVR AS
with CVR as (
SELECT 
year (ws.created_at) as yearr,
quarter(ws.created_at) quarterr,
COUNT(DISTINCT ws.website_session_id) AS total_sessions,
COUNT(DISTINCT o.order_id) AS total_orders,
ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT ws.website_session_id)  , 4) AS conversion_rate
FROM website_sessions ws
LEFT JOIN orders o 
ON ws.website_session_id = o.website_session_id
group by  year (ws.created_at), quarter(ws.created_at)
ORDER BY  year (ws.created_at), quarter(ws.created_at)
)
Select *,
Lag(conversion_rate, 1, 0) over (order by yearr, quarterr) as Previous_CVR,
Conversion_rate - Lag(conversion_rate, 1, 0) over (order by yearr, quarterr) as Quarterly_inc ,
round(((Conversion_rate - Lag(conversion_rate, 1, 0) over (order by yearr, quarterr)) / Lag(conversion_rate, 1, 0) over (order by yearr, quarterr)) * 100 , 2) as Quarterly_inc_in_Percentage
from CVR; 


-- quarterly session to order conversion rate, revenue per order, revenue per session
CREATE VIEW session_to_order_conv_rate_revenueperorder_revenuepersession AS
SELECT 
YEAR(ws.created_at) AS yearr,
QUARTER(ws.created_at) AS quarterr,
COUNT(DISTINCT ws.website_session_id) AS total_sessions,
COUNT(DISTINCT o.order_id) AS total_orders,
ROUND(SUM(o.price_usd), 2) AS total_revenue_usd,
ROUND(COUNT(DISTINCT o.order_id) * 1.0 / COUNT(DISTINCT ws.website_session_id) * 100, 2) AS conversion_rate_percentage,
ROUND(SUM(o.price_usd) / COUNT(DISTINCT o.order_id), 2) AS revenue_per_order_usd,
ROUND(SUM(o.price_usd) / COUNT(DISTINCT ws.website_session_id), 2) AS revenue_per_session_usd
FROM website_sessions ws
LEFT JOIN orders o
  ON ws.website_session_id = o.website_session_id
GROUP BY YEAR(ws.created_at), QUARTER(ws.created_at)
ORDER BY yearr, quarterr;


-- quarterly view of order by UTM source, campaign
SELECT
  YEAR(ws.created_at) AS yearr,
  QUARTER(ws.created_at) AS quarterr,
  ws.utm_source,
  ws.utm_campaign,
  COUNT(DISTINCT o.order_id) AS total_orders
FROM website_sessions ws
LEFT JOIN orders o
  ON ws.website_session_id = o.website_session_id
GROUP BY
  YEAR(ws.created_at),
  QUARTER(ws.created_at),
  ws.utm_source,
  ws.utm_campaign
ORDER BY
  yearr,
  quarterr,
  total_orders DESC;


-- quarterly session to order conversion rate by UTM source, campaign
CREATE VIEW Quarterly_order_cvr_utmsource_compaign AS
SELECT
  YEAR(ws.created_at) AS yearr,
  QUARTER(ws.created_at) AS quarterr,
  ws.utm_source,
  ws.utm_campaign,
  COUNT(DISTINCT ws.website_session_id) AS total_sessions,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(
    COUNT(DISTINCT o.order_id) * 100.0 / NULLIF(COUNT(DISTINCT ws.website_session_id), 0),
    2
  ) AS conversion_rate_percentage
FROM website_sessions ws
LEFT JOIN orders o
  ON ws.website_session_id = o.website_session_id
GROUP BY
  YEAR(ws.created_at),
  QUARTER(ws.created_at),
  ws.utm_source,
  ws.utm_campaign
ORDER BY
  yearr,
  quarterr,
  conversion_rate_percentage DESC;
  
  
  -- Bounce rate & Landing page Performance
  -- Bounce = when someone only views one page in a session and leaves.
  -- Bounce Rate = (Bounced Sessions / Total Sessions) × 100
  CREATE VIEW Bouncerate_landingpage AS
WITH first_pageview_per_session AS (
  SELECT
    wp.website_session_id,
    wp.pageview_url AS landing_page
  FROM (
    SELECT
      website_session_id,
      MIN(website_pageview_id) AS first_pageview_id
    FROM website_pageviews
    GROUP BY website_session_id
  ) AS first_views
  INNER JOIN website_pageviews wp
    ON wp.website_session_id = first_views.website_session_id
    AND wp.website_pageview_id = first_views.first_pageview_id
),
bounced_sessions AS (
  SELECT
    wp.website_session_id
  FROM website_pageviews wp
  GROUP BY wp.website_session_id
  HAVING COUNT(wp.website_pageview_id) = 1
)
SELECT
  fp.landing_page,
  COUNT(DISTINCT fp.website_session_id) AS total_sessions,
  COUNT(DISTINCT o.order_id) AS total_orders,
  COUNT(DISTINCT b.website_session_id) AS total_bounces,
  ROUND(
    COUNT(DISTINCT b.website_session_id) * 100.0 / COUNT(DISTINCT fp.website_session_id),
    2
  ) AS bounce_rate_percentage
FROM first_pageview_per_session fp
LEFT JOIN bounced_sessions b
  ON fp.website_session_id = b.website_session_id
LEFT JOIN orders o
  ON fp.website_session_id = o.website_session_id
GROUP BY fp.landing_page
ORDER BY total_sessions DESC;


-- Analyzing the revenue generated by each landing page

-- Step 1: Get the first pageview_id for each session
CREATE VIEW revenue_by_landing_page AS
WITH first_page_only AS (
  SELECT 
    website_session_id,
    MIN(website_pageview_id) AS first_page_id
  FROM website_pageviews
  GROUP BY website_session_id
),

-- Step 2: Join to get the actual landing page URL
landing_pages AS (
  SELECT 
    wp.website_session_id,
    wp.pageview_url
  FROM website_pageviews wp
  JOIN first_page_only fp
    ON wp.website_session_id = fp.website_session_id
   AND wp.website_pageview_id = fp.first_page_id
),

-- Step 3: Aggregate performance metrics per landing page
landing_page_performance AS (
  SELECT 
    lp.pageview_url AS landing_page,
    COUNT(DISTINCT lp.website_session_id) AS total_sessions,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.price_usd) AS total_revenue
  FROM landing_pages lp
  LEFT JOIN orders o
    ON lp.website_session_id = o.website_session_id
  GROUP BY lp.pageview_url
)

-- Step 4: Final output
SELECT 
  landing_page,
  total_sessions,
  total_orders,
  total_revenue
FROM landing_page_performance
ORDER BY total_revenue DESC;



-- full conversion funnel from all landing pages to orders

-- This query analyzes website session behavior by identifying:

-- Where the session landed (e.g., homepage, specific landing pages).

-- What product or funnel pages were viewed during the session.

-- Summarizes how many sessions progressed through various stages of the funnel.




-- Flags each pageview with binary indicators (1 or 0) based on the pageview_url.
SET SESSION MAX_EXECUTION_TIME = 100000;  -- 60 seconds
CREATE TABLE conversion_funnel AS
WITH flagged AS (
   SELECT 
        wp.website_session_id AS session_id, 
        wp.website_pageview_id,
		wp.pageview_url, 
		CASE WHEN wp.pageview_url = '/home' THEN 1 ELSE 0 END AS homepage,
		CASE WHEN wp.pageview_url = '/products' THEN 1 ELSE 0 END AS products_page,
		CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE 0 END AS mr_fuzzy_page, 
		CASE WHEN wp.pageview_url = '/cart' THEN 1 ELSE 0 END AS cart_page,
		CASE WHEN wp.pageview_url = '/shipping' THEN 1 ELSE 0 END AS shipping_page,
		CASE WHEN wp.pageview_url = '/billing' THEN 1 ELSE 0 END AS billing_page,
		CASE WHEN wp.pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END AS thankyou_page,
		CASE WHEN wp.pageview_url = '/lander-1' THEN 1 ELSE 0 END AS lander_1,
		CASE WHEN wp.pageview_url = '/billing-2' THEN 1 ELSE 0 END AS billing_2,
		CASE WHEN wp.pageview_url = '/the-forever-love-bear' THEN 1 ELSE 0 END AS forever_love_bear,
		CASE WHEN wp.pageview_url = '/lander-2' THEN 1 ELSE 0 END AS lander_2,
		CASE WHEN wp.pageview_url = '/lander-3' THEN 1 ELSE 0 END AS lander_3,
		CASE WHEN wp.pageview_url = '/the-birthday-sugar-panda' THEN 1 ELSE 0 END AS sugar_panda,
		CASE WHEN wp.pageview_url = '/lander-4' THEN 1 ELSE 0 END AS lander_4,
		CASE WHEN wp.pageview_url = '/lander-5' THEN 1 ELSE 0 END AS lander_5,
		CASE WHEN wp.pageview_url = '/the-hudson-river-mini-bear' THEN 1 ELSE 0 END AS hudson_river_bear
   FROM mavenfuzzyfactory.website_pageviews wp
),

session_level AS (
	select session_id, 
	MAX(homepage) AS saw_homepage,
    MAX(products_page) AS saw_products_page,
    MAX(mr_fuzzy_page) AS saw_mr_fuzzy_page,
    MAX(cart_page) AS saw_cart_page,
    MAX(shipping_page) AS saw_shipping_page,
    MAX(billing_page) AS saw_billing_page,
    MAX(thankyou_page) AS saw_thankyou_page,
    MAX(lander_1) AS saw_lander_1,
    MAX(billing_2) AS saw_billing_2,
    MAX(forever_love_bear) AS saw_forever_love_bear,
    MAX(lander_2) AS saw_lander_2,
    MAX(lander_3) AS saw_lander_3,
    MAX(sugar_panda) AS saw_sugar_panda,
    MAX(lander_4) AS saw_lander_4,
    MAX(lander_5) AS saw_lander_5,
    MAX(hudson_river_bear) AS saw_hudson_river_bear
    FROM flagged
	GROUP BY 1
),

landing_page AS (
	SELECT 
    	website_session_id,
    	MIN(website_pageview_id) AS landing_page_id
  	FROM mavenfuzzyfactory.website_pageviews
  	GROUP BY website_session_id
),

sort AS (
	SELECT 
		s.*, 
		p.pageview_url AS landing_page
	FROM session_level s
	LEFT JOIN landing_page l
	  ON s.session_id = l.website_session_id
	LEFT JOIN mavenfuzzyfactory.website_pageviews p
	  ON l.website_session_id = p.website_session_id 
	  AND l.landing_page_id = p.website_pageview_id
)

SELECT 
	CASE 
		when saw_homepage = 1 then 'saw_homepage'
		when saw_lander_1 = 1 then 'saw_lander_1'
		when saw_lander_2 = 1 then 'saw_lander_2'
		when saw_lander_3 = 1 then 'saw_lander_3'
		when saw_lander_4 = 1 then 'saw_lander_4'
		when saw_lander_5 = 1 then 'saw_lander_5'
		ELSE 'other'
	END AS landing_segment,

	COUNT(DISTINCT session_id) AS total_sessions,

	-- Product-related pageviews
	COUNT(DISTINCT CASE WHEN saw_products_page = 1 THEN session_id END) AS sessions_saw_products,
	COUNT(DISTINCT CASE WHEN saw_mr_fuzzy_page = 1 THEN session_id END) AS sessions_saw_mr_fuzzy,
	COUNT(DISTINCT CASE WHEN saw_forever_love_bear = 1 THEN session_id END) AS sessions_saw_forever_love_bear,
	COUNT(DISTINCT CASE WHEN saw_sugar_panda = 1 THEN session_id END) AS sessions_saw_sugar_panda,
	COUNT(DISTINCT CASE WHEN saw_hudson_river_bear = 1 THEN session_id END) AS sessions_saw_hudson_river_bear,

	-- Funnel pages
	COUNT(DISTINCT CASE WHEN saw_cart_page = 1 THEN session_id END) AS sessions_saw_cart,
	COUNT(DISTINCT CASE WHEN saw_shipping_page = 1 THEN session_id END) AS sessions_saw_shipping,
	COUNT(DISTINCT CASE WHEN saw_billing_page = 1 OR saw_billing_2 = 1 THEN session_id END) AS sessions_saw_billing,
	COUNT(DISTINCT CASE WHEN saw_thankyou_page = 1 THEN session_id END) AS sessions_saw_thankyou

FROM sort
GROUP BY 1
ORDER BY total_sessions DESC;









