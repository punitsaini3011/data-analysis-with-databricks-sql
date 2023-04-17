# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Lab: Final Lab Assignment

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

    <h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
    Lesson Objective</h2>
    <div class="instructions-div">
    <p>At the end of this lesson, you will be able to:</p>
    <ul>
    <li>Use Databricks SQL to ingest data</li>
    <li>Query the ingested data</li>
    <li>Produce visualizations from the queries</li>
    <li>Create an Alert</li>
    <li>Produce a dashboard from the visualizations</li>
    </ul></div>
    
    """, statements=None) 

step.render()
step.execute()

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Scenario</h2>
    <div class="instructions-div">
    <p>Here is the scenario for the final lab assignment:</p>
    <ul>
        <li>You work for a large retail outlet, called Specialty Electronics Outlet</li>
        <li>The outlet sells to customers in the United States</li>
        <li>Data streams from various retail locations on a continuous basis</li>
        <li>Data Engineers are responsible for making this data ready for you to analyze</li>
        <li>Data Scientists use this same data for running machine learning algorithms to make predictions</li>
    </ul></div>
    
    """, statements=None) 

step.render()
step.execute() 

# COMMAND ----------

step = DA.publisher.add_step(False, instructions="""    

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Your Task</h2>
    <div class="instructions-div">
    <p>Here is what you should accomplish:</p>
    <ul>
        <li>The lab is configured as a set of step-by-step instructions that will guide you through all the requirements</li>
        <li>The outcome of the lab will be a dashboard that is ready to be shared with stakeholders</li>
        <li>You will also configure Alerts to keep stakeholders informed of significant events</li>
        <li>Lastly, you will make Refresh Schedules to ensure all data remains up-to-date</li>
    </ul></div>
    
    """, statements=None) 

step.render()
step.execute()

# COMMAND ----------

step = DA.publisher.add_validation(True, instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Ingest Data</h2>
    <div class="instructions-div">    
    <p>You have been asked to ingest sales data from a new data source. The source is a .json file that contains eight columns. The file contains header information and is located at <span class="monofont">wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v02/retail-org/sales_orders/sales_orders_json</span>. The final table should be in Delta format (the default) and should be called <span class="monofont">sales_orders</span>. Any intermediate tables should be dropped.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below</li>
        <li>Run the query in Databricks SQL</li>
        <li>Double-check that the <span class="monofont">sales_orders</span> table is in Delta format using the Data Explorer</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    </div>""", test_code="""USE {schema_name};
DROP TABLE IF EXISTS sales_orders_json;
CREATE TABLE sales_orders_json 
    USING FILL_IN 
    OPTIONS (
        path 'wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v02/retail-org/sales_orders/sales_orders_json',
        inferSchema "true"
);
DROP TABLE IF EXISTS sales_orders;
CREATE TABLE sales_orders AS
    SELECT * FROM FILL_IN;
DROP TABLE IF EXISTS sales_orders_json;
SELECT * FROM FILL_IN;


""", statements=["DROP TABLE IF EXISTS sales_orders_json;", """CREATE TABLE sales_orders_json 
    USING json 
    OPTIONS (
        path 'wasb://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v02/retail-org/sales_orders/sales_orders_json',
        inferSchema "true"
);""","DROP TABLE IF EXISTS sales_orders;", """CREATE TABLE sales_orders AS
    SELECT * FROM sales_orders_json;""", "DROP TABLE sales_orders_json;", "SELECT * FROM sales_orders;"], label="""How many rows are in the <span class="monofont">sales_orders</span> table? (type only numbers) """, expected="4074", length=10)

step.render()
step.execute()

# COMMAND ----------

step = DA.publisher.add_validation(True, instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Query the Ingested Data</h2>
    <div class="instructions-div">    
    <p>Now that we have sales data ingested, we have been asked to find those customers who have spent more than $175,000. Notice in the dataset that column <span class="monofont">ordered_products</span> is an array that contains json data. We need to explode the array in order to access the data inside. We need to access "price" and "qtY" and calculate the total amount spent on these items. We will then sum these amounts and group by the customers who purchased those products. We can do this all in one command by using CTEs.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below</li>
        <li>Run the query in Databricks SQL</li>
        <li>Save the query as "High Dollar Customers"</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    </div>""", test_code="""USE {schema_name};
FILL_IN order_info AS
    (SELECT customer_id, explode(ordered_products) AS product_info FROM sales_orders),
total_sales AS
    (SELECT customer_id, sum(product_info["price"] * product_info["qty"]) AS total_spent FROM order_info GROUP BY customer_id)
SELECT customer_name, total_spent FROM total_sales
    FILL_IN customers
    ON customers.customer_id = total_sales.customer_id
    FILL_IN total_spent > 175000
    FILL_IN BY total_spent DESC;


""", statements="""WITH order_info AS
    (SELECT customer_id, explode(ordered_products) AS product_info FROM sales_orders),
total_sales AS
    (SELECT customer_id, sum(product_info["price"] * product_info["qty"]) AS total_spent FROM order_info GROUP BY customer_id)
SELECT customer_name, total_spent FROM total_sales
    INNER JOIN customers
    ON customers.customer_id = total_sales.customer_id
    WHERE total_spent > 175000
    ORDER BY total_spent DESC;""", label="""How much has Bradsworth Digital Solutions, Inc spent? """, expected="209533", length=10)

step.render()
step.execute() 

# COMMAND ----------

step = DA.publisher.add_validation(True, instructions="""

<h3>Query Two</h3>
    <div class="instructions-div">    
    <p>What are the sales in each of the loyalty segments? We can answer this question by joining the <span class="monofont">sales_orders</span> table, the <span class="monofont">customers</span> table, and the <span class="monofont">loyalty_segments</span> table and using a CTE, the aggregate function <span class="monofont">sum()</span>, and a column expression that multiplies two columns together to calculate sales.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Make the required changes to the query below</li>
        <li>Run the query in Databricks SQL</li>
        <li>Save the query as "Sales in Loyalty Segments"</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    </div>""", test_code="""USE {schema_name};
FILL_IN sold_products AS
(SELECT *, explode(ordered_products) as products FROM sales_orders
    FILL_IN customers
        ON customers.customer_id = sales_orders.customer_id
    JOIN loyalty_segments
        ON customers.loyalty_segment = loyalty_segments.loyalty_segment_id)
SELECT loyalty_segment_description, FILL_IN(products['price'] * products['qty']) AS sales FROM sold_products
    GROUP BY loyalty_segment_description;
    


""", statements="""WITH sold_products AS
(SELECT *, explode(ordered_products) as products FROM sales_orders
    JOIN customers
        ON customers.customer_id = sales_orders.customer_id
    JOIN loyalty_segments
        ON customers.loyalty_segment = loyalty_segments.loyalty_segment_id)
SELECT loyalty_segment_description, sum(products['price'] * products['qty']) AS sales FROM sold_products
    GROUP BY loyalty_segment_description;""", label="""What is the sales amount for customers in loyalty segment 3? """, expected="9910450", length=10)

step.render()
step.execute() 

# COMMAND ----------

step = DA.publisher.add_validation(False, instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Produce Visualizations from the Queries</h2>
    <div class="instructions-div">    
    <p>Now, let's produce visualizations for our queries. Let's first tackle the "High Dollar Customers" query. The company has put together a promotion. They want to reward their high dollar customers. The first to reach $250,000 in sales will get 20% off for a year. The second closest will get 15% off, and the third will get 10% off. We want to create two visualizations from this query that will help us see the progress of this marketing strategy. We will first make changes to the table visualization (the default).</p>
    <p>Complete the following:</p>
    <ol>
        <li>Click "Edit Visualization"</li>
        <li>Click "customer_name" to open the settings for this column</li>
        <li>Update the column name to "Customer Name" to make it look better</li>
        <li>For "Description" type "These are our top customers."</li>
        <li>Check your work by entering your answer to the question</li>
        <li>Under "Font Conditions" click "Add Condition"</li>
        <li>Open the first dropdown and select "total_spent"</li>
        <li>Change "=" to ">", and input the value "200000" in the "Value" field</li>
        <li>Click the checkerboard color tile, and select red</li>
        <li>Click "total_spent" to open the settings for this column</li>
        <li>Update the name from "total_spent" to "Total Spent"</li>
        <li>Add the same font condition</li>
        <li>Click "Save" in the lower-right column</li>
    </ol>
    <p>The font conditions we set will change all rows that have a "Total Spent" greater than 200000 to red text. This will make them stand out.</p>
    <ol start="14">
        <li>Hover over the "+" symbol and click "Visualization"</li>
        <li>Select "Counter" as the visualization type</li>
        <li>For "Counter Label" type "Highest Sales Total"</li>
        <li>For "Counter Value Column" make sure the column "total_spent" is selected</li>
        <li>Click the "Format" tab</li>
        <li>Optional: Change the decimal character and thousands separator</li>
        <li>"total_spent" is a dollar figure, so add "$" to "Formatting String Prefix"</li>
        <li>Click "Save" in the lower-right corner</li>
        <li>Change the name of the visualization to "Highest Sales Total"</li>
    </ol>
    <p>Let's move to the "Sales in Loyalty Segments" query.</p>
    <ol start="23">    
        <li>Hover over the "+" symbol and click "Visualization"</li>
        <li>Select "Bar" as the visualization type</li>
        <li>For "X Column" select "loyalty_segment_description"</li>
        <li>For "Y Column" add "sales"</li>
        <li>Change the names to something that looks nicer on the "X Axis" and "Y Axis" tabs</li>
        <li>Click "Save" in the lower-right corner</li>
        <li>Change the name of the visualization to "Sales in Loyalty Segments"</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p></p>
    </div>


""", test_code=None, statements=None, label="""What is the name of the right-most tab in the Bar visualization editor? """, expected="data labels", length=10)

step.render()
step.execute()

# COMMAND ----------

step = DA.publisher.add_validation(False, instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Create an Alert</h2>
    <div class="instructions-div">    
    <p>Let's create an Alert with our "High Dollar Customers" query that will let us know when the threshold of $250,000 has been met.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Click "Alerts" in the sidebar menu</li>
        <li>Click "Create Alert"</li>
        <li>From the Query dropdown, select the query: "High Dollar Customers"</li>
        <li>Use the dropdown to change the "Value" column to <span class="monofont">total_spent</span> and change "Threshold" to 250000</li>
        <li>Change "Refresh" to "Every 1 day"</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p></p>
    </div>


""", test_code=None, statements=None, label="""Will this Alert trigger even though there is no refresh schedule configured for the query itself? """, expected="yes", length=10)

step.render()
step.execute() 

# COMMAND ----------

step = DA.publisher.add_validation(False, instructions="""

<h2><img class="image-icon-inline" src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/step-icon_small.png" alt="heading-icon" />
Produce a Dashboard from the Visualizations</h2>
    <div class="instructions-div">    
    <p>Now that we have configured a refresh schedule, let's set up an Alert that will notify us when the income generated from sales increases beyond a threshold.</p>
    <p>Complete the following:</p>
    <ol>
        <li>Click "Dashboards" in the sidebar menu</li>
        <li>Click "Create Dashboard"</li>
        <li>Name the dashboard "Loyalty Tracking"</li>
        <li>Click "Add Visualization"</li>
        <li>Click "High Dollar Customers"</li>
        <li>Ensure "Table" is selected in "Choose Visualization"</li>
        <li>Change "Title" to "High Dollar Customers"</li>
        <li>Optional: write a description</li>
        <li>Repeat steps 4-8 with the counter visualization we also added to "High Dollar Customers" and the bar chart in "Sales in Loyalty Segments"</li>
        <li>Click "Add Textbox" and type "# Loyalty Segment Tracking"</li>
        <li>Optional: Move the visualizations around by clicking and dragging each one</li>
        <li>Optional: Resize each visualization by dragging the lower-right corner of the visualization</li>
        <li>Optional: Click "Colors" to change the color palette used by visualizations in the dashboard</li>
        <li>Click "Done Editing" in the upper-right corner</li>
        <li>Run every query and refresh all visualizations all at once by clicking "Refresh"</li>
        <li>Check your work by entering your answer to the question</li>
        <li>After pressing <span class="monofont">ENTER/RETURN</span>, green indicates a correct answer, and red indicates incorrect</li>
    </ol>
    <p></p>
    </div>


""", test_code=None, statements=None, label="""If you wanted to configure a set interval to refresh this dashboard automatically, what button would you click? """, expected="schedule", length=10)

step.render()
step.execute()

# COMMAND ----------

DA.cleanup()
html = DA.publisher.publish()
displayHTML(html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
