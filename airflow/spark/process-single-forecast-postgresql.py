import psycopg2
from connection import conn, cur
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import current_date
import pandas as pd
import matplotlib.pyplot as plt
import pickle
from fbprophet import Prophet
from datetime import datetime, date
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt
import logging


sc = SparkContext('local')
spark = SparkSession(sc)

# structure of the training data set
train_schema = StructType([
  StructField('date', DateType()),
  StructField('store', IntegerType()),
  StructField('item', IntegerType()),
  StructField('sales', IntegerType())
  ])

# read the training file into a dataframe
train = spark.read.csv(
  'dataset/train.csv', 
  header=True, 
  schema=train_schema
  )

train.show()
train.printSchema()

# make the dataframe queriable as a temporary view
train.createOrReplaceTempView('Train')


###############################################



# SQL can be run over DataFrames that have been registered as a table.

# # First sample query
query1= """
SELECT
  year(date) as year, 
  sum(sales) as sales
FROM Train
GROUP BY year(date)
ORDER BY year
"""
firstquery = spark.sql(query1)
print("Spark dataframe answered to the first sample query would be:")
firstquery.show()

##############################################


# Second sample query
query2= """
SELECT 
  TRUNC(date, 'MM') as month,
  SUM(sales) as sales
FROM Train
GROUP BY TRUNC(date, 'MM')
ORDER BY month
"""
secondquery = spark.sql(query2)
print("Spark dataframe answered to the second sample query would be:")
secondquery.show()


##############################################

# query to aggregate data to date (ds) level
sql_statement_item_store= """
SELECT
    store,
    item,
    CAST(date as date) as ds,
    SUM(sales) as y
  FROM Train
  WHERE store=1 AND item=1
  GROUP BY store, item, ds
  ORDER BY store, item, ds
"""
# assemble dataset in Pandas dataframe
item_store = spark.sql(sql_statement_item_store).toPandas()

# drop any missing records
item_store = item_store.dropna()
print("Pandas dataframe from available data, aggrigated base on date, store, and item would be ", "" ,item_store)




# query to aggregate data to date (ds) level
sql_statement= """
SELECT
    CAST(date as date) as ds,
    sales as y
  FROM Train
  WHERE store=1 AND item=1
  ORDER BY ds
"""
# assemble dataset in Pandas dataframe
history_pd = spark.sql(sql_statement).toPandas()

# drop any missing records
history_pd = history_pd.dropna()
print("Pandas dataframe from available data, aggrigated base on date would be ", "" ,history_pd)


# disable informational messages from fbprophet
logging.getLogger('py4j').setLevel(logging.ERROR)


# set model parameters
model = Prophet(
  interval_width=0.95,
  growth='linear',
  daily_seasonality=False,
  weekly_seasonality=True,
  yearly_seasonality=True,
  seasonality_mode='multiplicative'
  )

# fit the model to historical data
model.fit(history_pd)

# Save trained model to pickle
with open('trained-model/trained-model.pickle', 'wb') as f:
    pickle.dump(model, f)

# uncomment this code for later use of saved model through the pickle file
#with open('mypickle.pickle') as f:
#    loaded_obj = pickle.load(f)


# define a dataset including both historical dates & 90-days beyond the last available date
future_pd = model.make_future_dataframe(
  periods=90, 
  freq='d', 
  include_history=True
  )

# predict over the dataset
forecast_pd = model.predict(future_pd)
print("Prediction based on the historical model will be:" ,forecast_pd)


predict_fig = model.plot( forecast_pd, xlabel='date', ylabel='sales')

# adjust figure to display dates from last year + the 90 day forecast
xlim = predict_fig.axes[0].get_xlim()
new_xlim = ( xlim[1]-(180.0+365.0), xlim[1]-90.0)
predict_fig.axes[0].set_xlim(new_xlim)

print("Now you can see the figure which displaies dates from last year + the 90 day forecast. \
Please close the popup figure inorder to continue execution of the code.")
plt.show()


# get historical actuals & predictions for comparison
actuals_pd = history_pd[ history_pd['ds'] < pd.Timestamp(2018, 1, 1) ]['y']
predicted_pd = forecast_pd[ forecast_pd['ds'] < pd.Timestamp(2018, 1, 1) ]['yhat']

# calculate evaluation metrics
mae = mean_absolute_error(actuals_pd, predicted_pd)
mse = mean_squared_error(actuals_pd, predicted_pd)
rmse = sqrt(mse)

# print metrics to the screen
print("calculated evaluation metrics would be:")
print( '\n'.join(['MAE: {0}', 'MSE: {1}', 'RMSE: {2}']).format(mae, mse, rmse) )





######### Inserting final results into the PostgreSQL For the Item 1, Store 1: #########
# The code can be used for process-individual-stores-items.py as well.

######### Insert data into the Dim_Date table: #########
date_time = forecast_pd['ds']
for n in date_time:
  value_date = str(n.date())
  value_day = value_date[8:10]
  value_month = value_date[5:7]
  value_year = value_date[0:4]
  cur.execute("SELECT date FROM Dim_Date WHERE date = %s", (value_date,))
  if len(cur.fetchall()) > 0:
      pass
  else:
      query1 = "INSERT INTO Dim_Date (date, day, month, year) VALUES (%s, %s, %s, %s)"
      cur.execute(query1, (value_date, value_day, value_month, value_year))
print("Record created successfully in Dim_Date table")
conn.commit()


######### Insert data into the Dim_Store table: #########
store = item_store['store']
for n in store:
  value_store = str(n)
  cur.execute("SELECT store FROM Dim_Store WHERE store = %s", (value_store,))
  if len(cur.fetchall()) > 0:
      pass
  else:
      query2 = "INSERT INTO Dim_Store (store) VALUES (%s)"
      cur.execute(query2, (value_store))
print("Record created successfully in Dim_Store table")
conn.commit()



######### Insert data into the Dim_Item table: #########
item = item_store['item']
for n in item:
  value_item = str(n)
  cur.execute("SELECT item FROM Dim_Item WHERE item = %s", (value_item,))
  if len(cur.fetchall()) > 0:
      pass
  else:
      query2 = "INSERT INTO Dim_Item (item) VALUES (%s)"
      cur.execute(query2, (value_item))
print("Record created successfully in Dim_Item table")
conn.commit()


######### Insert data into the Fact_Sales table: #########
for index, row in forecast_pd.iterrows():
  date1 = row["ds"]
  value_date = str(date1)
  value_sales_predicted = str(row["yhat"])
  value_sales_predicted_upper = str(row["yhat_upper"])
  value_sales_predicted_lower = str(row["yhat_lower"])

  index_item_store = item_store.index
  index_item_store = len(index_item_store)

  selected_item = item_store[item_store.ds==date1]

  if index >= index_item_store:
    value_store_id = "1"
  else:
    value_store = str(selected_item.iloc[0]['store'])
    cur.execute("SELECT id FROM Dim_Store WHERE store = %s", (value_store,))
    rows = cur.fetchall()
    for r in rows:
      value_store_id = str(r[0]) 

  if index >= index_item_store:
    value_item_id = "1"
  else:
    value_item = str(selected_item.iloc[0]['item'])
    cur.execute("SELECT id FROM Dim_Item WHERE item = %s", (value_item,))
    rows = cur.fetchall()
    for r in rows:
      value_item_id = str(r[0]) 
  
  if index >= index_item_store:
    value_sales = None
  else:
    value_sales = str(selected_item.iloc[0]['y']) 

  cur.execute("SELECT store_id, date_id, item_id " \
  + "FROM Fact_Sales WHERE store_id = %s AND date_id = %s AND " \
  + "item_id = %s", (value_store_id, value_date, value_item_id))
  if len(cur.fetchall()) > 0:
    pass
  else:
    query5 = "INSERT INTO Fact_Sales (store_id, date_id, item_id, " \
    "sales, sales_predicted, sales_predicted_upper, sales_predicted_lower) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cur.execute(query5, (value_store_id, value_date, value_item_id, value_sales, \
    value_sales_predicted, value_sales_predicted_upper, value_sales_predicted_lower))

print("Record created successfully in Fact_Sales table")
conn.commit()

# Close the cursor
cur.close()
# Close the connection
conn.close()