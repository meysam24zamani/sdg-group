import psycopg2
#from connection import conn, cursor
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import current_date
import pandas as pd
import matplotlib.pyplot as plt
import pickle
from fbprophet import Prophet
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt


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

#train.show()
#train.printSchema()

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
with open('trained-model.pickle', 'wb') as f:
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


################################

#Second part

################################

print("################################Second part################################")

sql_statement_Second= """
SELECT
    store,
    item,
    CAST(date as date) as ds,
    SUM(sales) as y
  FROM train
  GROUP BY store, item, ds
  ORDER BY store, item, ds
"""

store_item_history = (
  spark
    .sql(sql_statement_Second)
    .repartition(sc.defaultParallelism, ['store', 'item'])
  ).cache()


result_schema =StructType([
  StructField('ds',DateType()),
  StructField('store',IntegerType()),
  StructField('item',IntegerType()),
  StructField('y',FloatType()),
  StructField('yhat',FloatType()),
  StructField('yhat_upper',FloatType()),
  StructField('yhat_lower',FloatType())
  ])

print("store_item_history would be:")
store_item_history.show()

@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def forecast_store_item(history_pd):
  
  # TRAIN MODEL AS BEFORE
  # --------------------------------------
  # remove missing values (more likely at day-store-item level)
  history_pd = history_pd.dropna()
  
  # configure the model
  model = Prophet(
    interval_width=0.95,
    growth='linear',
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=True,
    seasonality_mode='multiplicative'
    )
  
  # train the model
  model.fit(history_pd)
  # --------------------------------------


# BUILD FORECAST AS BEFORE
  # --------------------------------------
  # make predictions
  future_pd = model.make_future_dataframe(
    periods=90, 
    freq='d', 
    include_history=True
    )
  forecast_pd = model.predict(future_pd)  
  # --------------------------------------


# ASSEMBLE EXPECTED RESULT SET
  # --------------------------------------
  # get relevant fields from forecast
  f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
  
  # get relevant fields from history
  h_pd = history_pd[['ds','store','item','y']].set_index('ds')
  
  # join history and forecast
  results_pd = f_pd.join( h_pd, how='left' )
  results_pd.reset_index(level=0, inplace=True)
  
  # get store & item from incoming data set
  results_pd['store'] = history_pd['store'].iloc[0]
  results_pd['item'] = history_pd['item'].iloc[0]
  # --------------------------------------

  # return expected dataset
  return results_pd[ ['ds', 'store', 'item', 'y', 'yhat', 'yhat_upper', 'yhat_lower'] ]



results = (
  store_item_history
    .groupBy('store', 'item')
    .apply(forecast_store_item)
    .withColumn('training_date', current_date() )
    )

results.createOrReplaceTempView('new_forecasts')


sql_statement_third= """
create table if not exists forecasts (
  date date,
  store integer,
  item integer,
  sales float,
  sales_predicted float,
  sales_predicted_upper float,
  sales_predicted_lower float,
  training_date date
  )
using delta
partitioned by (training_date);

insert into forecasts 
select 
  ds as date,
  store,
  item,
  y as sales,
  yhat as sales_predicted,
  yhat_upper as sales_predicted_upper,
  yhat_lower as sales_predicted_lower,
  training_date
from new_forecasts;
"""


sql_statement_third_result = spark.sql(sql_statement_third)
sql_statement_third_result.show()













#pandas_df = pd.read_csv('dataset/train.csv')
#print("pandas_df is", pandas_df)



########################################################
#conn.set_session(autocommit=True)

##Delete all tables and make the schema empty.
#querydrop = """
#DROP SCHEMA public CASCADE;
#CREATE SCHEMA public;
#GRANT ALL ON SCHEMA public TO admin;
#GRANT ALL ON SCHEMA public TO public;
#COMMENT ON SCHEMA public IS 'standard public schema';
#"""
#cursor.execute(querydrop)
#print("All tables are deleted successfully")
#print("-----------------------------------")
#print("-----------------------------------")

#query1 = "SELECT year(date) as year, sum(sales) as sales FROM %s GROUP BY year(date) ORDER BY year"
#cursor.execute(query1, (Train))
#print("Query on data-> Done")
#conn.commit()


## Close the cursor
#cursor.close()

## Close the connection
#conn.close()
################################################################