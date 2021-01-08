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


sql_statement_second= """
SELECT
    store,
    item,
    CAST(date as date) as ds,
    SUM(sales) as y
  FROM Train
  GROUP BY store, item, ds
  ORDER BY store, item, ds
"""

store_item_history = (
  spark
    .sql(sql_statement_second)
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



######################################################
#
#sql_statement_third= """
#create table if not exists forecasts (
#  date date,
#  store integer,
#  item integer,
#  sales float,
#  sales_predicted float,
#  sales_predicted_upper float,
#  sales_predicted_lower float,
#  training_date date
#  )
#using delta
#partitioned by (training_date);"""
#
#
#sql_statement_fourth= """
#insert into forecasts 
#select 
#  ds as date,
#  store,
#  item,
#  y as sales,
#  yhat as sales_predicted,
#  yhat_upper as sales_predicted_upper,
#  yhat_lower as sales_predicted_lower,
#  training_date
#from new_forecasts;
#"""
#
#
#sql_statement_third_result = spark.sql(sql_statement_third)
#sql_statement_fourth_result = spark.sql(sql_statement_fourth)
#
#######################################################


# schema of expected result set
eval_schema =StructType([
  StructField('training_date', DateType()),
  StructField('store', IntegerType()),
  StructField('item', IntegerType()),
  StructField('mae', FloatType()),
  StructField('mse', FloatType()),
  StructField('rmse', FloatType())
  ])

# define udf to calculate metrics
@pandas_udf( eval_schema, PandasUDFType.GROUPED_MAP )
def evaluate_forecast( evaluation_pd ):
  
  # get store & item in incoming data set
  training_date = evaluation_pd['training_date'].iloc[0]
  store = evaluation_pd['store'].iloc[0]
  item = evaluation_pd['item'].iloc[0]
  
  # calulate evaluation metrics
  mae = mean_absolute_error( evaluation_pd['y'], evaluation_pd['yhat'] )
  mse = mean_squared_error( evaluation_pd['y'], evaluation_pd['yhat'] )
  rmse = sqrt( mse )
  
  # assemble result set
  results = {'training_date':[training_date], 'store':[store], 'item':[item], 'mae':[mae], 'mse':[mse], 'rmse':[rmse]}
  return pd.DataFrame.from_dict( results )

# calculate metrics
results = (
  spark
    .table('new_forecasts')
    .filter('ds < \'2018-01-01\'') # limit evaluation to periods where we have historical data
    .select('training_date', 'store', 'item', 'y', 'yhat')
    .groupBy('training_date', 'store', 'item')
    .apply(evaluate_forecast)
    )

results.createOrReplaceTempView('new_forecast_evals')



#######################################################
#
#sql_statement_fifth= """
#create table if not exists forecasts (
#  date date,
#  store integer,
#  item integer,
#  sales float,
#  sales_predicted float,
#  sales_predicted_upper float,
#  sales_predicted_lower float,
#  training_date date
#  )
#using delta
#partitioned by (training_date);"""
#
#
#
#sql_statement_sixth= """
#insert into forecasts 
#select 
#  ds as date,
#  store,
#  item,
#  y as sales,
#  yhat as sales_predicted,
#  yhat_upper as sales_predicted_upper,
#  yhat_lower as sales_predicted_lower,
#  training_date
#from new_forecasts;"""
#
#
#sql_statement_sixth_result = spark.sql(sql_statement_sixth)
#sql_statement_fifth_result = spark.sql(sql_statement_fifth)
#
########################################################
