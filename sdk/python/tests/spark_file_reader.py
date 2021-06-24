from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import pandas as pd
from datetime import datetime
from feast import FeatureStore

# NOT A Test only used as a demonstration of usage

store = FeatureStore(repo_path=".")

# with pandas
entity_df = pd.DataFrame.from_dict({
    "trip_id": [273392],
    "event_timestamp": [
        datetime(2016, 1, 1, 15, 54, 13),
    ]
})
entity_df['trip_id'] = entity_df['trip_id'].astype('int32')

training_df = store.get_historical_features(
    entity_df=entity_df, 
    feature_refs = [
        "trips:fare_amt",
        "trips:pickup_distance_jfk"
    ],
).to_df()

print(training_df.head())

# with spark
spark = SparkSession.builder.appName("Example").master("local[*]").getOrCreate()

entity_df_spark = spark.createDataFrame(entity_df)

# using to_engine with Spark implementation
training_df = store.get_historical_features(
    entity_df=entity_df_spark,
    feature_refs=[
        "trips:fare_amt",
        "trips:pickup_distance_jfk"],
).to_engine("feast.contrib.infra.offline_stores.spark.file.SparkFileRetrievalJob").to_df()

training_df.show(10, False)
