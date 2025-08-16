from common import spark_session
import pandas as pd 
spark = spark_session("step4_time_travel")

# List snapshots
snapshots = spark.sql("SELECT * FROM local.airline.flights.snapshots").toPandas()
print("hello")
print(snapshots[['snapshot_id','committed_at','operation']])

# Pick the oldest snapshot id for demo
first_snapshot = snapshots.sort_values('committed_at').iloc[0]['snapshot_id']

print("\nData at first snapshot:")
df_old = spark.sql(f"SELECT * FROM local.airline.flights VERSION AS OF {int(first_snapshot)} ORDER BY scheduled_dep_ts")
df_old.show(truncate=False)

print("Step 4 done: time travel query completed.")
