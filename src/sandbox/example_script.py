# This file represents a dbt'ish' version of models, but for pyspark .py transforms
# The surrounding imports, logging, spark connection, context, and target table has all been abstracted.
# Here, an analytic engineer can just focus on the transformation code only.
# NOTE: 'reff' is a simulation of dbts 'ref' macro.

example_source_table = "{{ reff('matches_br') }}"
second_example_source_table = "{{ reff('players_sr') }}"

df = spark.read.table(example_source_table)

NANOS_TO_SECS = 1_000_000_000

transformed_df = (
    df.withColumn("game_duration_secs", col("duration") / NANOS_TO_SECS)
        .withColumn("actual_duration_secs", col("irl_duration") / NANOS_TO_SECS)
        .withColumn("ts_seconds", col("started_timestamp") / float(NANOS_TO_SECS))
        .withColumn("game_started_timestamp", to_timestamp(col("ts_seconds")))
        .withColumn("game_date", to_date(col("game_started_timestamp")))
        .filter(col("leaderboard") == 'random_map')
)