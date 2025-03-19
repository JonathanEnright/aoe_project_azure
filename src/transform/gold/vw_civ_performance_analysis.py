"""
Script to export gold data to ADLS2 parquet files for reading into Streamlit.
This is simply to save costs, in production Streamlit would read from Databricks iteself. 
"""
from pyspark.sql.functions import col, when, countDistinct, desc, sum, asc, round, date_trunc, current_timestamp
from src.common.base_utils import create_adls2_session, create_databricks_session
from src.common.transform_utils import read_source_data
from src.common.load_utils import upload_to_adls2
from src.common.logging_config import setup_logging
import os
import pandas as pd
import io
import gzip


logger = setup_logging()

fact_table = 'aoe_dev.gold.fact_player_matches_gd'
match_table = 'aoe_dev.gold.dim_match_gd'
civ_table = 'aoe_dev.gold.dim_civ_gd'

STORAGE_ACCOUNT = 'jonoaoedlext' 
CONTAINER = 'dev'
DATA_ENVIRONMENT = 'consumption'
TARGET_TABLE = 'vw_civ_performance_analysis'


spark = create_databricks_session(catalog='aoe_dev', schema='gold')

def transform_dataframe(fact_table, match_table, civ_table):

    f = read_source_data(spark, fact_table)
    dm = read_source_data(spark, match_table)
    dc = read_source_data(spark, civ_table)

    df = f.join(dm, f.match_fk == dm.match_pk, 'inner') \
            .join(dc, f.civ_fk == dc.civ_pk, 'inner') \
            .select(
                dc.civ_name.alias('civ'),
                dm.game_date.alias('game_date'),
                dm.map.alias('map'),
                when(dm.avg_elo <= 600,'600 or less') \
                    .when(dm.avg_elo <= 800,'600-800') \
                    .when(dm.avg_elo <= 1000,'800-1000') \
                    .when(dm.avg_elo <= 1200,'1000-1200') \
                    .when(dm.avg_elo <= 1400,'1200-1400') \
                    .when(dm.avg_elo <= 1600,'1400-1600') \
                    .when(dm.avg_elo <= 1800,'1600-1800') \
                    .when(dm.avg_elo > 1800,'1801 or more') \
                    .otherwise('unknown') \
                    .alias('match_elo_bucket'),
                f.match_fk,
                f.winner
            ) 

    # Truncate game_date to a weekly level (week commencing), before aggregating
    df = df.withColumn('game_date', date_trunc('week', col('game_date')))

    trans_df = df.groupBy(
        'civ',
        'game_date',
        'map',
        'match_elo_bucket'
    ).agg(
        countDistinct('match_fk').alias('matches_played'), 
        sum(when(col('winner') == True, 1).otherwise(0)).alias('wins'),
        round(when(countDistinct('match_fk') == 0, 0).otherwise(
            sum(when(col('winner') == True, 1).otherwise(0)) / countDistinct('match_fk') * 100),2).alias('win_percentage')
    ).orderBy(asc('civ'), asc('map'), asc('match_elo_bucket'), asc('game_date'))

    # Load date time stamp
    trans_df = trans_df.withColumn("ldts", current_timestamp())
    
    return trans_df



def main():
    """
    Main ETL workflow:
      1. Apply transformations.
      2. Write the final data to the target table.
    """
    adls2 = create_adls2_session()
    
    file_name = f"{TARGET_TABLE}.csv.gz"
    
    trans_df = transform_dataframe(fact_table, match_table, civ_table)

    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = trans_df.toPandas()
    
    # Write the Pandas DataFrame to a BytesIO buffer as a compressed CSV
    with io.BytesIO() as buffer:
        with gzip.GzipFile(fileobj=buffer, mode='w') as gzip_buffer:
            pandas_df.to_csv(gzip_buffer, index=False)
        buffer.seek(0)
        
        # Upload the compressed CSV buffer to ADLS Gen2
        upload_to_adls2(adls2, buffer, STORAGE_ACCOUNT, CONTAINER, f"{DATA_ENVIRONMENT}/{file_name}")
    
    logger.info(f"Data for '{TARGET_TABLE}' now uploaded into {CONTAINER}/{DATA_ENVIRONMENT} as a compressed CSV file!")

    logger.info(f"Script '{os.path.basename(__file__)}' complete.")
    

if __name__ == "__main__":
    main()
