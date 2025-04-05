from setuptools import find_packages, setup

setup(
    name="aoe_azure",
    version="0.82",
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'elt_metadata=src.ingestion.elt_metadata:main',
            'elt_stat_matches=src.ingestion.elt_stat_matches:main',
            'elt_stat_players=src.ingestion.elt_stat_players:main',
            'elt_relic_api=src.ingestion.elt_relic_api:main',
            'upload_country_list=src.ingestion.upload_country_list:main',
            'matches_br=src.transform.bronze.matches_br:main',
            'players_br=src.transform.bronze.players_br:main',
            'dim_date_br=src.transform.bronze.dim_date_br:main',
            'relic_br=src.transform.bronze.relic_br:main',
            'country_list_br=src.transform.bronze.country_list_br:main',
            'dim_date_sr=src.transform.silver.dim_date_sr:main',
            'leaderboards_sr=src.transform.silver.leaderboards_sr:main',
            'matches_s2r=src.transform.silver.matches_s2r:main',
            'matches_sr=src.transform.silver.matches_sr:main',
            'player_leaderboard_stats_s2r=src.transform.silver.player_leaderboard_stats_s2r:main',
            'players_s2r=src.transform.silver.players_s2r:main',
            'players_sr=src.transform.silver.players_sr:main',
            'statgroup_sr=src.transform.silver.statgroup_sr:main',
            'dim_civ_gd=src.transform.gold.dim_civ_gd:main',
            'dim_date_gd=src.transform.gold.dim_date_gd:main',
            'dim_match_gd=src.transform.gold.dim_match_gd:main',
            'dim_player_gd=src.transform.gold.dim_player_gd:main',
            'fact_player_matches_gd=src.transform.gold.fact_player_matches_gd:main',
            'vw_civ_performance_analysis=src.transform.gold.vw_civ_performance_analysis:main',
            'vw_leaderboard_analysis=src.transform.gold.vw_leaderboard_analysis:main',
            'vw_opponent_civ_analysis=src.transform.gold.vw_opponent_civ_analysis:main'
        ]
    },
    package_data={
        "src.common": ["*.yaml"],
        "src.ingestion": ["*.yaml", "*.csv"],
        "src.transform": ["bronze/*.yaml"],
    },
)

