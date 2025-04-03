import time

import pandas as pd
import streamlit as st

from src.dashboards.aoe_app_civ_compare import (
    build_graphs as cc_build_graphs,
)
from src.dashboards.aoe_app_civ_compare import (
    get_filters as cc_get_filters,
)
from src.dashboards.aoe_app_civ_performance import (
    build_graphs as cp_build_graphs,
)
from src.dashboards.aoe_app_civ_performance import (
    get_filters as cp_get_filters,
)
from src.dashboards.aoe_app_leaderboard import (
    build_graphs as l_build_graphs,
)
from src.dashboards.aoe_app_leaderboard import (
    get_filters as l_get_filters,
)
from src.dashboards.aoe_app_utils import (
    get_data,
    initialize_state,
    query_data,
    reset_state_callback,
)

st.set_page_config(layout="wide")
# ----------------------------------------------------------------------------------------------------------------------
# Global vars to pass through to functions
storage_account = "jonoaoedlext"
container = "dev"

# Leaderboard Analysis
l_file_path = "consumption/vw_leaderboard_analysis.csv.gz"
l_categorical_filters = ["player_name", "country"]
l_prefix = "l"

# Opponent Civ Analysis
cc_file_path = "consumption/vw_opponent_civ_analysis.csv.gz"
cc_categorical_filters = ["opponent_civ", "map", "match_elo_bucket"]
cc_prefix = "cc"


# Civ Performance Analysis
cp_file_path = "consumption/vw_civ_performance_analysis.csv.gz"
cp_categorical_filters = ["civ", "map", "match_elo_bucket"]
cp_prefix = "cp"
# ----------------------------------------------------------------------------------------------------------------------


def reset_state_callback():
    """Reset session_date for filter variables and counter when button clicked."""
    st.session_state.counter = 1 + st.session_state.counter
    for cat_filter in [
        l_categorical_filters,
        cc_categorical_filters,
        cp_categorical_filters,
    ]:
        for col in cat_filter:
            st.session_state[f"{col}_query"] = []


def universal_layout(button_key, df):
    top1, gap, main_gap, gap2, top3 = st.columns([2, 1, 5, 1, 2])

    # Get max of ldts for Last Updated box
    df["ldts"] = pd.to_datetime(df["ldts"])
    max_ldts = df["ldts"].max()
    max_ldts_str = max_ldts.strftime("%Y-%m-%d, %I:%M %p")

    with top1:
        with st.container(height=75, border=True):
            st.write(f"Last updated: {max_ldts_str}")
    with main_gap:
        with st.container(height=75, border=False):
            st.markdown(
                "### :crossed_swords: **Age of Empires 2 Analysis** :crossed_swords:"
            )
    with top3:
        with st.container(height=75, border=False):
            st.button(
                "Reset All filters",
                on_click=reset_state_callback,
                use_container_width=True,
                key=button_key,
            )


def main():
    tab1, tab2, tab3 = st.tabs(
        ["Player Leaderboard", "Civ Counter-picker", "Civ Performance"]
    )

    with tab1:
        initialize_state(l_categorical_filters, l_prefix)
        df = get_data(storage_account, container, l_file_path)
        universal_layout("Player Leaderboard", df)
        l_get_filters(df, l_prefix)
        transformed_df = query_data(df, l_categorical_filters, l_prefix)
        l_build_graphs(transformed_df)

    with tab2:
        initialize_state(cc_categorical_filters, cc_prefix)
        df = get_data(storage_account, container, cc_file_path)
        universal_layout("Civ Counter-picker", df)
        cc_get_filters(df, cc_prefix)
        transformed_df = query_data(df, cc_categorical_filters, cc_prefix)
        cc_build_graphs(transformed_df)

    with tab3:
        initialize_state(cp_categorical_filters, cp_prefix)
        df = get_data(storage_account, container, cp_file_path)
        universal_layout("Civ Performance", df)
        cp_get_filters(df, cp_prefix)
        transformed_df = query_data(df, cp_categorical_filters, cp_prefix)
        cp_build_graphs(transformed_df)


print(f"The app has been run at {time.time()}")
main()


# General flow:
# 0. Universal layout, Create a Header, last_updated card, reset button, and sub-header for current page.       --UNIVERSAL
# 1. Initialise state for all session state variable. This includes creating a 'reset filters' button.          --UNIVERSAL
# 2. Get the data from adls2                                                                                    --UNIVERSAL in this project
# 3. Establish filters on the page (the `get_filters` function), this is unique to each page.                   --BESPOKE
# 4. The `query_data` ticks the rows to apply any filtering on the data dataframe (via a `selected` field)      --UNIVERSAL
#   NOTE: This is on the backend data of the df, not the one displayed just yet.
# 5. In `build_graphs` we apply the filtering from the `selected` field, and any other transformations.         --BESPOKE
#   It is also here we display the dataframe or visual required.
#   NOTE: This will be the end visual dataframe's data.
# 6. `updated_state` looks for any filters that have been applied, and reruns the script with the new filters.  --UNIVERSAL
