import pandas as pd
import streamlit as st


def get_filters(transformed_df: pd.DataFrame, prefix):
    """Display multi-select filters and update session state directly."""
    transformed_df["player_name"] = transformed_df["player_name"].astype(str)
    transformed_df["country"] = transformed_df["country"].astype(str)

    filter1, gap, filter2 = st.columns([5, 1, 5])
    with filter1:
        player_name_select = st.multiselect(
            label="Select the Player Name to lookup",
            options=sorted(set(transformed_df["player_name"].tolist())),
            key=f"{prefix}_player_name_query_{st.session_state.counter}",
            placeholder="ALL - (All values are applied)",
        )

    with filter2:
        country_select = st.multiselect(
            label="Select a Country to lookup",
            options=sorted(set(transformed_df["country"].tolist())),
            key=f"{prefix}_country_query_{st.session_state.counter}",
            placeholder="ALL - (All values are applied)",
        )

    # Update session state with selected values
    st.session_state[f"{prefix}_player_name_query"] = player_name_select
    st.session_state[f"{prefix}_country_query"] = country_select


def build_graphs(transformed_df):
    """Filters the dataframe to only retain selected rows.
    Utilising some containserisation, create the graphs and data preview objects.
    """
    filtered_df = transformed_df[transformed_df["selected"] == True]

    # Rename columns
    df_updated = filtered_df.rename(
        columns={
            "player_name": "Player Name",
            "rank": "Rank",
            "rating": "Rating",
            "country": "Country",
            "win_percentage": "Win Percent",
            "total_matches": "Total Matches",
            "wins": "Wins",
            "losses": "Losses",
            "last_played": "Last Played",
        }
    )

    df_updated = df_updated.drop("selected", axis=1)

    tab1, tab2 = st.tabs(["Visual", "Data"])

    with tab1:
        st.title("Aoe2 Weekly Leaderboard")
        st.dataframe(df_updated, use_container_width=True)
    with tab2:
        left, right = st.columns(2)
        with left:
            st.write(transformed_df)
        with right:
            st.session_state
