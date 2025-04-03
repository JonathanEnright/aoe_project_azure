import altair as alt
import pandas as pd
import streamlit as st


def get_filters(transformed_df: pd.DataFrame, prefix):
    """Splits the page into 2 for both multiselect filters.
    Saves the selection into a dictionary which is used to update session_state filters later.
    Reset button can be used as a shortcut which will reset all filters.
    """
    filter1, gap, filter2, gap2, filter3 = st.columns([5, 1, 5, 1, 5])
    with filter1:
        civ_select = st.selectbox(
            label="Select a civ to view performance over time",
            options=sorted(set(transformed_df["civ"].tolist())),
            key=f"{prefix}_civ_{st.session_state.counter}",
            index=1,
        )

    with filter2:
        map_select = st.selectbox(
            label="Select a map",
            options=sorted(set(transformed_df["map"].tolist())),
            key=f"{prefix}_cp_map_{st.session_state.counter}",
            index=1,
        )

    with filter3:
        match_elo_bucket_select = st.selectbox(
            label="Select the elo range to analyse",
            options=sorted(set(transformed_df["match_elo_bucket"].tolist())),
            key=f"{prefix}_cp_match_elo_bucket_{st.session_state.counter}",
            index=1,
        )

    st.session_state[f"{prefix}_civ_query"] = [civ_select]
    st.session_state[f"{prefix}_map_query"] = [map_select]
    st.session_state[f"{prefix}_match_elo_bucket_query"] = [match_elo_bucket_select]


def build_graphs(transformed_df):
    """Filters the dataframe to only retain selected rows.
    Utilising some containserisation, create the graphs and data preview objects.
    """
    filtered_df = transformed_df[transformed_df["selected"] == True]

    # Group and aggregate the data
    df_updated = (
        filtered_df.groupby(["civ", "map", "match_elo_bucket", "game_date"])
        .agg(matches_played=("matches_played", "sum"), wins=("wins", "sum"))
        .reset_index()
    )

    # Calculate the win percentage
    df_updated["civ_win_percent"] = (
        df_updated["wins"] / df_updated["matches_played"] * 100
    ).round(2)

    # Rename columns
    df_updated = df_updated.rename(
        columns={
            "civ": "Civ",
            "game_date": "Match Date",
            "map": "Map",
            "match_elo_bucket": "Elo",
            "matches_played": "Matches Played",
            "wins": "Wins",
            "civ_win_percent": "Win %",
        }
    )

    # Ensure 'Match Date' is in datetime format
    df_updated["Match Date"] = pd.to_datetime(df_updated["Match Date"])

    # Create an Altair chart
    chart = (
        alt.Chart(df_updated)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "Match Date:T", title="Match Date", axis=alt.Axis(format="%Y-%m-%d")
            ),
            y=alt.Y("Win %:Q", title="Civ Win (%)"),
            tooltip=["Civ", "Map", "Elo", "Match Date", "Win %"],
        )
        .properties(width=600, height=400, title="Win Percentage Over Time")
    )

    tab1, tab2 = st.tabs(["Visual", "Data"])

    with tab1:
        st.title("Civ Performance (Win %)")
        st.altair_chart(chart, use_container_width=True)
    with tab2:
        st.title("Civ Performance - Data")
        st.dataframe(df_updated)
