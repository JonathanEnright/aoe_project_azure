import streamlit as st
import pandas as pd

def get_filters(transformed_df: pd.DataFrame, prefix):
    '''Splits the page into 2 for both multiselect filters.
        Saves the selection into a dictionary which is used to update session_state filters later.
        Reset button can be used as a shortcut which will reset all filters.    
    '''
    filter1, gap, filter2, gap2, filter3 = st.columns([5,1,5,1,5])
    with filter1:
        opponent_civ_select = st.multiselect(
            label='Select the opponent civ to counter against'
            ,options=sorted(set(transformed_df['opponent_civ'].tolist()))
            ,key=f"{prefix}_opponent_civ_{st.session_state.counter}"
            ,placeholder="ALL - (All values are applied)"
        )

    with filter2:
        map_select = st.multiselect(
            label='Select a map'
            ,options=sorted(set(transformed_df['map'].tolist()))
            ,key=f"{prefix}_map_{st.session_state.counter}"
            ,placeholder="ALL - (All values are applied)"
        )
    
    with filter3:
        match_elo_bucket_select = st.multiselect(
            label='Select the elo range to analyse'
            ,options=sorted(set(transformed_df['match_elo_bucket'].tolist()))
            ,key=f"{prefix}_match_elo_bucket_{st.session_state.counter}"
            ,placeholder="ALL - (All values are applied)"
        )

    
    st.session_state[f'{prefix}_opponent_civ_query'] = [el for el in opponent_civ_select]
    st.session_state[f'{prefix}_map_query'] = [el for el in map_select]
    st.session_state[f'{prefix}_match_elo_bucket_query'] = [el for el in match_elo_bucket_select]


def build_graphs(transformed_df):
    '''Filters the dataframe to only retain selected rows.
        Utilising some containserisation, create the graphs and data preview objects.
    '''
    filtered_df = transformed_df[transformed_df['selected'] == True]
    

        # Group and aggregate the data
    df_updated = (
        filtered_df.groupby(['civ', 'opponent_civ'])
        .agg(
            Matches_Played=('matches_played', 'sum'),
            Wins_against_Opponent=('wins', 'sum')
        )
        .reset_index()
    )

    # Calculate the win percentage
    df_updated['Win Percentage against Opponent Civ'] = (
        df_updated['Wins_against_Opponent'] / df_updated['Matches_Played'] * 100
    ).round(2)

    # Rename columns
    df_updated = df_updated.rename(columns={
        'civ': 'Civ',
        'opponent_civ': 'Opponent Civ',
        'Matches_Played': 'Matches Played',
        'Wins_against_Opponent': 'Wins against Opponent Civ'
    })

    

    tab1, tab2 = st.tabs(['Visual', 'Data'])
    with tab1:
        st.title("Civ Counter Picker")
        st.dataframe(df_updated, use_container_width=True)
    with tab2:
        left, right = st.columns(2)
        with left:
            st.write(transformed_df)
        with right:
            st.session_state

