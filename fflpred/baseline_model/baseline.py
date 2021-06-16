import pandas as pd
import numpy as np
from merge_years.import_data import get_full_data

def mov_a_error(days = 2):
    '''
    Adds a "moving_a" columns to the dataframe inputed.
    How it works :
        1) creates a list of all players
        2.1) creates a dataframe with all games
        2.2) creates a "moving_a" column with the right value
        2.3) appends the player dataframe to a list of dataframes
        3) concatenates the list of dataframes into one dataframe
    '''
    # get full data
    df = get_full_data('../raw_data')
    
    rolled_df = df.groupby('name')
    rolled_df["moving_a"] = rolled_df["total_points"].rolling(days,closed='left').mean()
    rolled_df.reset_index(inplace=True)
    
    #players_list = df.name.unique()
    #players_df_with_ma = []
    #for player in players_list:
    #    unique_player_df = df[df["name"] == player]
    #    unique_player_df["moving_a"] = unique_player_df['total_points'].rolling(days,closed="left").mean()
    #    players_df_with_ma.append(unique_player_df)
    #output = pd.concat(players_df_with_ma)
    
    rolled_df['error'] = abs(rolled_df.moving_a - rolled_df.total_points)
    return rolled_df


def predict_next(player):
    '''
    baseline prediction of total_points for a player's next game (not played yet)
    '''
    data = mov_a_error()
    players_list = data.name.unique()
    ### Add some other If conditionners -> is he still playing in england inn 2021 etc? 
    if player in players_list:
        player_df = data[data["name"] == player].tail(3)
    else:
        return "Player Unknown"
    return player_df['total_points'].mean()