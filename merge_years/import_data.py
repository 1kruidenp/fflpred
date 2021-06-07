import pandas as pd

def get_full_data(raw_data_path, return_missing_column_list = False):
    """
    Receives the path of the raw-data folder (ending in /raw_data) and returns the dataset of all years combined
    and a dictionary of missing columns of format: Key = missing column name, Value = year(s) in which it existed
    """

    # Read raw_data into individual dataframes
    df21 = pd.read_csv(raw_data_path + '/2020-21/gws/merged_gw.csv',
                       encoding='utf_8')
    df20 = pd.read_csv(raw_data_path + '/2019-20/gws/merged_gw.csv',
                       encoding='utf_8')
    df19 = pd.read_csv(raw_data_path + '/2018-19/gws/merged_gw.csv',
                       encoding='ISO-8859-1')
    df18 = pd.read_csv(raw_data_path + '/2017-18/gws/merged_gw.csv',
                       encoding='ISO-8859-1')
    df17 = pd.read_csv(raw_data_path + '/2016-17/gws/merged_gw.csv',
                       encoding='ISO-8859-1')

    # Add a column identifying the season to each dataframe
    df21['season'] = 21
    df20['season'] = 20
    df19['season'] = 19
    df18['season'] = 18
    df17['season'] = 17

    # Create a list of all dataframes
    dflist = [df17, df18, df19, df20, df21]

    # Create a list of all columns existing in each season
    consistent_columns = []
    for column in list(df21.columns):
        if  column in list(df20.columns) and \
            column in list(df19.columns) and \
            column in list(df18.columns) and \
            column in list(df17.columns):
            consistent_columns.append(column)

    # Creating a dictionary with columns that are only in some years.
    # Key = column name, value = years for which the column exists.
    missing_columns = {}
    for df in dflist:
        for column in list(df.columns):
            if not column in consistent_columns:
                if column in missing_columns.keys():
                    missing_columns[column].append(df.loc[0, ['season']][0])
                else:
                    missing_columns[column] = [df.loc[0, ['season']][0]]

    # Concatenate seasons based on consistent columns
    frames_to_concat = [
        df21[consistent_columns], df20[consistent_columns],
        df19[consistent_columns], df18[consistent_columns],
        df17[consistent_columns]
    ]
    complete_data = pd.concat(frames_to_concat)

    # Remove trailing underscores and numbers from names
    complete_data.name = complete_data.name.str.rstrip('_1234567890')

    # Swap out underscores for spaces within names
    complete_data.name = complete_data.name.str.replace('_', ' ')

    # Drop column 'round' as it is the same as 'GW'
    complete_data.drop(columns=['round'], inplace=True)

    if return_missing_column_list:
        return complete_data, missing_columns
    return complete_data
