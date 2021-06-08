import pandas as pd


def get_full_data(raw_data_path, return_missing_column_list=False):
    """
    Receives the path of the raw-data folder (ending in /raw_data) and returns the dataset of all years combined.
    If return_missing_column_list is True, returns a  dictionary of missing columns of format:
    Key = missing column name, Value = year(s) in which it existed
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

    # Set names to lowercase
    complete_data.name = complete_data.name.apply(lambda n: n.lower())

    # Correct game weeks for 2020
    complete_data['GW'] = complete_data['GW'].apply(corrector)


    # Import data about player positions
    raw21 = pd.read_csv(raw_data_path + '/2020-21/players_raw.csv',
                        encoding='utf_8')
    raw20 = pd.read_csv(raw_data_path + '/2019-20/players_raw.csv',
                        encoding='utf_8')
    raw19 = pd.read_csv(raw_data_path + '/2018-19/players_raw.csv',
                        encoding='utf-8')
    raw18 = pd.read_csv(raw_data_path + '/2017-18/players_raw.csv',
                        encoding='utf-8')
    raw17 = pd.read_csv(raw_data_path + '/2016-17/players_raw.csv',
                        encoding='utf-8')

    position_by_name = {  #Dictionary to map element_type to position
        1: 'GK',
        2: 'DEF',
        3: 'MID',
        4: 'FWD'
    }

    raw_seasons = [raw17, raw18, raw19, raw20, raw21]

    for season in raw_seasons:  #Iterate through the different raw files

        positions = map(lambda num: position_by_name[num],
                        season['element_type'])  #Map the positions
        season['position'] = list(positions)  #Assign the map to new columns
        season['name'] = season['first_name'] + ' '\
                            + season['second_name']  #Merge the first and second names of the raw players
        season['name'] = season['name'].str.lower()  #Set these names to lower
        for i, row in season.iterrows():  #Change caglar to çaglar
            if row['name'] == 'caglar söyüncü':
                season['name'].at[i] = 'çaglar söyüncü'

    #Change the position of the GK Danny Ward to MID so that we have duplicates that we can drop
    #We will change the position after dropping,
    #IMPORTANT DO NOT REMOVE THIS STEP WITHOUT REMOVING THE STEP OF CHANGING THE POSITION BACK AND VICE VERSA
    for i, row in raw19.iterrows():
        if row['name'] == 'Danny Ward' and row['position'] == 'GK':
            raw19['position'].at[i] = 'MID'

    #Here we split the data frame into the seasons and join the raw positions on the name
    df17 = complete_data.loc[complete_data['season'] == 17].copy()
    abc = raw17[['name', 'position']]
    abc.set_index('name', drop=True, inplace=True)
    df17 = df17.join(abc, on=['name'])

    df18 = complete_data.loc[complete_data['season'] == 18].copy()
    abc = raw18[['name', 'position']]
    abc.set_index('name', drop=True, inplace=True)
    df18 = df18.join(abc, on=['name'])

    df19 = complete_data.loc[complete_data['season'] == 19].copy()
    abc = raw19[['name', 'position']]
    abc.set_index('name', drop=True, inplace=True)
    df19 = df19.join(abc, on=['name'])

    df20 = complete_data.loc[complete_data['season'] == 20].copy()
    abc = raw20[['name', 'position']]
    abc.set_index('name', drop=True, inplace=True)
    df20 = df20.join(abc, on=['name'])

    df21 = complete_data.loc[complete_data['season'] == 21].copy()
    abc = raw21[['name', 'position']]
    abc.set_index('name', drop=True, inplace=True)
    df21 = df21.join(abc, on=['name'])

    #Reconcatenate the data into a complete dataset
    complete_data = pd.concat([df17, df18, df19, df20, df21])

    complete_data.drop_duplicates(inplace=True)  #Drop the duplicates (Danny Ward and Ben Davies)

    for i, row in complete_data.iterrows():  #Change the goalkeeper Danny Ward to the correct position.
        if row['name'] == 'danny ward' and row['element'] == 105:
            complete_data['position'].at[i] = 'GK'

    # Turn kickoff date into datetime, sort dataframe by name and kickoff date,
    complete_data.kickoff_time = pd.to_datetime(complete_data.kickoff_time)
    complete_data = complete_data.sort_values(by=['name', 'kickoff_time'],
                                              ascending=True)
    complete_data.reset_index(drop=True, inplace=True)
    complete_data['kickoff_date'] = complete_data['kickoff_time']\
                                    .apply(lambda d: d.date())
    complete_data['kickoff_time'] = complete_data['kickoff_time']\
                                    .apply(lambda d: d.time())

    complete_data.reset_index(drop = True)

    if return_missing_column_list:
        return complete_data, missing_columns
    return complete_data


# Function to correct gameweeks for 2020
def corrector(gw):
    if gw > 38:
        return gw - 9
    return gw
