#%% Imports
import pyodbc
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
from statsbombpy import sb #Statsbomb Github: https://github.com/statsbomb/statsbombpy
import time
import sqlalchemy
import urllib

#%% Connect to SQL server.

def connect_to_sql_server():
    """
    :param   server:       Name of SQL server you want to connect to.
    :param   database:     Name of database you want to connect to.
    :param   username:     Azure account username.
    :return: conn, cursor: Connection to server, cursor for server.
    """

    # Input server and database name.
    server = input("Enter server name:")
    database = input("Enter database name:")

    # Connection string is different depending on if connection is local or not.
    if server == "localhost":

        # Connection to server/database.
        conn_string = "Driver={ODBC Driver 18 for SQL Server}" + \
                      ";Server=" + server + \
                      ";Database=" + database + \
                      ";Trusted_Connection=yes" + \
                      ";TrustServerCertificate=yes"

    else:
        # Input username and password. Needed for non-local connections.
        username = input("Enter username:")

        # Connection to server/database.
        conn_string = 'DRIVER={ODBC Driver 18 for SQL Server}' \
                      ';SERVER=tcp:' + server +\
                      ';PORT=1433' +\
                      ';DATABASE=' + database + \
                      ';UID=' + username + \
                      ';Authentication=ActiveDirectoryInteractive;'

    # Connection to server.
    conn = pyodbc.connect(conn_string)

    # Cursor for database.
    cursor = conn.cursor()

    return conn, cursor


#%% Connect to SQL server using SQL alchemy.

def connect_to_sql_alchemy_server():
    """
        :param   server:   Name of SQL server you want to connect to.
        :param   database: Name of database you want to connect to.
        :param   username: Azure account username.
        :return: engine:   SQL alchemy engine connected to desired SQL server.
    """

    # Input server and database name.
    server = input("Enter server name:")
    database = input("Enter database name:")

    # Connection string is different depending on if connection is local or not.
    if server == "localhost":
        # Connection to server/database.
        params = urllib.parse.quote_plus("Driver={ODBC Driver 18 for SQL Server}" + \
                                          ";Server=" + server + \
                                          ";Database=" + database + \
                                          ";Trusted_Connection=yes" + \
                                          ";TrustServerCertificate=yes")
        conn_string = "mssql+pyodbc:///?odbc_connect={}".format(params)

        # Localhost can handle all rows being inserted at once, so fast_executemany is set to True.
        engine = sqlalchemy.create_engine(conn_string, echo=True, fast_executemany=True)

    else:
        # Input username and password. Needed for non-local connections.
        username = input("Enter username:")

        # Connection to server/database.
        params = urllib.parse.quote_plus('DRIVER={ODBC Driver 18 for SQL Server}' \
                                         ';SERVER=tcp:' + server + \
                                         ';PORT=1433' + \
                                         ';DATABASE=' + database + \
                                         ';UID=' + username + \
                                         ';Authentication=ActiveDirectoryInteractive;')
        conn_string = "mssql+pyodbc:///?odbc_connect={}".format(params)

        # Foreign SQL server can't handle all rows being inserted at once, so fast_executemany is set to False.
        engine = sqlalchemy.create_engine(conn_string, echo=True, fast_executemany=False)

    return engine


#%% Select table from SQL server.

def select_sql_table(table_schema, table_name, connection):
    """
    :param   table_schema: Schema for table/view in SQL.
    :param   table_name:   Name of table/view you want to load data from.
    :param   connection:   Connection to SQL server.
    :return: table_df:     Pandas dataframe of table from SQL.
    """
    # Define SQL query.
    input_query = f"""SELECT * FROM [{table_schema}].[{table_name}]
                      WHERE meta_is_current = 1"""

    # Initialise start time to see how long it takes to loop through one game.
    start_time = time.time()

    # Extract table from SQL server.
    table_df = pd.read_sql_query(sql=input_query, con=connection)

    # Finish time of extracting the table.
    end_time = time.time()

    print(f"Table/View loaded in {(end_time - start_time) / 60} minutes.")

    return table_df


# %% Load in a certain season from a certain league.

def SB_load_matches_from_season(league_name, season_name):
    """
    :param   league_name: Name of the league that you want data from (str) E.g. Premier League
    :param   season_name: Name of the season that you want data from (str) E.g. 2003/2004
    :return: matches_df:  Dataframe of all matches from selected season of selected league.
    """

    # The competitions that have Statsbomb data available.
    competitions_df = sb.competitions()

    # Extract SB competition id for desired competition.
    comp_id = competitions_df['competition_id'][
        (competitions_df['competition_name'] == league_name) & (competitions_df['season_name'] == season_name)].array[0]

    # Extract SB season id for desired season.
    seas_id = competitions_df['season_id'][
        (competitions_df['competition_name'] == league_name) & (competitions_df['season_name'] == season_name)].array[0]

    # Return the matches for the desired competition.
    matches_df = sb.matches(competition_id=comp_id, season_id=seas_id).sort_values(by='match_date')

    return matches_df


#%% Get all unique SB competition + season ids.

def SB_get_unique_competitions(sb_competitions_df):
    """
    :param  sb_competitions_df: The Statsbomb free competitions dataframe.
    :return comp_season_ids:    List of tuples containing all unique SB competition + season ids.
    """
    # Get a list of all unique competition + season ids in SB data.
    comp_season_ids = []
    for comp_id, seas_id in zip(sb_competitions_df.competition_id, sb_competitions_df.season_id):
        # Combine competition and season ids into one tuple.
        comp_season_tuple = (comp_id, seas_id)

        # Ensure only unique tuples are added to the list of competition and season ids.
        if comp_season_tuple not in comp_season_ids:
            comp_season_ids.append(comp_season_tuple)

    return comp_season_ids


#%% Get tuples with all unique SB game ids, competition ids, season ids and modified dates.

def SB_get_unique_games(comp_season_ids):
    """
    :param   comp_season_ids: List of tuples containing all unique SB competition + season ids.
    :return: sb_game_tuples:  List of tuples containing unique SB game ids, competition ids, season ids, and modified dates.
    :return: sb_game_ids:     List of all unique SB game ids.
    """
    # Get a list of tuples that include all the SB game ids, competition ids, season ids and modified date.
    sb_game_tuples = []
    sb_game_ids = []
    for comp_season_id in comp_season_ids:
        # Extract dataframe of games from specified competition + season.
        # Added try/except as one competition is not giving a dataframe on SB side.
        try:
            game_df = sb.matches(competition_id=comp_season_id[0], season_id=comp_season_id[1])

            # Add unique games to the list of game ids.
            for id, mod_date in zip(game_df.match_id, game_df.last_updated):
                if id not in sb_game_ids:
                    sb_game_ids.append(id)
                    sb_game_tuples.append((id, comp_season_id[0], comp_season_id[1], pd.to_datetime(mod_date)))

        except AttributeError:
            print(f"comp_id = {comp_season_id[0]}, season_id = {comp_season_id[1]} match dataframe does not exist.")

    return sb_game_tuples, sb_game_ids


#%% Get all unique SB event ids.

def SB_get_unique_events(sb_game_ids):
    """
    :param   sb_game_ids:   List containing all SB game ids.
    :return: unique_events: List of all unique event ids for all SB event data.
    """
    # Initialise list that will contain all unique SB event ids.
    unique_events = []

    # Loop through all SB unique game ids.
    for game_id in sb_game_ids:
        # Define events dataframe for specific game.
        events_df = sb.events(match_id=game_id)

        # Add all unique event ids to list.
        for event_id in events_df["id"]:
            if event_id not in unique_events:
                unique_events.append(event_id)

    return unique_events


#%% Extract value from SB event column.

def SB_extract_event_column_value(events_df, event_row, column_name, column_type):
    """
    :param:  events_df:   SB events dataframe for a specific match.
    :param:  event_row:   The row number for the event.
    :param:  column_name: Name of the column in the events_df.
    :param:  column_type: 7 choices for what column type the value is coming from ("T/F", "id", "str", "int", "float", "coords", "coords_z")
    :return: value:       Desired value from a specified column and row from event dataframe.
    """
    # Not all columns exist in every SB event dataframe, so KeyErrors occur when you attempt to use a column that
    # isn't present in the specified event dataframe.
    # If column type is True/False, we want values that don't exist to return 0, there are no 'False' entries in SB data, just nan.
    if column_type == "T/F":
        try:
            value = str(events_df.loc[event_row, column_name])
            if value == "nan":
                value = 0
            elif value == "True":
                value = 1
        except KeyError:
            value = 0

    # If column type is GUID id, we want values that don't exist to return None.
    elif column_type == "id":
        try:
            value = str(events_df.loc[event_row, column_name])
            if value == "nan":
                value = None
        except KeyError:
            value = None

    # If column type is a string, we want values that don't exist to return N/A.
    elif column_type == "str":
        try:
            value = str(events_df.loc[event_row, column_name])
            if value == "nan":
                value = "N/A"
        except KeyError:
            value = "N/A"

    # If column type is a float, we want values that don't exist to return 9999.
    elif column_type == "int":
        try:
            value = events_df.loc[event_row, column_name]
            if str(value) == "nan":
                value = -1 # Changed 14/01/24: Previously was 999999
            else:
                value = int(value)
        except KeyError:
            value = -1 # Changed 14/01/24: Previously was 999999

    # If column type is a float, we want values that don't exist to return 9999.
    elif column_type == "float":
        try:
            value = events_df.loc[event_row, column_name]
            if str(value) == "nan":
                value = -1 # Changed 14/01/24: Previously was 999999
            else:
                value = float(value)
        except KeyError:
            value = -1 # Changed 14/01/24: Previously was 999999

    # If column type is x and y coordinates, we want values that don't exist to return (9999, 9999).
    elif column_type == "coords":
        try:
            value = events_df.loc[event_row, column_name]
            if str(value) == "nan":
                value = (-1, -1) # Changed 14/01/24: Previously was (999999, 999999)
            else:
                value = (value[0], value[1])
        except KeyError:
            value = (-1, -1) # Changed 14/01/24: Previously was (999999, 999999)

    # If column type is x, y, z coordinates, we want values that don't exist to return (-1, -1, -1).
    # This is for shot end location qualifier, if shot is on target, z coord is included, not included otherwise.
    elif column_type == "coords_z":
        try:
            value = events_df.loc[event_row, column_name]
            if str(value) == "nan":
                value = (-1, -1, -1) # Changed 14/01/24: Previously was (999999, 999999, 999999)
            elif len(value) == 2:
                value = (value[0], value[1], -1) # Changed 14/01/24: Previously was (..., ..., 999999)
            else:
                value = (value[0], value[1], value[2])
        except KeyError:
            value = (-1, -1, -1) # Changed 14/01/24: Previously was (999999, 999999, 999999)

    else:
        print(f"Column type {column_type} is incorrect for {column_name}.")

    return value


#%% Get all SB event column names.

def get_sb_event_column_names():
    """
    :return: all_col_names: List of all column names that appear in all free SB events data.
    """
    # Load in Statsbomb (SB) competition data.
    sb_competitions = sb.competitions()

    # Get a list of all unique competition + season ids in SB data.
    comp_season_ids = SB_get_unique_competitions(sb_competitions_df=sb_competitions)

    # Get a list of tuples that include all the SB game ids, competition ids, season ids and modified date.
    sb_game_tuples, sb_game_ids = SB_get_unique_games(comp_season_ids=comp_season_ids)

    # Initiate empty df that all event dataframes will be appended to.
    all_col_names = []

    # Loop through each game id and extract its events.
    for game_id in sb_game_ids:
        events_df = sb.events(match_id=game_id)
        for col_name in events_df.columns:
            if col_name not in all_col_names:
                all_col_names.append(col_name)
                print(f"{col_name} from {game_id} added")

    return all_col_names


#%% Get all SB event column names.

def get_sb_max_length_columns(dict_of_str_colnames):
    """
    :param   dict_of_str_colnames: Dictionary containing all string columns in SB event data with 0 length.
    :return: dict_of_str_colnames: Updated dictionary with all the maximum characters that appeared for each column in the SB data.
    """
    # Load in Statsbomb (SB) competition data.
    sb_competitions = sb.competitions()

    # Get a list of all unique competition + season ids in SB data.
    comp_season_ids = SB_get_unique_competitions(sb_competitions_df=sb_competitions)

    # Get a list of tuples that include all the SB game ids, competition ids, season ids and modified date.
    sb_game_tuples, sb_game_ids = SB_get_unique_games(comp_season_ids=comp_season_ids)

    # Loop through each game id and inspect its events.
    for game_id in sb_game_ids:
        events_df = sb.events(match_id=game_id)

        # Loop through all SB str column names.
        for column_name in dict_of_str_colnames:
            # Sometimes a column name won't appear in every events dataframe.
            if column_name in events_df.columns:
                # Extract max length for column in specified events df.
                str_lengths = [len(str(entry)) for entry in events_df[column_name]]
                max_entry_length = max(str_lengths)
                # max_entry_length = events_df[column_name].str.len().max()

                # If this max length is greater than previously seen max lengths, it overwrites them.
                if max_entry_length > dict_of_str_colnames[column_name]:
                    dict_of_str_colnames[column_name] = max_entry_length
                    print(f"Max {column_name} value found in {game_id}:length {dict_of_str_colnames[column_name]}")

    return dict_of_str_colnames


# all_SB_str_cols = {"type": 0, "player": 0, "position": 0, "team": 0, "possession_team": 0, "pass_body_part": 0, "pass_height": 0,
#                    "pass_outcome": 0, "pass_recipient": 0, "pass_technique": 0, "pass_type": 0, "shot_body_part": 0, "shot_freeze_frame": 0,
#                    "shot_outcome": 0, "shot_technique": 0, "shot_type": 0, "dribble_outcome": 0, "duel_outcome": 0, "duel_type": 0,
#                    "foul_committed_card": 0, "foul_committed_type": 0, "clearance_body_part": 0, "goalkeeper_body_part": 0,
#                    "goalkeeper_outcome": 0, "goalkeeper_position": 0, "goalkeeper_technique": 0, "goalkeeper_type": 0,
#                    "substitution_outcome": 0, "substitution_replacement": 0, "bad_behaviour_card": 0, "ball_receipt_outcome": 0,
#                    "50_50": 0, "interception_outcome": 0, "play_pattern": 0, "related_events": 0, "tactics": 0}
# max_col_lengths = get_sb_max_length_columns(dict_of_str_colnames=all_SB_str_cols)

#%% Drop columns that do not contain useful data.

def drop_useless_columns(df):
    """
    :param   df: Pandas dataframe loaded from SQL database.
    :return: df: Same dataframe as inputted, but with columns that contain only default values deleted.
    """
    # Drop columns containing the substring 'meta'. ~ symbol corresponds with NOT.
    df = df.loc[:, ~df.columns.str.contains('meta')]

    # Initialise list of column names for columns that will be dropped.
    drop_columns = []

    # Loop through all column names for dataframe.
    for column_name in df.columns:
        # If all True/False columns are False, these columns should be dropped.
        if df[column_name].dtype == bool and (df[column_name] == 0).all():
            drop_columns.append(column_name)

        # If all numeric columns are -1, these columns should be dropped.
        elif df[column_name].dtype in ["int64", "float64"] and (df[column_name] == -1).all():
            drop_columns.append(column_name)

        # If all string (object when extracted from SQL) columns are N/A, these columns should be dropped.
        elif df[column_name].dtype == "object" and (df[column_name] == "N/A").all():
            drop_columns.append(column_name)

    # Drop the identified columns
    df.drop(columns=drop_columns, inplace=True)

    return df


#%% Convert coordinates from Opta to Statsbomb or vice-versa.

def convert_coords(value, input_data_source, output_data_source, x_or_y):
    """
    :param value:              Value that will be converted, can be integer/float or pandas column.
    :param input_data_source:  Source where inputted value comes from, can be "Statsbomb" or "Opta".
    :param output_data_source: What outputted value's data source should be, can be "Statsbomb" or "Opta".
    :param x_or_y:             Converting x or y coordinates. Different pitch dimensions for length and width.
    :return:
    """
    if input_data_source == "Opta" and output_data_source == "Statsbomb":
        # Length of Opta pitch is 100, SB is 120. Multiply by (120/100) = (6/5) = 1.2
        if x_or_y == "x":
            converted_value = value * (6 / 5)

        # Width of Opta pitch is 100, SB is 80. Multiply by (80/100) = (4/5) = 0.8
        elif x_or_y == "y":
            converted_value = value * (4 / 5)

        else:
            print("Conversion only available for x and y coordinates.")
            return

    elif input_data_source == "Statsbomb" and output_data_source == "Opta":
        # Length of SB pitch is 120, Opta is 100. Multiply by (100/120) = (5/6) = 0.833
        if x_or_y == "x":
            converted_value = value * (5 / 6)

        # Width of SB pitch is 80, Opta is 100. Multiply by (100/80) = (5/4) = 1.25
        elif x_or_y == "y":
            converted_value = value * (5 / 4)

        else:
            print("Conversion only available for x and y coordinates.")
            return

    else:
        print("Converting only valid from Opta to SB and vice-versa.")
        return

    return converted_value


