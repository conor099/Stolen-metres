#%% Imports.
import sys
sys.path.insert(0, "C:/Users/conor/OneDrive/Desktop/Football_fun")
import Custom_Functions as func
import time
import pandas as pd
import matplotlib.pyplot as plt
# matplotlib.use('Qt5Agg')  # Use the Qt5Agg backend for interactive plotting

#%% Connect to SQL server using SQL alchemy.

sql_engine = func.connect_to_sql_alchemy_server()

#%% Load SB and Opta throw-ins from SQL server.

fact_sb_throw_ins = func.select_sql_table(table_schema="Fact", table_name="SB_Throw_Ins", connection=sql_engine)
fact_opta_throw_ins = func.select_sql_table(table_schema="Fact", table_name="Opta_Throw_Ins", connection=sql_engine)

#%% Drop columns containing no data.

fact_sb_throw_ins = func.drop_useless_columns(df=fact_sb_throw_ins)
fact_opta_throw_ins = func.drop_useless_columns(df=fact_opta_throw_ins)


#%% Extract all SB throw-in events and the event preceding them.

def extract_throw_ins_and_preceding_event(throw_ins_df, data_source):
    """
    :param   throw_ins_df: Fact table for throw-ins as a pandas dataframe.
    :param   data_source:  String indicating if throws come from 'Statsbomb' or 'Opta'.
    :return  throw_ins_df: Same Fact table but with rows filtered out.
    """

    # Use SB or Opta indexes to order dataframe and extract throw-ins, depending on which data source you're looking for.
    if data_source == "Statsbomb":
        # Order dataframe and reset index to guarantee the order of the preceding events before a throw-in.
        throw_ins_df = throw_ins_df.sort_values(by=["dim_game_id", "sb_event_index"]).reset_index(drop=True)

        # Extract row indexes of all throw-ins. Convert to set as it's faster to check if a number is in a set rather than a list.
        throw_in_indexes = set(throw_ins_df.index[throw_ins_df["sb_pass_type"] == "Throw-in"])

    elif data_source == "Opta":
        # Order dataframe and reset index to guarantee the order of the preceding events before a throw-in.
        throw_ins_df = throw_ins_df.sort_values(by=["dim_game_id", "opta_event_index"]).reset_index(drop=True)

        # Extract row indexes of all throw-ins. Convert to set as it's faster to check if a number is in a set rather than a list.
        throw_in_indexes = set(throw_ins_df.index[throw_ins_df["opta_pass_throw_in"] == 1])

    else:
        print("Data source does not exist. Please select either Statsbomb or Opta")
        return

    # Extract row indexes of events just before throw-ins.
    events_before_throws = set([i - 1 for i in throw_in_indexes
                                if i >= 0
                                and i - 1 not in throw_in_indexes])
    two_events_before_throws = set([i - 2 for i in throw_in_indexes
                                    if i >= 0
                                    and i - 2 not in throw_in_indexes
                                    and i - 2 not in events_before_throws])
    three_events_before_throws = set([i - 3 for i in throw_in_indexes
                                      if i >= 0
                                      and i - 3 not in throw_in_indexes
                                      and i - 3 not in events_before_throws
                                      and i - 3 not in two_events_before_throws])

    # Initialise list of indexes for rows that need to be removed.
    row_indexes_to_delete = []

    # Loop throw all event row indexes.
    for index in throw_ins_df.index:
        # Remove events if they are 2 before a throw-in and the event before a throw-in is not filtered.
        if index in events_before_throws and index - 1 in two_events_before_throws and index - 1 >= 0:
            row_indexes_to_delete.append(index - 1)
            print(f"Indexes = {index - 1}. Row is 2 events before throw-in.")

        # Remove events if they are 3 before a throw-in and the event before a throw-in is not filtered.
        elif index in events_before_throws and index - 2 in two_events_before_throws and index - 2 >= 0:
            row_indexes_to_delete.append(index - 2)
            print(f"Indexes = {index - 2}. Row is 3 events before throw-in.")

        # Remove events 3 before a throw-in when the event 2 before a throw-in is not filtered and the event before a throw-in is filtered.
        elif index in two_events_before_throws and index - 1 in three_events_before_throws and index - 1 >= 0:
            row_indexes_to_delete.append(index - 1)
            print(f"Indexes = {index - 1}. Row is 3 events before throw-in and row before throw-in was filtered.")

    # Remove rows from the dataframe.
    throw_ins_df = throw_ins_df.drop(row_indexes_to_delete)

    return throw_ins_df


fact_sb_throw_ins = extract_throw_ins_and_preceding_event(throw_ins_df=fact_sb_throw_ins, data_source="Statsbomb")
fact_opta_throw_ins = extract_throw_ins_and_preceding_event(throw_ins_df=fact_opta_throw_ins, data_source="Opta")


#%% Add column to dataframe showing how far a player gained on a throw.

def add_throw_in_column(throw_ins_df, data_source):
    """
    :param throw_ins_df:  Fact table for throw-ins as a pandas dataframe.
    :param data_source:   String indicating if throws come from 'Statsbomb' or 'Opta'.
    :return throw_ins_df: Same Fact table but with column indicating how many metres were 'stolen' for a throw-in.
    """
    # Add empty column to dataframe with default value -1.0001.
    throw_ins_df["metres_gained"] = -1.0001

    # Use SB or Opta indexes to extract throw-ins and non-throw-ins, depending on which data source you're looking for.
    if data_source == "Statsbomb":
        # Extract row indexes of all throw-ins. Convert to set as it's faster to check if a number is in a set rather than a list.
        throw_in_indexes = set(throw_ins_df.index[throw_ins_df["sb_pass_type"] == "Throw-in"])
        non_throw_in_indexes = set(throw_ins_df.index[throw_ins_df["sb_pass_type"] != "Throw-in"])

    elif data_source == "Opta":
        # Extract row indexes of all throw-ins. Convert to set as it's faster to check if a number is in a set rather than a list.
        throw_in_indexes = set(throw_ins_df.index[throw_ins_df["opta_pass_throw_in"] == 1])
        non_throw_in_indexes = set(throw_ins_df.index[throw_ins_df["opta_pass_throw_in"] == 0])

    else:
        print("Data source does not exist. Please select either 'Statsbomb' or 'Opta'")
        return

    # Loop through all event row indexes.
    for index in throw_ins_df.index:
        # Only calculate metres gained for throw-ins that had passes as events before them.
        if index in throw_in_indexes and index - 1 in non_throw_in_indexes:
            # SB: Pitch coordinates flip if possession changes hands. If possession changes:
            if throw_ins_df["dim_team_id"][index] != throw_ins_df["dim_team_id"][index - 1] and data_source == "Statsbomb":
                distance_gained = round(throw_ins_df["sb_x_coord"][index] - (120 - throw_ins_df["sb_pass_end_x_coord"][index - 1]), 2)

            # SB: If possession doesn't change (pass event before was blocked/deflected):
            elif throw_ins_df["dim_team_id"][index] == throw_ins_df["dim_team_id"][index - 1] and data_source == "Statsbomb":
                distance_gained = round(throw_ins_df["sb_x_coord"][index] - throw_ins_df["sb_pass_end_x_coord"][index - 1], 2)

            # Opta: All 'Out' events before the throws have been guaranteed to be for the team that will take the throw.
            elif data_source == "Opta":
                # Convert Opta x and y coords to SB so that all the metres_gained is kept consistent
                throw_ins_df["converted_sb_x_coord"] = func.convert_coords(value=throw_ins_df["opta_x_coord"],
                                                                           input_data_source="Opta",
                                                                           output_data_source="Statsbomb", x_or_y="x")
                throw_ins_df["converted_sb_y_coord"] = func.convert_coords(value=throw_ins_df["opta_y_coord"],
                                                                           input_data_source="Opta",
                                                                           output_data_source="Statsbomb", x_or_y="y")
                throw_ins_df["converted_sb_end_x_coord"] = func.convert_coords(value=throw_ins_df["opta_pass_end_x_coord"],
                                                                               input_data_source="Opta",
                                                                               output_data_source="Statsbomb", x_or_y="x")
                throw_ins_df["converted_sb_end_y_coord"] = func.convert_coords(value=throw_ins_df["opta_pass_end_y_coord"],
                                                                               input_data_source="Opta",
                                                                               output_data_source="Statsbomb", x_or_y="y")
                distance_gained = round(throw_ins_df["converted_sb_x_coord"][index] - throw_ins_df["converted_sb_x_coord"][index - 1], 2)

            # Add the metres gained for each throw to the specific throw-in row.
            throw_ins_df.loc[index, "metres_gained"] = distance_gained

    return throw_ins_df


fact_sb_throw_ins = add_throw_in_column(throw_ins_df=fact_sb_throw_ins, data_source="Statsbomb")
fact_opta_throw_ins = add_throw_in_column(throw_ins_df=fact_opta_throw_ins, data_source="Opta")


#%% Define function to fill NaN values based on data type. Used for merging the dataframes below.

def fill_nan_values_based_on_datatype(df, guid_columns):
    """
    :param df:           Dataframe that you want to replace NaN values with something else.
    :param guid_columns: List of columns that are GUIDs. Need to specify these as they appear as objects/strings.
    :return: df:         Dataframe with all NaN values filled in.
    """
    for column in df.columns:
        # Fill with -1 if series is float/integer.
        if df[column].dtype in ["int64", "float64"]:
            df[column] = df[column].fillna(-1)

        # Fill with 'N/A' if series is a string.
        elif df[column].dtype == "object" and column not in guid_columns:
            df[column] = df[column].fillna('N/A')

        # Fill with '00000000-0000-0000-0000-000000000000' if series is a GUID. These appear as objects in Python.
        elif df[column].dtype == "object" and column in guid_columns:
            df[column] = df[column].fillna('00000000-0000-0000-0000-000000000000')

        # Fill with False if series is True/False.
        elif df[column].dtype == bool:
            df[column] = df[column].fillna(0)

    return df


#%% Merge the Opta and SB throw-ins into one dataframe.

def merge_sb_and_opta_throw_ins(sb_throw_ins, opta_throw_ins, guid_columns):
    """
    :param sb_throw_ins:   Pandas dataframe containing Statsbomb throw-ins and their preceding event.
    :param opta_throw_ins: Pandas dataframe containing Opta throw-ins and their preceding event.
    :param guid_columns:   List of columns that are GUIDs. Need to specify these as they appear as objects/strings.
    :return: merged_df:    Combined dataframe containing both dataframes.
    """
    # Copy both dataframes so that changes within the function don't affect the dataframes outside.
    sb_throw_ins = sb_throw_ins.copy()
    opta_throw_ins = opta_throw_ins.copy()

    # Loop through columns in sb_throw_ins. Makes sure that columns in Opta df match those in SB df and vice versa.
    for column in sb_throw_ins.columns:
        # If column is not already in opta_throw_ins and is in sb, then add column to opta.
        if column not in opta_throw_ins.columns:
            # Boolean columns are particularly annoying to add to a column, need to be specifically defined or they convert to objects.
            opta_throw_ins[column] = 0 if sb_throw_ins[column].dtype == 'bool' else pd.Series(dtype=sb_throw_ins[column].dtype)
            opta_throw_ins[column] = opta_throw_ins[column].astype(bool) if sb_throw_ins[column].dtype == 'bool' else opta_throw_ins[column]

    # Loop through columns in opta_throw_ins.
    for column in opta_throw_ins.columns:
        # If column is not already in sb_throw_ins and is in opta, then add column to sb.
        if column not in sb_throw_ins.columns:
            # Boolean columns are particularly annoying to add to a column, need to be specifically defined or they convert to objects.
            sb_throw_ins[column] = 0 if opta_throw_ins[column].dtype == 'bool' else pd.Series(dtype=opta_throw_ins[column].dtype)
            sb_throw_ins[column] = sb_throw_ins[column].astype(bool) if opta_throw_ins[column].dtype == 'bool' else sb_throw_ins[column]

    # Fill NaN values for each dataframe.
    sb_throw_ins = fill_nan_values_based_on_datatype(df=sb_throw_ins, guid_columns=guid_columns)
    opta_throw_ins = fill_nan_values_based_on_datatype(df=opta_throw_ins, guid_columns=guid_columns)

    # Combine the two dataframes.
    merged_df = pd.concat([sb_throw_ins, opta_throw_ins], axis=0, ignore_index=True)

    # Add meta_is_current column to merged dataframe.
    merged_df["meta_is_current"] = True

    return merged_df


fact_throw_ins = merge_sb_and_opta_throw_ins(sb_throw_ins=fact_sb_throw_ins, opta_throw_ins=fact_opta_throw_ins,
                                             guid_columns=["sb_event_id", "sb_pass_assisted_shot_id",
                                                           "sb_shot_key_pass_id"])


#%% Create table in SQL and fill with data.

def create_throw_ins_table_in_sql(engine, table_schema, table_name, throw_ins_df, guid_columns):
    """
    :param engine:       SQL alchemy engine.
    :param table_schema: Schema of table you want to create in SQL.
    :param table_name:   Name of table you want to create in SQL.
    :param throw_ins_df: Pandas dataframe that you want to insert in SQL.
    :param guid_columns: List of columns that are GUIDs. Need to specify these as they appear as objects/strings.
    :return:             Table is created in SQL database.
    """
    # Initialise start time to see how long it takes to delete rows.
    start_time = time.time()

    # Initiate dictionary to store datatypes of all the columns. This is to ensure that each column is correctly defined in the SQL server.
    data_types = {}

    # Loop through each column name and append each column's datatype to the dictionary.
    for column in throw_ins_df.columns:
        # Find data type of column being looped through.
        data_type = throw_ins_df[column].dtype

        # If data type is a string and not a GUID, then data type in SQL needs to be a NVARCHAR().
        if data_type == "object" and column not in guid_columns:
            max_length = throw_ins_df[column].astype(str).map(len).max()
            data_types[column] = f"NVARCHAR({max_length})"

        # If data type is a GUID, then data type in SQL needs to be a UNIQUEIDENTIFIER.
        elif data_type == "object" and column in guid_columns:
                data_types[column] = "UNIQUEIDENTIFIER"

        # If data type is a float, then data type in SQL needs to be a FLOAT.
        elif data_type == "float64":
            data_types[column] = "FLOAT"

        # If data type is an integer, then data type in SQL needs to be an INT.
        elif data_type == "int64":
            data_types[column] = "INT"

        # If data type is a date, then data type in SQL needs to be a DATETIME2.
        elif data_type == "datetime64[ns]":
            data_types[column] = "DATETIME2"

        # If data type is a bool, then data type in SQL needs to be a BIT.
        elif data_type == "bool":
            data_types[column] = "BIT"

        # Raise error if one of the columns has a weird datatype.
        else:
            print(column, data_type)
            raise ValueError(f"Column {column} has a datatype that has not been configured. Datetype = {data_type}")

    # Drop the table if it is already in the SQL database.
    engine.execute(f"DROP TABLE IF EXISTS {table_schema}.{table_name}")

    # Create a string representing column names and data types for SQL.
    sql_columns_str = ', '.join([f"{col} {col_type}" for col, col_type in data_types.items()])

    # Create the SQL table with specified columns and data types, along with meta columns.
    create_table_query = f"CREATE TABLE {table_schema}.{table_name} ({sql_columns_str}," \
                         f"meta_row_modified DATETIME2, " \
                         f"meta_valid_from DATETIME2, " \
                         f"meta_valid_to DATETIME2) " \
                         f"ALTER TABLE {table_schema}.{table_name} ADD  DEFAULT (getdate()) FOR [meta_row_modified] " \
                         f"ALTER TABLE {table_schema}.{table_name} ADD  DEFAULT (getdate()) FOR [meta_valid_from] " \
                         f"ALTER TABLE {table_schema}.{table_name} ADD  DEFAULT ('9999-01-01') FOR [meta_valid_to] "
    engine.execute(create_table_query)

    # Insert the DataFrame into the SQL database.
    throw_ins_df.to_sql(table_name, engine, schema=table_schema, if_exists="append", index=False)
    print(f"Table '{table_schema}.{table_name}' created successfully.")

    # Calculate time it takes to create table and insert data.
    end_time = time.time()
    print(f"Time taken to create table and insert data: {(end_time - start_time) / 60} minutes.")


create_throw_ins_table_in_sql(engine=sql_engine, table_schema="Fact", table_name="Throw_Ins",
                              throw_ins_df=fact_throw_ins,
                              guid_columns=["sb_event_id", "sb_pass_assisted_shot_id", "sb_shot_key_pass_id"])


#%% Plot all throws and preceding events.

def plot_throws_and_preceding(throw_ins_df):

    # Extract all throw-ins.
    throw_ins = throw_ins_df[(throw_ins_df["sb_pass_type"] == "Throw-in")&
                                (throw_ins_df["dim_game_id"] == 152)]

    non_throw_passes = throw_ins_df[(throw_ins_df["sb_pass_type"] != "Throw-in") &
                                           (throw_ins_df["dim_game_id"] == 152) &
                                           (throw_ins_df["sb_pass_end_x_coord"] != -1)]

    plt.scatter(throw_ins["sb_x_coord"], throw_ins["sb_y_coord"], marker="o")
    # plt.scatter(non_throws_or_passes["sb_x_coord"], non_throws_or_passes["sb_y_coord"], marker="o")
    plt.scatter(non_throw_passes["sb_pass_end_x_coord"], non_throw_passes["sb_pass_end_y_coord"], marker="o")

    # Annotate points with labels
    for index, row in throw_ins.iterrows():
        plt.text(row['sb_x_coord'], row['sb_y_coord'], row['sb_event_index'], ha='right', va='bottom')

    # for index, row in non_throws_or_passes.iterrows():
    #     plt.text(row['sb_x_coord'], row['sb_y_coord'], row['sb_event_index'], ha='right', va='bottom')

    for index, row in non_throw_passes.iterrows():
        plt.text(row['sb_pass_end_x_coord'], row['sb_pass_end_y_coord'], row['sb_event_index'], ha='right', va='bottom')

    plt.title(f"No. throws = {len(throw_ins)}. No. passes = {len(non_throw_passes)}.")
    plt.show()


# plot_throws_and_preceding(fact_sb_throw_ins)






