import pandas as pd
import locale
from unidecode import unidecode


# set locale for es, that would prevent the date error, bc the months are in Spanish
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')


# functions
# ---------------------------------------------------------------

def drop_cols(df, cols):
    cols_to_drop = cols

    for col in cols_to_drop:
        df = df.drop(col, axis=1)
    
    return df

def fragment_df(df, exercises):

    exercises_dfs = []

    for exercise in exercises:
        # select rows using the name of the exercise
        exercises_dfs.append( df[df['exercise_title'] == exercise] )

    return exercises_dfs

def process_na_column(df, cols):
    # reset index
    df = df.reset_index(drop=True)
    
    # iterate all cols
    for col in cols:
        # iterate df rows
        for index, row in df.iterrows():
            # check na
            if pd.isna(row[col]):
                # first row
                if index == 0:
                    next_non_na = None
                    # get the next value doing an iteration between index+1 (next_index) and the df length
                    for next_index in range(index + 1, len(df)):
                        # if its not a na, fill value and break loop
                        if not pd.isna(df.at[next_index, col]):
                            next_non_na = df.at[next_index, col]
                            # set the value
                            df.at[index, col] = next_non_na
                            break

                elif index > 0:
                    # get the prev value
                    prev_non_na = df.at[index-1, col]

                    next_non_na = None
                    # if col is not a text and the index is not the same as last row, use the prev and the following data to create the mean
                    if (col == 'weight_kg' or col == 'reps') and index != len(df):
                        # to get the next value, use the same as for index == 0
                       
                        for next_index in range(index + 1, len(df)):
                            # if its not a na, fill value and break loop
                            if not pd.isna(df.at[next_index, col]):
                                next_non_na = df.at[next_index, col]
                                # set the value
                                df.at[index, col] = next_non_na
                                break
                    # if we have a next no nan, create the mean       
                    if next_non_na:
                        mean = (prev_non_na + next_non_na) /2
                        df.at[index, col] = mean
                    else:
                        # set the value
                        df.at[index, col] = prev_non_na

    return df
# ---------------------------------------------------------------


# get file
df = pd.read_csv('1-input/workout_data.csv')

# drop the unused cols
cols_to_drop = ['title', 'end_time', 'description', 'set_type', 'superset_id', 'exercise_notes', 'set_index', 'distance_km', 'duration_seconds', 'rpe']

df = drop_cols(df, cols_to_drop)

# convert start_date into datetime type, be sure the months are the same language as the locale defined before
df['start_time'] = pd.to_datetime(df['start_time'], format='%d %b %Y, %H:%M')


# get a df for exercise using the list
# first create the list using the column 'exercise_title' dropping the rows with na exercise
exercises_list = df['exercise_title'].dropna().unique().tolist()

# fragment the big df into little df, 1 for exercise and save it into 'exercises_df_list'
exercises_df_list = fragment_df(df, exercises_list)

# save the list locally
with open('exercises_list.txt', 'w') as file:
    for exercise in exercises_list:
        # remove accents, convert to lowercase, and replace spaces with underscores
        collection_name = unidecode(exercise.lower().replace(' ', '_').replace('(', '').replace(')', ''))
        file.write(collection_name + '\n')


# process all cols, this scheme is used also in DataBricks to read the csv
scheme_cols = ['start_time', 'exercise_title', 'weight_kg', 'reps']

# For each exercise dataframe, process the NaN columns
for i in range(len(exercises_df_list)):
    # Replace the old df with the processed df (no need to overwrite the entire list)
    exercises_df_list[i] = process_na_column(exercises_df_list[i], scheme_cols)



# print the selected rows
counter = 0
for df in exercises_df_list:
    print(f'\n{df}')

    # get dates and exercise type to create the filename
    first_date = str(df.head(1)['start_time'].iloc[0]).split(' ')[0]

    last_date = str(df.tail(1)['start_time'].iloc[0]).split(' ')[0]

    # get the exercise
    exercise = unidecode(df.head(1)['exercise_title'][0].lower().replace(' ', '_').replace('(', '').replace(')', ''))


    # save into system to upload in your cloud 
    df.to_csv(f'2-output/{exercise}-{last_date}_{first_date}.csv', index=False)
