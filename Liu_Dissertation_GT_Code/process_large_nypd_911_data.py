import pandas as pd

#########################################################
def visual_data(df, city_name):
    import matplotlib.pyplot as plt
    from matplotlib.dates import MonthLocator, DateFormatter
    import pandas as pd
    df['year_month'] = pd.to_datetime(df['year_month'])
    # Create figure and axis objects
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot the data
    ax.plot(df['year_month'], df[f"{city_name}_MVT_911"], marker='o', linestyle='-')

    # Set labels and title
    ax.set_xlabel('Year-Month')
    ax.set_ylabel(f'{city_name} MVT 911 Count')
    ax.set_title(f'{city_name} MVT 911 Count Over Time')
    
    # Set date format for x-axis
    ax.xaxis.set_major_locator(MonthLocator())
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
    
    # Set x-axis limits to exclude empty months before and after real data
    min_date = min(df['year_month'])
    max_date = max(df['year_month'])
    ax.set_xlim(min_date, max_date)
    
    # Decrease font size of x-axis labels
    ax.tick_params(axis = 'x', labelsize = 7)  # Adjust fontsize here

    # Rotate x-axis labels for better readability
    plt.xticks(rotation = 80)

    # Show grid
    ax.grid(True)

    # Show plot
    plt.tight_layout()
    plt.show()
    
    
def remove_duplicated_calls(df, date_column, time_column, id_columns):
    
    df = df[~df[date_column].isna()]

   
    df[date_column] = pd.to_datetime(df[date_column])
    print(df.columns)
    # Sort the DataFrame by date_time
    df = df.sort_values(by=date_column)

    # Initialize a column for potential duplicates
    df['potential_duplicate'] = False

    # subset wanted columns
    subset_column_list = df.columns.tolist()
    
    # Columns to remove
    columns_to_remove = id_columns
    columns_to_remove.append(time_column)

    # Remove columns using list comprehension
    subset_column_list = [col for col in subset_column_list if col not in columns_to_remove]

    duplicated_df = df[df.duplicated(subset=subset_column_list, keep=False)]

    # Calculate the time difference in minutes between consecutive calls
    duplicated_df['time_diff'] = duplicated_df[time_column].diff().dt.total_seconds() / 60
    
    # Mark potential duplicates based on 5-minute intervals
    duplicated_df['duplicated_and_less_than_xx_minutes'] = (duplicated_df['time_diff'] <= 60)
    
    subset_column_list.append('duplicated_and_less_than_xx_minutes')
    print(subset_column_list)
    # filter out true duplicated ones, where they are duplicated in columns other than
    # date and id numbers, and duplicated within xx minutes, keep the first duplicated ones
    duplicated_df['unique_case_filtered_after_duplication'] = duplicated_df.duplicated(subset=subset_column_list, keep='first')
    # Filter out duplicates
    # Filter out the true duplicates from the original DataFrame
    filtered_df = df[~df.index.isin(duplicated_df.index)]
    
    unique_cases_to_add_back = duplicated_df[duplicated_df['unique_case_filtered_after_duplication']]

    # Append these unique cases to the filtered DataFrame
    final_df = pd.concat([filtered_df, unique_cases_to_add_back])

    # Drop the helper columns from the final filtered DataFrame if needed
    final_df = final_df.drop(columns=['time_diff', 'potential_duplicate', 'true_duplicate'], errors='ignore')

    percentage_of_duplicated = (len(df) - len(final_df))/len(df)*100
    
    raw_calls_number = len(df)
    
    duplicated_calls = len(df) - len(final_df)
    
    
    
    return final_df, percentage_of_duplicated, raw_calls_number, duplicated_calls


###########################################################


file_path = r"D:\Google Drive\0JJPHD\0 Dissertation Proposal\Data_Dissertation\city_level_911_dispatch_data\NYPD_cfs.csv"
chunk_size = 10 ** 6

# Initialize an empty list to store the chunk dataframes
chunk_list = []

# Iterate over the file in chunks
for chunk in pd.read_csv(file_path, chunksize=chunk_size):
    # Process the chunk as needed
    print("Chunk shape:", chunk.shape)
    
    # Append the chunk dataframe to the list
    chunk_list.append(chunk)

# Concatenate all the chunks into a single dataframe
full_df = pd.concat(chunk_list)


# Clean Data by year > 2017 <= 2022
full_df.INCIDENT_DATE = pd.to_datetime(full_df.INCIDENT_DATE, format = "%m/%d/%Y")
full_df["INCIDENT_DATE"].dt.year.head()
filtered_df = full_df[(full_df["INCIDENT_DATE"].dt.year >= 2017) & (full_df["INCIDENT_DATE"].dt.year <= 2022)]

#Filter MVT cases
filtered_df = filtered_df[filtered_df["TYP_DESC"].str.contains("LARCENY.*VEHICLE", na=False, regex = True)]
filtered_df.columns
#remove duplicated columns
filtered_df_2, nyc_percentage_of_duplicated, nyc_raw_calls_number, nyc_duplicated_calls = remove_duplicated_calls(filtered_df, "INCIDENT_DATE", "INCIDENT_DATE", 
                                        ['OBJECTID', 'CAD_EVNT_ID', 
                                         'CREATE_DATE', 
                                         'INCIDENT_TIME','ADD_TS', 'DISP_TS',
                                         'ARRIVD_TS', 'CLOSNG_TS'])

print("Percentage of duplicated calls: ", nyc_percentage_of_duplicated, "\n", 
      "Total Raw Call Counts: ", nyc_raw_calls_number, 
      "\n", "Total Duplicated Call Counts: ", nyc_duplicated_calls)

filtered_df_2["INCIDENT_DATE"] = filtered_df_2["INCIDENT_DATE"].dt.strftime('%Y-%m')
filtered_df_2 = filtered_df_2[["INCIDENT_DATE", "TYP_DESC"]]
grouped_df = filtered_df_2.groupby("INCIDENT_DATE").size().reset_index()
grouped_df.columns = ["year_month", "New York City_MVT_911"]
grouped_df.to_csv(r"C:\Users\tosea\Liu_Dissertation_GT_Code\New York City_911_by_month.csv")
visual_data(grouped_df, "New York City")

