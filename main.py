from utils.variables import json_twitter, json_geo, gcca_codes, ccities
from utils.variables import states_dict, capitals_dict, replacements
from utils.classes import quality_data, reading_json, process_data
import pandas as pd

def main():
    chunk_size = 100
    chunk_counter = 0

    # Read Json Files
    read = reading_json()
    df_tw = read.read_tweets(json_twitter, chunk_size)          #Twitter file -- DataFrame
    df_geo = read.read_geo(json_geo)                            #Sal file -- Dict

    # Estandarize dataset 
    qua = quality_data(df_tw, df_geo)
    est = qua.standarize(states_dict)
    est = qua.replacement(capitals_dict, replacements)


    
    # small_df_of_tweets_ready_to_be_processed = pd.DataFrame(columns = ['auth_id', 'place_name'])
        #     small_df_of_tweets_ready_to_be_processed = pd.concat([small_df_of_tweets_ready_to_be_processed, df_tw])
        #     chunk_counter = chunk_counter + 1
        #     if chunk_counter == chunk_size:
        #         #Process the data to get the
        #         print(small_df_of_tweets_ready_to_be_processed)
        #         proc = process_data(small_df_of_tweets_ready_to_be_processed)
        #         chunk_counter = 0
        #         small_df_of_tweets_ready_to_be_processed = pd.DataFrame(columns = ['auth_id', 'place_name'])
        
        # if chunk_counter != 0:
        #     print(f"SEND LAST CHUNK of size: {chunk_counter}")

    # Process data
    proc = process_data(est)
    #Answer question 1
    df1 = proc.point_1(gcca_codes)
    assert df_tw.shape[0] == df1.iloc[:,1].sum()

    #Answer question 2
    df2 = proc.point_2()
    assert df_tw.shape[0] == df2.iloc[:,2].sum()

    #Answer question 3
    df3 = proc.point_3(ccities)

    #Docs to csv
    output_folder_path = './output/'
    df1.to_csv(output_folder_path+'df1.csv')
    df2.to_csv(output_folder_path+'df2.csv')
    df3.to_csv(output_folder_path+'df3.csv')

if __name__ == '__main__':
    main()