from utils.classes import reading_json, process_data
import ijson
import pandas as pd

def main():
    #Read Json Files
    read = reading_json()
    chunk_size = 800
    summary_df1 = pd.DataFrame(columns = ['Greater Capital City', 'Number of Tweets Made'])
    chunk_counter = 0
    small_df_of_tweets_ready_to_be_processed = pd.DataFrame(columns = ['auth_id', 'place_name'])
    with open("./input/twitter-data-small.json", "rb") as f:
        for tweet in ijson.items(f, "item"):
            df_tw = read.read_tweets(tweet)
            small_df_of_tweets_ready_to_be_processed = pd.concat([small_df_of_tweets_ready_to_be_processed, df_tw])
            chunk_counter = chunk_counter + 1
            if chunk_counter == chunk_size:
                #Process the data to get the
                print(small_df_of_tweets_ready_to_be_processed)
                proc = process_data(small_df_of_tweets_ready_to_be_processed)
                chunk_counter = 0
                small_df_of_tweets_ready_to_be_processed = pd.DataFrame(columns = ['auth_id', 'place_name'])
        
        if chunk_counter != 0:
            print(f"SEND LAST CHUNK of size: {chunk_counter}")

            #Answer question 1
            # df1 = proc.point_1()
            # assert df_tw.shape[0] == df1.iloc[:,1].sum()
            # summary_df1 = pd.concat([summary_df1, df1]).groupby(['Greater Capital City']).sum().reset_index()

    #Docs to csv
    output_folder_path = './output/'
    summary_df1.to_csv(output_folder_path+'df1.csv')

if __name__ == '__main__':
    main()