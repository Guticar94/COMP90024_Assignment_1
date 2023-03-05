from utils.classes import reading_json, process_data
from utils.variables import json_tweeter, json_geo, json_enrich
import ijson
import pandas as pd

def main():
    #Read Json Files
    read = reading_json()
    # chunk_size = 100 // Maybe it will be nice to process in Streamed Batches
    summary_df1 = pd.DataFrame(columns = ['Greater Capital City', 'Number of Tweets Made'])
    with open("./input/twitter-data-small.json", "rb") as f:

        for tweet in ijson.items(f, "item"):
            # read = json.load(chunk)
            df_tw = read.read_tweets(tweet)
            # df_geo = read.read_geo(json_geo, json_enrich)

            #Process the data to get the
            proc = process_data(df_tw)

            #Answer question 1
            df1 = proc.point_1()
            assert df_tw.shape[0] == df1.iloc[:,1].sum()
            summary_df1 = pd.concat([summary_df1, df1]).groupby(['Greater Capital City']).sum().reset_index()

            #Answer question 2
            # df2 = proc.point_2()
            # assert df_tw.shape[0] == df2.iloc[:,2].sum()
            #print(df2.iloc[:10,:])

            # #Answer question 3
            # df3 = proc.point_3()

    #Docs to csv
    output_folder_path = './output/'
    summary_df1.to_csv(output_folder_path+'df1.csv')
    # df2.to_csv(output_folder_path+'df2.csv')
    # df3.to_csv(output_folder_path+'df3.csv')

if __name__ == '__main__':
    main()