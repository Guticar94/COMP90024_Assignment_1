# Import working libraries
from mpi4py import MPI
import pandas as pd
import ijson
import argparse
import time
import logging

#Import helpers and variables
from utils.variables import (
    json_twitter, json_geo, gcca_codes, ccities
)
# from utils.classes import ResultAggregator, reading_json
from utils.helpers import (
    send_buckets_and_gather_results,
    update_signal_for_workers,
    process_tweets,
    ResultAggregator,
    reading_json,
    process_data
)

# MPI Parameters
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
node_name = MPI.Get_processor_name()
total_number_of_available_nodes = comm.Get_size()


# Head node function
def mpi_rank_0(chunk_size, df_geo,logger):
    # Create one bucket of tweets per available node
    collection_of_buckets = []
    
    # Counter of chunck sizes
    ch_size = chunk_size-1
    # Define dict to append values
    tweets = {'auth_id':[],'place_name':[]}
    
    # Dataframe agreggator
    result_aggregator = ResultAggregator()

    #Counter for printing --- Can be deleted afterwards
    pr_cnt = 0
    tweet_cnt = 0
    # Reading the twitter json with list Comprehension
    with open(json_twitter, "r") as f:
        # Iterate the json file one by one
        for tweet_counter, tweet in enumerate(ijson.items(f, "item")):
            # Fill temporal dict with twitter values
            tweets['auth_id'].append([[tweet['data']['author_id']][0]][0])
            tweets['place_name'].append([tweet['includes']['places'][0]['full_name']][0])
            
            # If bucket is completed, then append to collection of buckets to be sent
            if tweet_counter == ch_size:
                # Individual Chunck of chunk_size size to df format
                bucket_of_individual_tweets = pd.DataFrame(tweets)
                # Append chunk to bucket
                collection_of_buckets.append(bucket_of_individual_tweets)
                # Empty dict
                tweets = {'auth_id':[],'place_name':[]}
                # Update Chunck counter to fill next chunk
                ch_size += chunk_size
                tweet_cnt = tweet_cnt + chunk_size
                logger.info(f"IN PROGRESS: Total Tweets analysed: {tweet_cnt}")
                # If collection of buckets reach its maximum capacity, then start scattering
                if len(collection_of_buckets) == total_number_of_available_nodes:
                    
                    # Temporal printing to see results
                    pr_cnt += 1 #Update print counter
                    logger.info(f'Scattering {int((tweet_counter+1)/pr_cnt)} tweets in collection number {pr_cnt} at node {rank} with name {node_name}')

                    # Scatter the buckets, get processed data, and update workers signal
                    send_buckets_and_gather_results(collection_of_buckets, comm, df_geo, result_aggregator)                    
                    update_signal_for_workers(True, comm)
                    collection_of_buckets = [] # Reset collection of buckets

    # Processed residual incomplete buckets
    # Individual Chunck of chunk_size size to df format
    bucket_of_individual_tweets = pd.DataFrame(tweets)
    # Append residual chink to bucket
    collection_of_buckets.append(bucket_of_individual_tweets)

    # Temporary variable for printing
    val = sum([[len(val) for val in collection_of_buckets]][0])

    # Fill the bucket for scattering to all nodes 
    # (We could explore the option of point to point connection at this point)
    collection_of_buckets = collection_of_buckets + [None] *\
            (total_number_of_available_nodes - len(collection_of_buckets))
    
    # Temporal printing to see results
    logger.info(f'Scattering last {val} tweets from collection number {pr_cnt+1} at node {rank}')

    # Scatter the bucket and get processed data
    send_buckets_and_gather_results(collection_of_buckets, comm, df_geo, result_aggregator)
    logger.info(f"FINISHED: A total of {tweet_counter+1} tweets were analyzed")
    # Here we tell the workers to STOP
    update_signal_for_workers(False, comm)
    return result_aggregator

def mpi_rank_workers(df_geo):
    data = None
    are_there_tweets_being_processed = True
    while are_there_tweets_being_processed:
        data = comm.scatter(data, root=0)
        if isinstance(data, pd.DataFrame):
            data_procesed = process_tweets(data, df_geo)
        else:
            data_procesed = [None,None]
        comm.gather(data_procesed, root=0)
        are_there_tweets_being_processed = comm.bcast(
            are_there_tweets_being_processed, root=0
        )


def main():
    if args.get('chunk'):
        chunk_size = args['chunk']
    else:
        chunk_size = 100

    read = reading_json()
    df_geo = read.read_geo(json_geo)  # Sal file -- Dict

    if rank == 0:
        # Getting logs
        logging.basicConfig(level= logging.DEBUG, filename='./output/main-2node-8core.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger.info(f"Node List: {args.get('nodelist')}")
        st = time.time()
        # Run node 0 function
        result_aggregator = mpi_rank_0(chunk_size, df_geo,logger)

        # Temporal printing to see results
        result_aggregator.df1['Number of Tweets Made'].sum()

        # Here we answer the assignment questions
        # (We may do this through point to point communication) 
        # Call class
        proc = process_data(result_aggregator.df1)

        #Answer question 1
        df1 = proc.point_1(gcca_codes)

        #Answer question 2
        df2 = proc.point_2()

        #Answer question 3
        df3 = proc.point_3(ccities)


        # Docs to csv
        output_folder_path = args['path']
        df1.to_csv(output_folder_path + "df1-2node-8core.csv")
        df2.to_csv(output_folder_path + "df2-2node-8core.csv")
        df3.to_csv(output_folder_path + "df3-2node-8core.csv")

        et = time.time()
        # get the execution time
        elapsed_time = et - st
        logger.info(f"Execution time: {elapsed_time} seconds")

    else:
        mpi_rank_workers(df_geo)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='The following program will answer the three questions of assignment 1 for subject COMP90024')
    parser.add_argument('-p','--path', help='Description for foo argument', required=True)
    parser.add_argument('-c','--chunk', help='Description for bar argument', required=False, type=int)
    parser.add_argument('-n','--nodelist', help='Description for foo argument', required=False)
    args = vars(parser.parse_args())
    main()
