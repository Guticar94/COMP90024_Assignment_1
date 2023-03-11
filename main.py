import argparse
from utils.variables import (
    json_twitter,
)
from utils.classes import ResultAggregator, reading_json
from utils.helpers import (
    create_empty_data_frame_to_accumulate_chunks_of_tweets,
    send_buckets_to_workers,
    update_signal_for_workers,
    process_residual_tweets,
    process_tweets,
)
from mpi4py import MPI
import pandas as pd
import ijson

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
total_number_of_available_nodes = comm.Get_size()


def mpi_rank_0(chunk_size, read):
    # Create one bucket of tweets per available node
    collection_of_buckets = []
    tweet_counter = 0
    bucket_of_individual_tweets = (
        create_empty_data_frame_to_accumulate_chunks_of_tweets()
    )
    result_aggregator = ResultAggregator()

    # Reading the twitter json with list Comprehension
    with open(json_twitter, "rb") as f:
        for tweet in ijson.items(f, "item"):
            df = pd.DataFrame(
                [
                    {
                        "auth_id": [tweet["data"]["author_id"]][0],
                        "place_name": [tweet["includes"]["places"][0]["full_name"]][0],
                    }
                ]
            )
            bucket_of_individual_tweets = pd.concat([bucket_of_individual_tweets, df])
            tweet_counter = tweet_counter + 1
            # If bucket is completed, then append to collection of buckets
            if tweet_counter == chunk_size:
                collection_of_buckets.append(bucket_of_individual_tweets)
                # If collection of buckets reach its maximum capacity, then start scattering
                if len(collection_of_buckets) == total_number_of_available_nodes:
                    send_buckets_to_workers(
                        collection_of_buckets, comm, rank, read, result_aggregator
                    )
                    update_signal_for_workers(True, comm)
                    collection_of_buckets = []

                # Reset counter and clear the bucket from tweets
                tweet_counter = 0
                bucket_of_individual_tweets = (
                    create_empty_data_frame_to_accumulate_chunks_of_tweets()
                )

    # Processed residual incomplete buckets
    process_residual_tweets(
        total_number_of_available_nodes,
        collection_of_buckets,
        tweet_counter,
        bucket_of_individual_tweets,
        comm,
        read,
        rank,
        result_aggregator,
    )

    return result_aggregator


def mpi_rank_workers(read):
    data = None
    are_there_tweets_being_processed = True
    while are_there_tweets_being_processed:
        data = comm.scatter(data, root=0)
        if isinstance(data, pd.DataFrame):
            data_procesed = process_tweets(read, data, rank)
        else:
            data_procesed = [None,None]
        comm.gather(data_procesed, root=0)
        are_there_tweets_being_processed = comm.bcast(
            are_there_tweets_being_processed, root=0
        )


def main(args):
    if args.get('chunk'):
        chunk_size = args['chunk']
    else:
        chunk_size = 100
    read = reading_json()

    if rank == 0:
        result_aggregator = mpi_rank_0(chunk_size, read)
        
        # Docs to csv
        output_folder_path = args['path']
        result_aggregator.df1.to_csv(output_folder_path + "df1.csv")
        # df2.to_csv(output_folder_path + "df2.csv")
        # df3.to_csv(output_folder_path + "df3.csv")

    else:
        mpi_rank_workers(read)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='The following program will answer the three questions of assignment 1 for subject COMP90024')
    parser.add_argument('-p','--path', help='Description for foo argument', required=True)
    parser.add_argument('-c','--chunk', help='Description for bar argument', required=False)
    args = vars(parser.parse_args())
    main(args)
