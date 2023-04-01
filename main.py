# Import working libraries
from mpi4py import MPI
import pandas as pd
import ijson
import argparse
import time
import logging

# Import helpers and variables
from utils.variables import (
    json_twitter,
    json_geo,
    gcca_codes,
    ccities,
    states_dict,
    capitals_dict,
    replacements,
)

from utils.helpers import (
    aggregate_results,
    gather_results,
    process_tweets,
    ResultAggregator,
    quality_data,
    reading_json,
    process_data,
)

# MPI Parameters
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
node_name = MPI.Get_processor_name()
total_number_of_available_nodes = comm.Get_size()


# Head node function
def mpi_rank_0(chunk_size, df_geo, logger, twitter_input_file):
    # Create one bucket of tweets per available node
    comm.bcast(chunk_size, root=0)
    # Counter of chunk sizes
    ch_size = chunk_size - 1
    # Define dict to append values
    tweets = {"auth_id": [], "place_name": []}

    # Dataframe agreggator
    result_aggregator = ResultAggregator()

    incrementor = 0
    # Reading the twitter json with list Comprehension
    with open(twitter_input_file, "r") as f:
        # Iterate the json file one by one
        for tweet_counter, tweet in enumerate(ijson.items(f, "item")):
            # If bucket is completed, then append to collection of tweets
            if (
                tweet_counter >= chunk_size * (rank + incrementor)
                and tweet_counter < chunk_size * (rank + incrementor) + chunk_size
            ):
                # Fill temporal dict with twitter values
                tweets["auth_id"].append([[tweet["data"]["author_id"]][0]][0])
                tweets["place_name"].append(
                    [tweet["includes"]["places"][0]["full_name"]][0]
                )

                if tweet_counter == chunk_size * (rank + incrementor) + chunk_size - 1:
                    # Individual Chunck of chunk_size size to df format
                    bucket_of_individual_tweets = pd.DataFrame(tweets)

                    # Update Chunck counter to fill next chunk
                    ch_size += chunk_size

                    # Processed data, and update local aggregator
                    tweets_aggregated = result_aggregator.update_aggregation(
                        [process_tweets(bucket_of_individual_tweets, df_geo)]
                    )  # Aggregating partials results
                    logger.info(
                        f"IN PROGRESS: Total Tweets analysed: {tweet_counter + 1}"
                    )
                    incrementor = incrementor + total_number_of_available_nodes
                    if total_number_of_available_nodes == 1:
                        # Empty dict
                        tweets = {"auth_id": [], "place_name": []}
            else:
                # Empty dict
                tweets = {"auth_id": [], "place_name": []}

    # Processed residual incomplete buckets
    # Individual Chunck of chunk_size size to df format
    bucket_of_individual_tweets = pd.DataFrame(tweets)

    # Temporal printing to see results
    # Processed data, and update local aggregator
    tweets_aggregated = result_aggregator.update_aggregation(
        [process_tweets(bucket_of_individual_tweets, df_geo)]
    )  # Aggregating partials results

    logger.info(
        f"IN PROGRESS: Total Tweets analysed: {tweets_aggregated} in rank {rank}"
    )

    # get processed data, and aggregate results
    all_aggregators = gather_results(comm, result_aggregator)
    super_aggregator = aggregate_results(all_aggregators)
    logger.info(
        f'FINISHED: A total of {super_aggregator.df1["Number of Tweets Made"].sum()} tweets were analyzed'
    )
    return super_aggregator


def mpi_rank_workers(df_geo, twitter_input_file):
    chunk_size = 0
    # Define dict to append values
    tweets = {"auth_id": [], "place_name": []}
    # Dataframe agreggator
    result_aggregator = ResultAggregator()
    incrementor = 0
    chunk_size = comm.bcast(chunk_size, root=0)
    with open(twitter_input_file, "r") as f:
        # Iterate the json file one by one
        for tweet_counter, tweet in enumerate(ijson.items(f, "item")):
            if (
                tweet_counter >= chunk_size * (rank + incrementor)
                and tweet_counter < chunk_size * (rank + incrementor) + chunk_size
            ):
                # Fill temporal dict with twitter values
                tweets["auth_id"].append([[tweet["data"]["author_id"]][0]][0])
                tweets["place_name"].append(
                    [tweet["includes"]["places"][0]["full_name"]][0]
                )

                if tweet_counter == chunk_size * (rank + incrementor) + chunk_size - 1:
                    # Individual Chunck of chunk_size size to df format
                    bucket_of_individual_tweets = pd.DataFrame(tweets)
                    data_procesed = process_tweets(bucket_of_individual_tweets, df_geo)
                    tweets_aggregated = result_aggregator.update_aggregation(
                        [data_procesed]
                    )  # Aggregating partials results
                    incrementor = incrementor + total_number_of_available_nodes
            else:
                # Empty dict
                tweets = {"auth_id": [], "place_name": []}

        bucket_of_individual_tweets = pd.DataFrame(tweets)
        data_procesed = process_tweets(bucket_of_individual_tweets, df_geo)
        result_aggregator.update_aggregation(
            [data_procesed]
        )  # Aggregating partials results

        comm.gather(result_aggregator, root=0)


def main():
    if args.get("input"):
        twitter_input_file = args["input"]
    else:
        twitter_input_file = json_twitter

    if args.get("chunk"):
        chunk_size = args["chunk"]
    else:
        chunk_size = 100

    if args.get("tag"):
        experiment_tag_id = args["tag"]
    else:
        experiment_tag_id = "default"

    read = reading_json()
    df_geo = read.read_geo(json_geo)  # Sal file -- Dict

    if rank == 0:
        # Getting logs
        logging.basicConfig(
            level=logging.DEBUG,
            filename=f"./output/main-{experiment_tag_id}.log",
            filemode="w",
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger.info(f"Node List: {args.get('nodelist')}")
        st = time.time()
        # Run node 0 function
        result_aggregator = mpi_rank_0(chunk_size, df_geo, logger, twitter_input_file)

        # Temporal printing to see results
        result_aggregator.df1["Number of Tweets Made"].sum()

        # Here we answer the assignment questions
        # (We may do this through point to point communication)
        # Estandarize dataset
        qua = quality_data(result_aggregator.df1, df_geo)
        est = qua.standarize(states_dict)
        est = qua.replacement(capitals_dict, replacements)
        # Call class
        proc = process_data(est)

        # Answer question 1
        df1 = proc.point_1(gcca_codes)

        # Answer question 2
        df2 = proc.point_2()

        # Answer question 3
        df3 = proc.point_3(ccities)

        # Docs to csv
        output_folder_path = args["path"]
        df1.to_csv(output_folder_path + f"df1-{experiment_tag_id}.csv")
        df2.to_csv(output_folder_path + f"df2-{experiment_tag_id}.csv")
        df3.to_csv(output_folder_path + f"df3-{experiment_tag_id}.csv")

        et = time.time()
        # get the execution time
        elapsed_time = et - st
        logger.info(f"Execution time: {elapsed_time} seconds")

    else:
        mpi_rank_workers(df_geo, twitter_input_file)

    MPI.Finalize()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="The following program will answer the three questions of assignment 1 for subject COMP90024"
    )
    parser.add_argument(
        "-i", "--input", help="Path to twitter file dataset", required=False
    )
    parser.add_argument("-p", "--path", help="Path to output folder", required=True)
    parser.add_argument(
        "-c", "--chunk", help="Number of tweets per chunk", required=False, type=int
    )
    parser.add_argument("-t", "--tag", help="Experiment Identifier", required=False)
    parser.add_argument(
        "-n", "--nodelist", help="Node lists where the script will run", required=False
    )
    args = vars(parser.parse_args())
    main()
