import pandas as pd
from utils.classes import quality_data, process_data
from utils.variables import (
    json_geo,
    gcca_codes,
    ccities,
    states_dict,
    capitals_dict,
    replacements,
)


def process_tweets(read, df_tw, rank):
    df_geo = read.read_geo(json_geo)  # Sal file -- Dict

    # Estandarize dataset
    qua = quality_data(df_tw, df_geo)
    est = qua.standarize(states_dict)
    est = qua.replacement(capitals_dict, replacements)

    # Process data
    proc = process_data(est)
    # Answer question 1
    df1 = proc.point_1(gcca_codes)
    assert df_tw.shape[0] == df1.iloc[:, 1].sum()

    # Answer question 2
    df2 = None
    # df2 = proc.point_2()
    # assert df_tw.shape[0] == df2.iloc[:, 2].sum()

    # Answer question 3
    # df3 = proc.point_3(ccities)
    return (df1, df2)


def create_empty_data_frame_to_accumulate_chunks_of_tweets():
    return pd.DataFrame(columns=["auth_id", "place_name"])


def process_gathered_partial_results(partial_results, result_aggregator):
    partial_results_for_q1, partial_results_for_q2 = zip(*partial_results)

    # Aggregating partials results for q1
    result_aggregator.update_results_question_1(partial_results_for_q1)


def process_residual_tweets(
    total_number_of_available_nodes,
    collection_of_buckets,
    tweet_counter,
    bucket_of_individual_tweets,
    comm,
    read,
    rank,
    result_aggregator,
):
    # If chunk list is not completed, then create None Objects
    if (
        len(collection_of_buckets) < total_number_of_available_nodes
        and len(collection_of_buckets) != 0
    ):
        collection_of_buckets.append(bucket_of_individual_tweets)
        collection_of_buckets = collection_of_buckets + [None] * (
            total_number_of_available_nodes - len(collection_of_buckets)
        )
        send_buckets_to_workers(
            collection_of_buckets, comm, rank, read, result_aggregator
        )
        update_signal_for_workers(False, comm)
    elif tweet_counter != 0:
        data_processed = process_tweets(read, bucket_of_individual_tweets, rank)
        process_gathered_partial_results([data_processed], result_aggregator)
        # Here we process the residual tweets (on rank 0 process) and tell the other workers to STOP
        collection_of_buckets = [None] * total_number_of_available_nodes
        comm.scatter(collection_of_buckets, root=0)
        update_signal_for_workers(False, comm)


def send_buckets_to_workers(
    bucket_of_tweets_chunks, comm, rank, read, result_aggregator
):
    data = comm.scatter(bucket_of_tweets_chunks, root=0)

    data_procesed = process_tweets(read, data, rank)
    partial_results = comm.gather(data_procesed, root=0)

    process_gathered_partial_results(partial_results, result_aggregator)


def update_signal_for_workers(are_there_tweets_being_processed, comm):
    comm.bcast(are_there_tweets_being_processed, root=0)
