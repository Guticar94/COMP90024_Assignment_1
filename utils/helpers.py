# Import working libraries
import pandas as pd
import re

#Import variables
from utils.variables import (
    states_dict,
    capitals_dict,
    replacements,
    gcca_codes
)
#____________________________________________________________________________________________________
# Read SAL (Statistical Area Level) dataframe
class reading_json:
    def read_geo(self, path):
        # Reading geographical data
        return pd.read_json(path, orient="index")["gcc"]
#____________________________________________________________________________________________________
# Apply standarization rules and merge data by place name
class quality_data:
    def __init__(self, df, geo):
        # Read data
        self.wk_ds = df
        self.geo = geo
    # Standarize geo notation and change of structure to string comparison
    def standarize(self, states_dict):
        # Set the names to lower case and drop special characters except Parenthesis to geo data
        self.geo.index = [
            val.lower().replace(" ", "").replace(".", "") for val in self.geo.index
        ]
        self.geo = self.geo.to_dict()

        # Standarize tweets data states notation as sal notation
        for key, val in states_dict.items():
            self.wk_ds["place_name"] = self.wk_ds["place_name"].apply(
                lambda x: re.sub(key, val, x)
            )

        # Set the names to lower case and drop special characters except Parenthesis to tweets data
        self.wk_ds["place_name"] = self.wk_ds["place_name"].apply(
            lambda x: x.replace(",", "").replace(" ", "").lower()
        )

    # Make data replacements to get standar structure of GCCA
    def replacement(self, ccap_dict, replacements):
        # Replace capital cities to gcc (Custom Regexp dictionary)
        self.wk_ds[["place_name"]] = self.wk_ds[["place_name"]].replace(
            {"place_name": ccap_dict}, regex=True
        )
        # Replace territories to Sal gcc (Regexp merge)
        self.wk_ds["place_name"] = self.wk_ds[["place_name"]].replace(
            {"place_name": self.geo}
        )
        # # Replace remaining territories to gcc (Custom Regexp dictionary)
        self.wk_ds["place_name"] = self.wk_ds[["place_name"]].replace(
            {"place_name": replacements}, regex=True
        )
        return self.wk_ds
#____________________________________________________________________________________________________
# Apply standarization and replace processing changes to incomming dataframes
def process_tweets(df_tw, df_geo):    
    # Estandarize dataset
    qua = quality_data(df_tw, df_geo)
    est = qua.standarize(states_dict)
    est = qua.replacement(capitals_dict, replacements)

    # Agreggated Data Frame and return estandarized agreggation
    df = est.reset_index().groupby(['place_name', 'auth_id']).count().reset_index()
    df.columns = ["Greater Capital City", "Author Id", "Number of Tweets Made"]
    assert est.shape[0] == df.iloc[:, 2].sum()
    return df
#____________________________________________________________________________________________________
# Responses to assignment questions
class process_data:
    def __init__(self, df):
        # Read data
        self.wk_ds = df

    def point_1(self, gcca_codes):
        # In the end non Australian territories may be grouped in other categorie__________________________________

        self.pnt_1 = self.wk_ds.groupby("Greater Capital City")['Number of Tweets Made'].sum().\
                reset_index().sort_values('Number of Tweets Made', ascending = False)
        self.pnt_1.iloc[:,0] = (
            self.pnt_1.iloc[:,0] + " (" +
            self.pnt_1.replace({'Greater Capital City': gcca_codes}).iloc[:, 0] +
              ")")
        return self.pnt_1

    def point_2(self):
        # Top 10 tweeters with their number of tweets
        self.pnt_2 = self.wk_ds.groupby("Author Id")['Number of Tweets Made'].sum().reset_index().\
                    sort_values('Number of Tweets Made', ascending = False)
        self.pnt_2.iloc[:, 0] = self.pnt_2.iloc[:, 0].apply(lambda x: "#" + str(x))
        return self.pnt_2

    def point_3(self, ccities):
        #### Only Tweets from capital cities
        # Group de data for the task
        pnt_3 = (
            self.wk_ds.reset_index()[self.wk_ds["Greater Capital City"].
                                     isin(ccities)][["Author Id", "Greater Capital City", "Number of Tweets Made"]]
                                     .groupby(["Author Id", "Greater Capital City"])
                                     ['Number of Tweets Made'].sum()
                                     .unstack()
                                    #  .droplevel(0, axis=1)
        )

        # Create function to aggregate all in text
        def series_agg(serie):
            text = str(len(serie)) + "(#" + str(serie.sum()) + " tweets - "
            for val in range(0, len(serie)):
                text = text + str(serie.values[val]) + " from " + serie.index[val]
                if val < len(serie) - 1:
                    text += ", "
                else:
                    text += ")"
            return text

        # Ordering by more locations and number of tweets
        pnt_3_ = (
            pnt_3.agg(func=["count", "sum"], axis=1)
            .reset_index()
            .sort_values(["count", "sum"], ascending=[False, False])
            .reset_index(drop=True)
            .reset_index(names="Rank")
            .drop(["count", "sum"], axis=1)
        )

        # Create ranking variable
        pnt_3_["Rank"] = pnt_3_["Rank"] + 1

        # Change columns names
        pnt_3_.columns = ["Rank", "Author Id"]

        # Agregate twits and locations in text cells
        agreg = []
        for user in pnt_3_["Author Id"]:
            agreg.append(series_agg(pnt_3.loc[user, :].dropna().astype(int)))
        pnt_3_["Number of Unique City Locations and #Tweets"] = agreg

        return pnt_3_
#____________________________________________________________________________________________________

class ResultAggregator:
    def __init__(self):
        self.df1 = pd.DataFrame(
            columns=["Greater Capital City", "Author Id", "Number of Tweets Made"]
        )

    def update_aggregation(self, partial_results, codes= gcca_codes):
        for partial_result in partial_results:
            if isinstance(partial_result, pd.DataFrame):
                self.df1 = (
                    pd.concat([self.df1, partial_result])
                    .groupby(["Greater Capital City", "Author Id"])["Number of Tweets Made"]
                    .sum()
                    .reset_index())
                #Filter only australian territories to improve performance
                # self.df1 = self.df1[self.df1['Greater Capital City'].isin(codes.keys())]

#____________________________________________________________________________________________________
def process_tweets_and_gather_results(bucket_of_individual_tweets,comm, df_geo, result_aggregator):
    # This function scatter the processes to the worker nodes and gather its results
    # data = comm.scatter(collection_of_buckets, root=0) # Sending bucket-chunks to each node in the network (Here we can test only send to workers)
    partial_results = comm.gather(process_tweets(bucket_of_individual_tweets, df_geo), root=0) # Gathering results of processing nodes
    result_aggregator.update_aggregation(partial_results) # Aggregating partials results

    return result_aggregator.df1["Number of Tweets Made"].sum()