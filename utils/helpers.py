# Import working libraries
import pandas as pd
import re

# Import variables
from utils.variables import gcca_codes


# ____________________________________________________________________________________________________
# Read SAL (Statistical Area Level) dataframe
class reading_json:
    def read_geo(self, path):
        # Reading geographical data
        return pd.read_json(path, orient="index")["gcc"]


# ____________________________________________________________________________________________________
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
            self.wk_ds["Greater Capital City"] = self.wk_ds[
                "Greater Capital City"
            ].apply(lambda x: re.sub(key, val, x))

        # Set the names to lower case and drop special characters except Parenthesis to tweets data
        self.wk_ds["Greater Capital City"] = self.wk_ds["Greater Capital City"].apply(
            lambda x: x.replace(",", "").replace(" ", "").lower()
        )

    # Make data replacements to get standar structure of GCCA
    def replacement(self, ccap_dict, replacements):
        # Replace capital cities to gcc (Custom Regexp dictionary)
        self.wk_ds[["Greater Capital City"]] = self.wk_ds[
            ["Greater Capital City"]
        ].replace({"Greater Capital City": ccap_dict}, regex=True)
        # Replace territories to Sal gcc (Regexp merge)
        self.wk_ds["Greater Capital City"] = self.wk_ds[
            ["Greater Capital City"]
        ].replace({"Greater Capital City": self.geo})
        # # Replace remaining territories to gcc (Custom Regexp dictionary)
        self.wk_ds["Greater Capital City"] = self.wk_ds[
            ["Greater Capital City"]
        ].replace({"Greater Capital City": replacements}, regex=True)
        return self.wk_ds


# ____________________________________________________________________________________________________
# Apply standarization and replace processing changes to incomming dataframes
def process_tweets(df_tw, df_geo):
    # Agreggated Data Frame and return estandarized agreggation
    df = df_tw.reset_index().groupby(["place_name", "auth_id"]).count().reset_index()
    df.columns = ["Greater Capital City", "Author Id", "Number of Tweets Made"]
    assert df_tw.shape[0] == df.iloc[:, 2].sum()
    return df


# ____________________________________________________________________________________________________
# Responses to assignment questions
class process_data:
    def __init__(self, df):
        # Read data
        self.wk_ds = df

    def point_1(self, gcca_codes):
        # In the end non Australian territories may be grouped in other categorie__________________________________

        self.pnt_1 = (
            self.wk_ds.groupby("Greater Capital City")["Number of Tweets Made"]
            .sum()
            .reset_index()
            .sort_values("Number of Tweets Made", ascending=False)
        )
        self.pnt_1.iloc[:, 0] = (
            self.pnt_1.iloc[:, 0]
            + " ("
            + self.pnt_1.replace({"Greater Capital City": gcca_codes}).iloc[:, 0]
            + ")"
        )
        return self.pnt_1

    def point_2(self):
        # Top 10 tweeters with their number of tweets
        self.pnt_2 = (
            self.wk_ds.groupby("Author Id")["Number of Tweets Made"]
            .sum()
            .reset_index()
            .sort_values("Number of Tweets Made", ascending=False)
        )
        self.pnt_2.iloc[:, 0] = self.pnt_2.iloc[:, 0].apply(lambda x: "#" + str(x))
        return self.pnt_2

    def point_3(self, ccities):
        #### Only Tweets from capital cities
        # Group de data for the task
        pnt_3 = (
            self.wk_ds.reset_index()[self.wk_ds["Greater Capital City"].isin(ccities)][
                ["Author Id", "Greater Capital City", "Number of Tweets Made"]
            ]
            .groupby(["Author Id", "Greater Capital City"])["Number of Tweets Made"]
            .sum()
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


# ____________________________________________________________________________________________________


class ResultAggregator:
    def __init__(self):
        self.df1 = pd.DataFrame(
            columns=["Greater Capital City", "Author Id", "Number of Tweets Made"]
        )

    def update_aggregation(self, partial_results, codes=gcca_codes):
        for partial_result in partial_results:
            if isinstance(partial_result, pd.DataFrame):
                self.df1 = (
                    pd.concat([self.df1, partial_result])
                    .groupby(["Greater Capital City", "Author Id"])[
                        "Number of Tweets Made"
                    ]
                    .sum()
                    .reset_index()
                )
        return self.df1["Number of Tweets Made"].sum()
        # Filter only australian territories to improve performance
        # self.df1 = self.df1[self.df1['Greater Capital City'].isin(codes.keys())]


# ____________________________________________________________________________________________________
def gather_results(comm, result_aggregator):
    # This function scatter the processes to the worker nodes and gather its results
    all_aggregators = comm.gather(
        result_aggregator, root=0
    )  # Gathering results of processing nodes

    return all_aggregators


def aggregate_results(all_aggregators):
    # Dataframe agreggator
    super_aggregator = ResultAggregator()
    # This function scatter the processes to the worker nodes and gather its results

    for aggregator in all_aggregators:
        if isinstance(aggregator.df1, pd.DataFrame):
            super_aggregator.df1 = (
                pd.concat([super_aggregator.df1, aggregator.df1])
                .groupby(["Greater Capital City", "Author Id"])["Number of Tweets Made"]
                .sum()
                .reset_index()
            )

    return super_aggregator
