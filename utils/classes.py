# Import working libraries
import pandas as pd
import re


class reading_json:
    # Read SAL (Statistical Area Level) dataframe
    def read_geo(self, path):
        # Reading geographical data
        return pd.read_json(path, orient="index")["gcc"]


class quality_data:
    def __init__(self, df, geo):
        # Read data
        self.wk_ds = df
        self.geo = geo

    def standarize(self, states_dict):
        # Standarize geo notation and change of structure to string comparison
        self.geo.index = [
            val.lower().replace(" ", "").replace(".", "") for val in self.geo.index
        ]
        self.geo = self.geo.to_dict()

        # Standarize states notation
        for key, val in states_dict.items():
            self.wk_ds["place_name"] = self.wk_ds["place_name"].apply(
                lambda x: re.sub(key, val, x)
            )
        self.wk_ds["place_name"] = self.wk_ds["place_name"].apply(
            lambda x: x.replace(",", "").replace(" ", "").lower()
        )

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


class process_data:
    def __init__(self, df):
        # Read data
        self.wk_ds = df

    def point_1(self, gcca_codes):
        # Non Australian territories may be grouped in other categorie
        self.pnt_1 = self.wk_ds["place_name"].value_counts().reset_index()
        self.pnt_1["index"] = (
            self.pnt_1["index"]
            + " ("
            + self.pnt_1.replace({"index": gcca_codes}).iloc[:, 0]
            + ")"
        )
        self.pnt_1.columns = ["Greater Capital City", "Number of Tweets Made"]
        return self.pnt_1

    def point_2(self):
        # Top 10 tweeters with their number of tweets
        self.pnt_2 = self.wk_ds["auth_id"].value_counts().reset_index().reset_index()
        self.pnt_2.iloc[:, 0] = (self.pnt_2.iloc[:, 0] + 1).apply(
            lambda x: "#" + str(x)
        )
        self.pnt_2.columns = ["Rank", "Author Id", "Number of Tweets Made"]
        # pnt_2.iloc[:10,:].to_csv('pnt_2.csv')
        return self.pnt_2

    def point_3(self, ccities):
        #### Only Tweets from capital cities
        # Group de data for the task
        pnt_3 = (
            self.wk_ds.reset_index()[self.wk_ds["place_name"].isin(ccities)][
                ["auth_id", "place_name", "index"]
            ]
            .groupby(["auth_id", "place_name"])
            .count()
            .unstack()
            .droplevel(0, axis=1)
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


class ResultAggregator:
    def __init__(self):
        self.df1 = pd.DataFrame(
            columns=["Greater Capital City", "Number of Tweets Made"]
        )

    def update_results_question_1(self, partial_results_for_q1):
        for partial_result in partial_results_for_q1:
            if isinstance(partial_result, pd.DataFrame):
                self.df1 = (
                    pd.concat([self.df1, partial_result])
                    .groupby(["Greater Capital City"])
                    .sum()
                    .reset_index()
                )

    def get_result_q1(self):
        return self.df1
