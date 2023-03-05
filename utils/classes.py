#Import working libraries
import pandas as pd
import json
from .variables import ccities, replacements, gcca_codes

class reading_json:
    def read_tweets(self, data):
        # Reading the twitter json
        # with open("./input/twitter-data-small.json", "r") as file:
        #     data = json.load(file)

        self.df = pd.DataFrame({'auth_id':[data['data']['author_id']],\
                'place_name': [data['includes']['places'][0]['full_name']]})
    
        return self.df
    
    def read_geo(self, json_geo, json_enrich):
        #Reading geographical data
        self.df_1 = pd.read_json(json_geo, orient='index')
        self.df_1  = self.df_1.reset_index(names='states')
        self.df_1  = self.df_1.set_index('sal').sort_index()

        #Enriching geographical data
        self.df_2 = pd.read_csv('./input/georef.csv', sep=';').drop(['Geo Shape'], axis=1)
        self.df_2 = self.df_2.set_index('Official Code Suburb').sort_index()

        #Joinin geo data
        self.df_geo = self.df_2.join(self.df_1).drop(['Iso 3166-3 Area Code', 'Year', 'states', 'ste', 'Geo Point'], axis = 1).reset_index(names = 'SA3')
        return self.df_geo
    

class process_data:
    def __init__(self, df):
        self.wk_ds = df
        self.wk_ds['place_name'] = df[['place_name']].replace({'place_name':replacements}, regex=True)

    def point_1(self):
        #Here we haven't used yet the sal file. Plus the non Australian territories must be grouped in other categorie
        self.pnt_1 = self.wk_ds['place_name'].value_counts().reset_index()
        self.pnt_1['index'] = self.pnt_1['index']+" ("+self.pnt_1.replace({"index": gcca_codes}).iloc[:,0]+")"
        self.pnt_1.columns = ['Greater Capital City', 'Number of Tweets Made']
        return self.pnt_1
    
    def point_2(self):
        #Top 10 tweeters with their number of tweets
        self.pnt_2 = self.wk_ds['auth_id'].value_counts().reset_index().reset_index()
        self.pnt_2.iloc[:,0] = (self.pnt_2.iloc[:,0] + 1).apply(lambda x: '#'+str(x))
        self.pnt_2.columns = ['Rank', 'Author Id', 'Number of Tweets Made']
        #pnt_2.iloc[:10,:].to_csv('pnt_2.csv')
        return self.pnt_2
    
    def point_3(self):
        #### Only Tweets from capital cities
        # Group de data for the task
        pnt_3 = self.wk_ds.reset_index()[self.wk_ds['place_name'].isin(ccities)]\
                    [['auth_id','place_name','index']].groupby(['auth_id','place_name']).count().unstack().droplevel(0, axis=1)

        # Create function to aggregate all in text
        def series_agg(serie):
            text = str(len(serie))+"(#"+str(serie.sum())+" tweets - "
            for val in range(0,len(serie)):
                text =text+str(serie.values[val])+"_"+serie.index[val]
                if val < len(serie)-1:
                    text += ", "
                else:
                    text += ")"
            return text
        
        # Ordering by more locations and number of tweets
        pnt_3_ = pnt_3.agg(func=['count','sum'], axis=1).reset_index().sort_values(['count','sum'], ascending=[False, False]).\
                    reset_index(drop=True).reset_index(names='Rank').drop(['count','sum'],axis=1)

        # Create ranking variable
        pnt_3_['Rank'] = pnt_3_['Rank']+1

        # Change columns names
        pnt_3_.columns = ['Rank', 'Author Id']

        # Agregate twits and locations in text cells
        agreg = []
        for user in pnt_3_['Author Id']:
            agreg.append(series_agg(pnt_3.loc[user,:].dropna().astype(int)))
        pnt_3_['Number of Unique City Locations and #Tweets'] = agreg
        return pnt_3_

    

        
