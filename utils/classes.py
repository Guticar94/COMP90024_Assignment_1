#Import working libraries
import pandas as pd
from .variables import replacements, gcca_codes

class reading_json:
    def read_tweets(self, data):
        # Reading the twitter json

        self.df = pd.DataFrame({'auth_id':[data['data']['author_id']],\
                'place_name': [data['includes']['places'][0]['full_name']]})
    
        return self.df
    
    def read_geo(self, json_geo):
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
        #Here we haven't used yet the sal file. Plus the non Australian territories must be grouped in other categories
        self.pnt_1 = self.wk_ds['place_name'].value_counts().reset_index()
        self.pnt_1['index'] = self.pnt_1['index']+" ("+self.pnt_1.replace({"index": gcca_codes}).iloc[:,0]+")"
        self.pnt_1.columns = ['Greater Capital City', 'Number of Tweets Made']
        return self.pnt_1