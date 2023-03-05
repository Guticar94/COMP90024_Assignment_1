# Json and csv files
json_tweeter = 'twitter-data-small.json'
json_geo = 'sal.json'
json_enrich = 'georef.csv'

#Creating refence lists to standirize data columns
#Greater Capital City Statistical Areas
gcca_codes = {'1gsyd': 'Greater Sydney',
'1rnsw': 'Rest of NSW',
'2gmel': 'Greater Melbourne',
'2rvic': 'Rest of Vic.',
'3gbri': 'Greater Brisbane',
'3rqld': 'Rest of QLD',
'4gade': 'Greater Adelaide',
'4rsau': 'Rest of SA',
'5gper': 'Greater Perth',
'5rwau': 'Rest of WA',
'6ghob': 'Greater Hobart',
'6rtas': 'Rest of Tas.',
'7gdar': 'Greater Darwin',
'7rnte': 'Rest of NT',
'8acte': 'Australian Capital Territory',
'acc': 'Australia Capital Cities',
'roa': 'Australian Rest of States',
}

# Regexp for gcca_codes
replacements = {
    '.*ydney.*': '1gsyd',
    '.*outh..ales.*':'1rnsw',
    '.*elbourne.*':'2gmel',
    '.*ictoria.*': '2rvic',
    '.*risbane.*':'3gbri',
    '.*ueensland.*':'3rqld',
    '.*delaide.*':'4gade',
    '.*outh..ustralia.*':'4rsau',
    '.*erth.*':'5gper',
    '.*estern..ustralia.*':'5rwau',
    '.*obart.*':'6ghob',
    '.*asmania.*':'6rtas',
    '.*arwin.*':'7gdar',
    '.*orthern..erritory.*':'7rnte',
    '(.*ustralian..apital..erritory.*|.*anberra.*)':'8acte',
    '.*ustralia..apital..ities.*':'acc',
    '.*Australian.*tates.*':'roa',
    }

#Capita Cities
ccities = [
    '1gsyd',
    '2gmel',
    '3gbri',
    '4gade',
    '5gper',
    '6ghob',
    '7gdar',
    'acc'
]

#### Temporal files drop
#Remove csv files
import os
file_path = '/Users/guticar/Documents/COMP90024_Assignment_1/'
try:
    os.remove(file_path+'df1.csv')
except:
    print('No df1.csv file')
try:
    os.remove(file_path+'df2.csv')
except:
    print('No df2.csv file')
try:
    os.remove(file_path+'df3.csv')
except:
    print('No df3.csv file')