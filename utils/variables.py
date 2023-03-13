# Json and csv files
json_twitter = './input/tinyTwitter.json'
json_geo = './input/sal.json'

#Creating refence lists to standirize data columns
#Greater Capital City Statistical Areas
# Regexp for gcca_codes
capitals_dict = {
    '.*ydney.*': '1gsyd',
    '.*elbourne.*':'2gmel',
    '.*risbane.*':'3gbri',
    '.*delaide.*':'4gade',
    '.*erth.*':'5gper',
    '.*obart.*':'6ghob',
    '.*arwin.*':'7gdar',
    '(.*anberra.*|.acc.)':'8acte',
    }
    
states_dict = {
    '.ew..outh..ales':'(nsw)',
    '.ictoria': '(vic)',
    '.ueensland':'(qld)',
    '.outh..ustralia':'(sa)',
    '.estern..ustralia':'(wa)',
    '.asmania':'(tas)',
    '.orthern..erritory.':'(nt)',
    '(.ustralian..apital..erritory)':'(acc)',
}


# Regexp for gcca_codes
replacements = {
    '.*\(nsw\).*':'1rnsw',
    '.*\(vic\).*': '2rvic',
    '.*\(qld\).*':'3rqld',
    '.*\(sa\).*':'4rsau',
    '.*\(wa\).*':'5rwau',
    '.*\(tas\).*':'6rtas',
    '.*\(nt\).*':'7rnte',
    }


#Greater Capital City Statistical Areas, point 1
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

#Capita Cities, point 3
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