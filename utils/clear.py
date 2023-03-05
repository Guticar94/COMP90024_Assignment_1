
#### Temporal files drop
#Remove csv files
import os
file_path = './output/'
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