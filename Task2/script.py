from subprocess import PIPE, Popen
from os import listdir
from datetime import datetime
import time
import json
from os.path import isfile, join
import os
import pathlib
import pandas as pd 
from pandas.io.json import json_normalize 
from urllib.parse import urlparse
import shutil
import argparse

start = time.time()
#One optional argument -u
parser = argparse.ArgumentParser()
parser.add_argument("path", help="please enter your directory path")
parser.add_argument("-u", action="store_true", dest="format", default=False,help="please change your time format")
args = parser.parse_args()

#check duplicates
files = [item for item in listdir(args.path) if isfile(join(args.path, item)) if "json" in item]
checksums = {}
duplicates = []
for filename in files:
    with Popen(["md5sum", filename], stdout=PIPE) as proc:
        checksum = proc.stdout.read().split()[0]
        if checksum in checksums:
            os.remove(filename)
            duplicates.append(filename)
        checksums[checksum] = filename

items = os.scandir(args.path)
files = []
for item in items:
    if item.is_file() and '.json' in item.name and '-Done' not in item.name:
        files.append(item.name) 
#read files     
for file in files:
    records = [json.loads(line) for line in open (file) if '_heartbeat_' not in json.loads(line )]
    #convert the list to a dataframe
    df = json_normalize(records)
    ######################################Transformation#################################################
    # Drop nan
    df=df.dropna()
    # getting web browser and os info
    df['web_browser'] = df['a'].str.split('(').str[0]
    df['operating_sys'] = df['a'].str.split('(').str[1]
    #url shorter
    df['from_url'] = df.apply(lambda row: urlparse(row['r']).netloc if 'http' in row['r'] else row['r'] , axis = 1)
    df['to_url'] = df.apply(lambda row: urlparse(row['u']).netloc if 'http' in row['u'] else row['u'] , axis = 1)

    # find the city from that the request was initiated
    df['city'] = df['cy']

    # track the longitude where the request was sent
    df[['longitude','latitude']]= pd.DataFrame(df.ll.tolist(), index= df.index)

    # Retrieve each city time zone
    df['time_zone']=df['tz']


    # convert time_in and time_out to local timezone
    df['time_in']=df['t']
    df['time_in'] = pd.to_datetime(df['time_in'], unit = 's').dt.tz_localize('UTC').dt.tz_convert('UTC')
    
    df['time_out']=df['hc']
    df['time_out'] = pd.to_datetime(df['time_out'], unit = 's').dt.tz_localize('UTC').dt.tz_convert('UTC')
   
    if args.format:
       df['time_in'] = pd.to_datetime(df['time_in'], unit = 's').dt.tz_localize('UTC').dt.tz_convert('UTC')
       df['time_out'] = pd.to_datetime(df['time_out'], unit = 's').dt.tz_localize('UTC').dt.tz_convert('UTC')
       df = df[['web_browser','operating_sys','from_url','to_url','city','longitude','latitude','time_zone','time_in','time_out']]
       name = file.split('.json')
       df.to_csv(args.path+'target/'+name[0]+'.csv')
       os.rename(file,name[0]+'-Done.json')
       num_lines = sum(1 for line in open(args.path+'target/'+name[0]+'.csv'))
       print(num_lines)
       print(args.path+'target/'+name[0]+'.csv')
    else :
      df = df[['web_browser','operating_sys','from_url','to_url','city','longitude','latitude','time_zone','time_in','time_out']]
      name = file.split('.json')
      df.to_csv(args.path+'target/'+name[0]+'.csv')
      os.rename(file,name[0]+'-Done.json')
      num_lines = sum(1 for line in open(args.path+'target/'+name[0]+'.csv'))
      print(num_lines)
      print(args.path+'target/'+name[0]+'.csv')

#the total excution time
print("%s total excution time: " %(time.time() - start))
