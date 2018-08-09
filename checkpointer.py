from datetime import datetime, timedelta, timezone
from os import path, walk
from json import loads as jsloads
from json import dumps as jsdumps
from gzip import GzipFile
import logging
import pathlib


def clean_features(min_time,features):
    '''
    Returns features newer than min_time
    '''
    return (filter(lambda f: f['properties']['time'] > min_time,features))

def clean_tweets(min_time,tweets):
    '''
    Returns tweets newer than min_time
    '''
    return (filter(lambda f: f['time'] > min_time,tweets))

class checkpointer():
    def __init__(self,config):
        self.time_format = config['DEFAULT']['time_format']
        self.buffer_days = int(config['DEFAULT']['buffer_days'])
        self.buffer_tdelta = timedelta(days=self.buffer_days)
        self.checkpoint_dir = config['DEFAULT']['checkpoint_dir']
        filenames = []
        if path.isdir(self.checkpoint_dir):
            _, _, filenames = next(walk(self.checkpoint_dir), (None, None, []))
        else:
            pathlib.Path(self.checkpoint_dir).mkdir(parents=True, exist_ok=True)
        self.checkpoint_files = filenames

    def checkpoint(self,last_modified,features,tweets):
        '''
        Saves current data to checkpoint.
        '''
        now = datetime.now(timezone.utc)
        curfile = path.join(self.checkpoint_dir,'{}.json.gz'.format(now.strftime('%Y%m')))
        logging.debug('checkpointing buffer')
        mon_beg = int(datetime(now.year,now.month,1).timestamp()*1000)

        outobj = {'metadata' : { 'Last-Modified' : last_modified },\
                  'features' : list(clean_features(mon_beg,features)),\
                  'tweets'   : list(clean_tweets(mon_beg,tweets)) }
        with GzipFile(curfile, 'w') as fout:
            fout.write(jsdumps(outobj).encode('utf-8'))
        outobj = None
        
        min_time = int((now - self.buffer_tdelta).timestamp()*1000)
        return last_modified,list(clean_features(min_time,features)), list(clean_tweets(min_time,tweets))
        
    def load_checkpoint(self):
        '''
        Loads checkpoint file
        '''
        logging.debug('loading buffer')
        now = datetime.now(timezone.utc)
        curfile = path.join(self.checkpoint_dir,'{}.json.gz'.format(now.strftime('%Y%m')))
        if path.exists(curfile):
            with GzipFile(curfile, 'r') as fin:
                inobj = jsloads(fin.read().decode('utf-8'))
                last_modified = inobj['metadata']['Last-Modified']
                logging.debug('Last-modified from checkpoint is {}'.format(last_modified))
                features = inobj['features']
                logging.debug('Loaded {} features from checkpoint.'.format(len(features)))
                tweets = inobj['tweets']
                logging.debug('Loaded {} tweets from checkpoint.'.format(len(tweets)))

            min_time = int((now - self.buffer_tdelta).timestamp()*1000)
            return last_modified,list(clean_features(min_time,features)), list(clean_tweets(min_time,tweets))
        return (now - timedelta(days=1)).timestamp(),list(),list()
