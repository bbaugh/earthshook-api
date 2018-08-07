#!/usr/bin/env python3
################################################################################
#  alerter-daemon.py
#
#  Script daemonizes sending of alerts.
#
#  Created by Brian Baughman on 2014-10-16.
#  Copyright 2018 Brian Baughman. All rights reserved.
################################################################################
from os import path, _exit, devnull
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from collections import defaultdict, Counter
from dateutil import parser as dateparser
from datetime import datetime, timedelta, timezone
from time import sleep
import logging
from daemon import DaemonContext
from urllib3 import PoolManager
from json import loads as jsloads
from json import dumps as jsdumps
from gzip import GzipFile

_pathname = path.dirname(path.abspath(__file__))

parser = ArgumentParser(description='Creates a daemon for sending alerts.')


parser.add_argument('-c','--config',type=FileType('r'),\
                    action="store", dest="config", \
                    default='earthshook-api.ini',\
                    help="Config file for daemon.")

parser.add_argument('-v', '--verbose', \
                    dest="loglevel", default='INFO',\
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],\
                    help="Set the logging level")

args = parser.parse_args()


_defaults = {}
_defaults['feedurl'] = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson'
_defaults['interval'] = 60
_defaults['pidfile'] = r'/tmp/.earthshook-api-daemon.lock'
_defaults['logfile'] = devnull
def check_config(cfg):
    '''
    Checks the config file and sets defaults if necesary
    '''
    if 'DEFAULT' not in cfg:
        raise ValueError('config does not contain DEFAULT section')
    for k,v in _defaults.items():
        if k not in cfg['DEFAULT']:
            cfg['DEFAULT'][k] = v

def check_resp_data(data):
    '''
    Makes sure that the data returned from RESP API is as expected.
    '''
    if data is None:
        logging.debug('response data is None')
        return None
    try:
        logging.debug('read JSON')
        return jsloads(data.decode('utf-8'))
    except:
        logging.debug('failed to load JSON')
        return None

class checker_app():
    def __init__(self,config):
        self.feedurl = config['DEFAULT']['feedurl']
        self.time_format = config['DEFAULT']['time_format']
        self.buffer_days = int(config['DEFAULT']['buffer_days'])
        self.buffer_tdelta = timedelta(days=self.buffer_days)
        self.bufferfile = config['DEFAULT']['bufferfile']
        self.pool = PoolManager()
        self.buffer = list()
        self.buffer_accounting = defaultdict(set)
        self.last_modified = (datetime.now(timezone.utc) \
                             - timedelta(days=self.buffer_days)).timestamp()
        if path.isfile(self.bufferfile):
            self.load_checkpoint()
            self.clean_buffer()
        
    def check_feed(self):
        '''
        returns True if Last-Modified date is after previously seen value
        '''
        logging.debug('checking feed')
        resp = self.pool.request('HEAD', self.feedurl)
        if resp.status == 200:
            logging.debug('head state 200')
            cur_last_mod =  dateparser.parse(resp.getheader('Last-Modified')).timestamp()
            logging.debug('Current last-modified {}'.format(cur_last_mod))
            return cur_last_mod > self.last_modified
        logging.debug('Failed head')
        return False
        
    def get_feed(self):
        '''
        simple wrapper that checks status of feed
        '''
        logging.debug('requesting feed')
        resp = self.pool.request('GET', self.feedurl)
        if resp.status == 200:
            logging.debug('get state 200')
            return resp
        logging.debug('Failed request')
        return None
        
    def clean_buffer(self):
        '''
        Filters out buffer outside the buffer_days window
        '''
        logging.debug('cleaning buffer')
        min_time = int((datetime.now(timezone.utc) - self.buffer_tdelta).timestamp()*1000)
        self.buffer = list(filter(lambda f: f['properties']['time'] > min_time,self.buffer))
        self.buffer_accounting = defaultdict(set)
        for f in self.buffer:
            self.buffer_accounting[f['id']].add(f['properties']['updated'])
        
    def load_checkpoint(self):
        '''
        Loads checkpoint file into buffer
        '''
        logging.debug('loading buffer')
        with GzipFile(self.bufferfile, 'r') as fin:
            inobj = jsloads(fin.read().decode('utf-8'))
            self.last_modified = inobj['metadata']['Last-Modified']
            logging.debug('Last-modified from checkpoint is {}'.format(self.last_modified))
            self.buffer = inobj['features']
            logging.debug('Loaded {} features from checkpoint into buffer.'.format(len(self.buffer)))
    
    def checkpoint_buffer(self):
        '''
        Saves current buffer to bufferfile.
        '''
        logging.debug('checkpointing buffer')
        outobj = {'metadata' : { 'Last-Modified' : self.last_modified },\
                  'features' : self.buffer }
        with GzipFile(self.bufferfile, 'w') as fout:
            fout.write(jsdumps(outobj).encode('utf-8'))
        outobj = None
    
    def get_props_summary(self,props):
        '''
        Creates standardized summary string from properties
        '''
        feat_time = datetime.fromtimestamp(props['time']/1000.,tz=timezone.utc)
        return '{} occured at {}. {} {} located {}'.format(props['type'].capitalize(),\
                             feat_time.strftime(self.time_format),\
                             props['magType'],props['mag'],
                             props['place'])
        
    def process_feature(self,feature):
        logging.debug('processing feature')
        psum = self.get_props_summary(feature['properties'])
        logging.debug(psum)
        self.buffer.append(feature)
    
    def update(self):
        logging.debug('Updating')
        resp = self.get_feed()
        if resp is None:
            return None
        data = check_resp_data(resp.data)
        if data is None:
            return None
        if 'features' not in data:
            return None
        nNew = 0
        for feat in data['features']:
            if feat['id'] in self.buffer_accounting:
                if feat['properties']['updated'] in self.buffer_accounting[feat['id']]:
                    continue
            self.process_feature(feat)
            nNew += 1
        self.last_modified =  dateparser.parse(resp.getheader('Last-Modified')).timestamp()
        logging.debug('Updated Last-Modified to {}'.format(self.last_modified))
        self.clean_buffer()
        self.checkpoint_buffer()
        logging.debug('{} new features.'.format(nNew))
        

def main(configfile,loglevel):
    config = ConfigParser()
    config.read_file(configfile)
    check_config(config)
    logging.basicConfig(format='%(asctime)s %(message)s',\
                        filename=config['DEFAULT']['logfile'],\
                        level=loglevel)

    logging.debug('creating daemon context')
    context = DaemonContext(umask=0o002,
                            files_preserve=[config['DEFAULT']['logfile']],
                            pidfile=config['DEFAULT']['pidfile'])
    logging.debug('initializing checker_app')
    app = checker_app(config)
    delay = float(config['DEFAULT']['interval'])
    logging.debug('Starting app')
    #with context:
    #    while True:
    for i in range(3):
        if app.check_feed():
            app.update()
        sleep(delay)

# Start daemon
if __name__ == "__main__":
    main(args.config,getattr(logging, args.loglevel))

