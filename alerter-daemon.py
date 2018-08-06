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
    def __init__(self,feedurl):
        self.feedurl = feedurl
        self.pool = PoolManager()
        self.events = defaultdict(set)
        self.last_modified = datetime.now(timezone.utc) - timedelta(days=1)
        
    def check(self):
        logging.debug('checking feed')
        resp = self.pool.request('HEAD', self.feedurl)
        if resp.status == 200:
            logging.debug('head state 200')
            cur_last_mod =  dateparser.parse(resp.getheader('Last-Modified'))
            return cur_last_mod > self.last_modified
        logging.debug('Failed head')
        return False
        
    def get_feed(self):
        logging.debug('requesting feed')
        resp = self.pool.request('GET', self.feedurl)
        if resp.status == 200:
            logging.debug('get state 200')
            return resp
        logging.debug('Failed request')
        return None

    def update(self):
        logging.debug('Updating')
        resp = self.get_feed()
        data = check_resp_data(resp.data)
        if data is None:
            return None
        if 'features' not in data:
            return None
        known_feats = Counter(  v for vals in self.events.values() for v in vals )
        nNew = 0
        for feat in data['features']:
            if 'properties' not in feat or 'id' not in feat:
                continue
            if feat['id'] in known_feats:
                continue
            props = feat['properties']
            if 'time' not in props or 'title' not in props or 'type' not in props:
                continue
            self.events[props['time']].add(feat['id'])
            nNew += 1
        self.last_modified =  dateparser.parse(resp.getheader('Last-Modified'))
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
    app = checker_app(config['DEFAULT']['feedurl'])
    delay = float(config['DEFAULT']['interval'])
    logging.debug('Starting app')
    #with context:
    #    while True:
    for i in range(3):
        if app.check():
            app.update()
        sleep(delay)

# Start daemon
if __name__ == "__main__":
    main(args.config,getattr(logging, args.loglevel))

