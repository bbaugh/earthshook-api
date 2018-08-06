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
from argparse import ArgumentParser
from configparser import ConfigParser
from collections import defaultdict, Counter
from time import sleep
import logging
from daemon import DaemonContext
from urllib3 import PoolManager
from json import loads as jsloads

_pathname = path.dirname(path.abspath(__file__))
_usgs_url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson'

parser = ArgumentParser(description='Creates a daemon for sending alerts.')


parser.add_argument("--interval",\
                    action="store", dest="interval", \
                    default=60,\
                    help="Number of seconds to wait between polls.")

parser.add_argument("--pidfile",\
                    action="store", dest="pidfile", \
                    default='/tmp/.alerter-daemon.lock',\
                    help="Lock file for daemon.")

parser.add_argument("--logfile",\
                    action="store", dest="logfile", \
                    default=devnull,\
                    help="Log file for daemon.")
                    
parser.add_argument("--feedurl",\
                    action="store", dest="feedurl", \
                    default=_usgs_url,\
                    help="USGS Feed URL.")

args = parser.parse_args()

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
        
    def get_feed(self):
        '''
        pool - instance of urllib3.PoolManager
        feedurl - URL to query for new items (https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)
        '''
        logging.debug('requesting feed')
        resp = self.pool.request('GET', self.feedurl)
        if resp.status == 200:
            logging.debug('request state 200')
            return check_resp_data(resp.data)
        logging.debug('Failed request')
        return None

    def update(self):
        logging.debug('Updating')
        data = self.get_feed()
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
        logging.debug('{} new features.'.format(nNew))
        

def main(feedurl,interval,logfile,pidfile):
    logging.basicConfig(format='%(asctime)s %(message)s',filename=logfile, level=logging.INFO)
    context = DaemonContext(umask=0o002,
                            files_preserve=[logfile],
                            pidfile=pidfile)
    app = checker_app(feedurl)
    delay = float(interval)
    logging.debug('Starting app')
    #with context:
    while True:
        app.update()
        sleep(delay)

# Start daemon
if __name__ == "__main__":
    main(args.feedurl,args.interval,args.logfile,args.pidfile)

