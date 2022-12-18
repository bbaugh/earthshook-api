#!/usr/bin/env python3
################################################################################
#  alerter-daemon.py
#
#  Script daemonizes sending of alerts.
#
#  Created by Brian Baughman on 2014-10-16.
#  Copyright 2018 Brian Baughman. All rights reserved.
################################################################################
from os import path, devnull
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from time import sleep
from datetime import datetime, timedelta, timezone
import logging
from daemon import DaemonContext
from daemon.pidfile import TimeoutPIDLockFile
from getpass import getpass
from checkpointer import checkpointer
from feed_interface import feed_interface, feature2key
from twitter_interface import twitter_interface

_pathname = path.dirname(path.abspath(__file__))

parser = ArgumentParser(description='Creates a daemon for sending alerts.')


parser.add_argument('-c','--config',type=str,\
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
_defaults['time_format'] = '%%Y-%%m-%%dT%%H:%%M:%%S %%Z'

_twitter_keys = [ 'api_key', 'api_secret', 'access_token','access_token_secret']
def check_config(cfg):
    '''
    Checks the config file and sets defaults if necesary
    '''
    if 'DEFAULT' not in cfg:
        raise ValueError('config does not contain DEFAULT section')
    for k,v in _defaults.items():
        if k not in cfg['DEFAULT']:
            cfg['DEFAULT'][k] = v
    if 'Twitter' not in cfg:
        cfg['Twitter'] = {}
    for k in _twitter_keys:
        if k not in cfg['Twitter']:
            cfg['Twitter'][k] = getpass(prompt='Please provide {}: '.format(k))
    

def get_summary(props,time_format):
    '''
    Creates standardized summary string from feature properties using given time_format(strftime)
    '''
    feat_time = datetime.fromtimestamp(props['time']/1000.,tz=timezone.utc)
    return '{} {} {} ({}) occured at {}, {} {}'.format(\
        props['magType'].capitalize(),props['mag'],\
        props['type'],\
        props['status'],\
        feat_time.strftime(time_format),\
        props['place'],\
        props['url'])
                         
def feature2tweet(feature,time_format):
    '''
    Constructs a dictionary to pass to tweepy.update_status from USGS GEOJson feature.
    '''
    logging.debug('processing feature to tweet')
    tweetdict = {}
    tweetdict['status'] = get_summary(feature['properties'],time_format)
    flong, flat, fdepth = feature['geometry']['coordinates']
    tweetdict['lat'] = float(flat)
    tweetdict['long'] = float(flong)
    return tweetdict

def clnd_and_sorted(features,time_format):
    flt_func = lambda f: f['properties']['tweeted'] == False
    srt_func = lambda f: f['properties']['time']
    for feature in sorted(filter(flt_func,features),key=srt_func):
        fkey = feature2key(feature)
        fpld = feature2tweet(feature,time_format)
        yield fkey,fpld, feature
        
def run(config,loglevel):
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',\
                        filename=config['DEFAULT']['logfile'],\
                        filemode='w',\
                        level=loglevel)
    time_format = config['DEFAULT']['time_format']
    feed = feed_interface(config)
    twitter = twitter_interface(config)
    chckpnt = checkpointer(config)
    feed.last_modified,feed.features,twitter.tweets = chckpnt.load_checkpoint()
    
    delay = float(config['DEFAULT']['interval'])
    logging.debug('Starting app')
    while True:
        if feed.check_feed():
            if feed.update():
                for fkey,fpld,feature in clnd_and_sorted(feed.features,time_format):
                    if twitter.tweet(fpld,fkey):
                        feature['properties']['tweeted'] = True
                feed.last_modified,feed.features,twitter.tweets = chckpnt.checkpoint(feed.last_modified,feed.features,twitter.tweets)
        sleep(delay)
def main(configfile,loglevel):
    config = ConfigParser()
    config.read(configfile)
    check_config(config)
    pfiles = []
    serr = devnull
    ferr = config['DEFAULT']['logfile']
    if ferr.find('.log') != -1:
        ferr = ferr.replace('.log','.err')
        serr = open(ferr,'w')
        pfiles.append(serr)
    with DaemonContext(umask=0o002,\
                       files_preserve=pfiles,\
                       stderr=serr,\
                       pidfile=TimeoutPIDLockFile(config['DEFAULT']['pidfile'])) as context:
        run(config,loglevel)
        

# Start daemon
if __name__ == "__main__":
    main(args.config,getattr(logging, args.loglevel))

