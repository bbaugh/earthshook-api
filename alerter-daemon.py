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
import logging
from daemon import DaemonContext
from getpass import getpass
from checkpointer import checkpointer
from feed_interface import feed_interface
from twitter_interface import twitter_interface

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
    feed = feed_interface(config)
    twitter = twitter_interface(config)
    chckpnt = checkpointer(config)
    feed.last_modified,feed.features,twitter.tweets = chckpnt.load_checkpoint()
    
    delay = float(config['DEFAULT']['interval'])
    logging.debug('Starting app')
    #with context:
    #    while True:
    for i in range(3):
        if feed.check_feed():
            if feed.update():
                for feature in filter(lambda f: f['properties']['tweeted'] == False,feed.features):
                    if twitter.process_feature(feature):
                        feature['properties']['tweeted'] = True
                chckpnt.checkpoint(feed.last_modified,feed.features,twitter.tweets)
        sleep(delay)

# Start daemon
if __name__ == "__main__":
    main(args.config,getattr(logging, args.loglevel))

