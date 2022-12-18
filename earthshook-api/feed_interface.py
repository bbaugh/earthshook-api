from dateutil import parser as dateparser
from datetime import datetime, timezone, timedelta
import logging
from urllib3 import PoolManager
from json import loads as jsloads

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

def feature2key(feature):
    '''
    Returns a unique key dict for the feature
    '''
    return { 'id' : feature['id'] , 'time' : feature['properties']['time'], 'updated' : feature['properties']['updated'] }

class feed_interface():
    def __init__(self,config):
        self.feedurl = config['DEFAULT']['feedurl']
        self.pool = PoolManager(cert_reqs='CERT_REQUIRED', \
                                ca_certs='/etc/ssl/certs/ca-certificates.crt')
        self.features = list()
        self.last_modified = (datetime.now(timezone.utc) - timedelta(days=1)).timestamp()

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
            if resp.data is None:
                return None,None
            try:
                logging.debug('read JSON')
                return resp.getheader('Last-Modified'),jsloads(resp.data.decode('utf-8'))
            except:
                logging.debug('failed to load JSON')
                return None,None
        logging.debug('Failed request')
        return None,None

        
    def seen(self,feature):
        '''
        Returns True if feature with id and properties.updated the same has been seen
        '''
        fid = feature['id']
        fupdated = feature['properties']['updated']
        for f in self.features:
            if f['id'] != fid:
                continue
            if f['properties']['updated'] != fupdated:
                continue
            return True
        return False
    
    def update(self):
        logging.debug('Updating')
        last_modified_str,data = self.get_feed()
        if data is None or last_modified_str is None:
            return False
        if 'features' not in data:
            return False
        nNew = 0
        nSeen = 0
        for feature in data['features']:
            if self.seen(feature):
                nSeen += 1
                continue
            feature['properties']['tweeted'] = False
            self.features.append(feature)
            nNew += 1
        self.last_modified =  dateparser.parse(last_modified_str).timestamp()
        logging.debug('Updated Last-Modified to {}'.format(self.last_modified))
        logging.debug('{} new features. {} skipped.'.format(nNew,nSeen))
        return True
