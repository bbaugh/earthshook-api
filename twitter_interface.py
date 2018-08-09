from datetime import datetime, timedelta, timezone
import logging
import tweepy

def get_props_summary(props,time_format):
    '''
    Creates standardized summary string from feature properties using given time_format(strftime)
    '''
    feat_time = datetime.fromtimestamp(props['time']/1000.,tz=timezone.utc)
    return '{} occured at {}. {} {} located {}'.format(props['type'].capitalize(),\
                         feat_time.strftime(time_format),\
                         props['magType'],props['mag'],
                         props['place'])


class twitter_interface():
    def __init__(self,config):
        self.time_format = config['DEFAULT']['time_format']
        self.api_key = config['Twitter']['api_key']
        self.api_secret = config['Twitter']['api_secret']
        self.access_token = config['Twitter']['access_token']
        self.access_token_secret = config['Twitter']['access_token_secret']
        self.auth = tweepy.OAuthHandler(self.api_key, self.api_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth)
        self.tweets = list()
        self.twitter_id = 0
    
    def seen(self,feature):
        '''
        Returns True if feature with id and properties.updated the same has been seen
        '''
        fid = feature['id']
        fupdated = feature['properties']['updated']
        for f in self.tweets:
            if f['id'] != fid:
                continue
            if f['updated'] != fupdated:
                continue
            return True
        return False
    
    def prop_sum(self,props):
        '''
        Creates standardized summary string from feature properties using given time_format(strftime)
        '''
        feat_time = datetime.fromtimestamp(props['time']/1000.,tz=timezone.utc)
        return '{} occured at {}. {} {} located {}'.format(props['type'].capitalize(),\
                             feat_time.strftime(self.time_format),\
                             props['magType'],props['mag'],
                             props['place'])
    def process_feature(self,feature):
        logging.debug('processing feature')
        if self.seen(feature):
            logging.debug('already tweeted')
            return True
        psum = self.prop_sum(feature['properties'])
        logging.debug(psum)
        flong, flat, fdepth = feature['geometry']['coordinates']
        '''
        stt = api.update_status(status=psum,\
                                lat=float(flat),\
                                long=float(flong))
        loggin.debug(stt)
        '''
        self.tweets.append({'id' : feature['id'],\
                            'time' : feature['properties']['time'],\
                            'updated' : feature['properties']['updated'],\
                            'twitter_id' : self.twitter_id })
        self.twitter_id += 1
        logging.debug('{} - {}'.format(feature['id'],self.twitter_id))
        return True