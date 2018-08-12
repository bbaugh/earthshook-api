import logging
import tweepy

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
    
    def seen(self,keydict):
        '''
        Returns True if feature with id and properties.updated the same has been seen
        '''
        for f in self.tweets:
            seen = True
            for k,v in keydict.items():
                if k not in f:
                    raise ValueError('keydict contains items not see in tweets list')
                if f[k] != v:
                    seen = False
                    break
            if seen:
                return True
        return False
    
    def tweet(self,tweet,keydict):
        logging.debug('processing feature')
        if self.seen(keydict):
            logging.debug('already tweeted')
            return True
    
        stt = self.api.update_status(**tweet)
        logging.debug(stt)

        archdict = {'twitter_id' : self.twitter_id}
        for k,v in keydict.items():
            archdict[k] = v
        self.tweets.append(archdict.copy())
        self.twitter_id += 1
        logging.debug('{} - {}'.format(tweet,self.twitter_id))
        return True