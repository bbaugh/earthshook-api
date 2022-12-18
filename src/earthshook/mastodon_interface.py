import logging

class mastodon_interface():
    def __init__(self,config):
        self.time_format = config['DEFAULT']['time_format']
        self.api_key = config['mastodon']['api_key']
        self.api_secret = config['mastodon']['api_secret']
        self.access_token = config['mastodon']['access_token']
        self.access_token_secret = config['mastodon']['access_token_secret']
        self.auth = tweepy.OAuthHandler(self.api_key, self.api_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth)
        self.posts = list()

    def seen(self,keydict):
        '''
        Returns True if feature with id and properties.updated the same has been seen
        '''
        for f in self.posts:
            seen = True
            for k,v in keydict.items():
                if k not in f:
                    raise ValueError('keydict contains items not see in posts list')
                if f[k] != v:
                    seen = False
                    break
            if seen:
                return True
        return False
    
    def post(self,post,keydict):
        logging.debug('processing feature')
        if self.seen(keydict):
            logging.debug('already posted')
            return True
        try:
            stt = self.api.update_status(**post)
        except tweepy.error.TweepError as excpt:
            logging.error(excpt)
            if excpt.api_code == 187:
                logging.error('{} - duplicated'.format(keydict))
                return True
            logging.error('{} - {}'.format(keydict,excpt.api_code))
            return False
        post_id = stt.id
        archdict = {'mastodon_id' : post_id}
        for k,v in keydict.items():
            archdict[k] = v
        self.posts.append(archdict.copy())
        logging.debug('{} - {}'.format(post,post_id))
        return True