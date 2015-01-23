# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 09:14:53 2015

@author: aitor
"""

from linkedin import linkedin
import json

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
USER_TOKEN = ''
USER_SECRET = ''

RETURN_URL = '' # Not required for developer authentication

CONFIG_FILEPATH = './confs/linkedin.json'

def _load_config(filepath):
    config = json.load(open(filepath, 'r'))
    CONSUMER_KEY = config['CONSUMER_KEY']
    CONSUMER_SECRET = config['CONSUMER_SECRET']
    USER_TOKEN = config['USER_TOKEN']
    USER_SECRET = config['USER_SECRET']
    
def _authenticate():
    auth = linkedin.LinkedInDeveloperAuthentication(CONSUMER_KEY, CONSUMER_SECRET, 
                                USER_TOKEN, USER_SECRET, 
                                RETURN_URL, 
                                permissions=linkedin.PERMISSIONS.enums.values())
    app = linkedin.LinkedInApplication(auth)
    return app

if __name__ == '__main__':
    print 'Loading config...'
    _load_config(CONFIG_FILEPATH)
    print 'Authenticating...'
    app = _authenticate()
    
    
    print 'DONE'