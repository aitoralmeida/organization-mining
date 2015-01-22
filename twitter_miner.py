# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 15:51:33 2015

@author: aitor
"""
import tweepy
import networkx as nx

import json
import operator

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
USER_TOKEN = ''
USER_SECRET = ''

CONFIG_FILEPATH = './confs/twitter.json'

config = json.load(open(CONFIG_FILEPATH, 'r'))
CONSUMER_KEY = config['CONSUMER_KEY']
CONSUMER_SECRET = config['CONSUMER_SECRET']
USER_TOKEN = config['USER_TOKEN']
USER_SECRET = config['USER_SECRET']
 
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(USER_TOKEN, USER_SECRET)

api = tweepy.API(auth)

def _get_relations(id_str):
    relations = {} #{'friends' : [], 'followers': []}
    user = api.get_user(id_str)
    followers = user.followers_ids()
    friends = user.friends()
    relations['friends'] = [f.id_str for f in friends]
    relations['followers'] = followers
    
    return relations
    
def _get_description(id_str):
    user = api.get_user(id_str)
    return user.description
    
def _initialize_queue(ids):
    queue = {}
    for id_str in ids:
        queue[id_str] = 1
        
    return queue
        
def _short_queue(queue):
    sorted_queue = sorted(queue.items(), key=operator.itemgetter(1))
    return sorted_queue
    
def _check_for_keywords(description, keywords):
    has_keyword = False
    
    for word in keywords:
        if word in description:
            has_keyword = True
            break
        
    return has_keyword
    
    
    
def crawl_organization(id_seeds, keywords):
    collected_ids = {}
    
    # 0- Initialize
    queue = _initialize_queue(id_seeds)
    crawled_ids = []
    while len(queue > 0):
        # 1- dequeue
        sorted_queue = _short_queue(queue)
        id_to_process = sorted_queue[-1][0]
        queue.pop(id_to_process)
        # 2- Add to crawled
        crawled_ids.append[id_to_process]
        # 3- check description
        description = _get_description(id_to_process)
        has_keyword = _check_for_keywords(description, keywords)
        if has_keyword:
            # 4- get new ids to crawl
            relations = _get_relations(id_to_process)
            collected_ids[id_to_process] = {'friends' : relations['friends'],'followers' : relations['followers']}
            unfiltered_relations = set(relations['friends'] + relations['followers'])
            new_ids = [id_str for id_str in unfiltered_relations if id_str not in crawled_ids]
            # 5- increase priority
            for id_str in queue:
                if id_str in new_ids:
                    queue[id_str] += 1
            # 6- add new ids to queue     
            for id_str in new_ids:
                if not id_str in queue:
                    queue[id_str] = 1
                    
    return collected_ids
    
def build_graph(collected_ids):
    G = nx.DiGraph()
    for user in collected_ids:
        friends = collected_ids[user]['friends']        
        for friend in friends:
            if friend in collected_ids:
                G.add_edge(user, friend)
                
        followers = collected_ids[user]['followers']
        for follower in followers:
            if follower in friends:
                G.add_edge(follower, user)
                
    return G
    
        
        
        
    
    
