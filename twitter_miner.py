# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 15:51:33 2015

Based on the algorithms described on "Organization Mining Using Online Social 
Networks"

@author: aitor
"""
import tweepy
import networkx as nx

import json
import operator
import time

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
USER_TOKEN = ''
USER_SECRET = ''

CONFIG_FILEPATH = './confs/twitter.json'
NETWORKS_FILEPATH = './networks/'

MAX_NON_EMPLOYEES_PROCESSED = 1000
MIN_PRIORITY = 1
SEED_PRIORITY = 100
WAIT_MINS = 15

config = json.load(open(CONFIG_FILEPATH, 'r'))
CONSUMER_KEY = config['CONSUMER_KEY']
CONSUMER_SECRET = config['CONSUMER_SECRET']
USER_TOKEN = config['USER_TOKEN']
USER_SECRET = config['USER_SECRET']

# Initialize twitter connection
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
    
def _get__name_description(id_str):
    user = api.get_user(id_str)
    return user.screen_name, user.description


    
def _initialize_queue(ids):
    queue = {}
    for id_str in ids:
        # version 1
        #queue[id_str] = 1
        
        # version 2
        queue[id_str] = SEED_PRIORITY
        
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

def get_ids_from_screen_names(screen_names):
    ids = set()
    for screen_name in screen_names:
        print ' - %s' % screen_name
        user = api.get_user(screen_name)
        ids.add(user.id_str)
    return ids    
    
# Crawls twitter searching for organization X employees. 
# id_seeds: seed ids to start the search
# keywords: keywords to look for in the users' descriptions   
def crawl_organization(id_seeds, keywords):
    print "Starting crawl for (%s) seeds" % len(id_seeds)
    print "Keywords: %s"% keywords
    collected_ids = {}
    
    # 0- Initialize
    # queue of ids to be processed
    queue = _initialize_queue(id_seeds)
    # ids already processed
    crawled_ids = []
    
    # version 2
    # keeps tracks of how many non-employees have been processed since the last
    # discovered employees
    accum_non_employee = 0    
    
    # version 1
    # while len(queue > 0):
        
    # version 2
    while (accum_non_employee < MAX_NON_EMPLOYEES_PROCESSED):
        # 1- dequeue
        sorted_queue = _short_queue(queue)
        id_to_process = sorted_queue[-1][0]        
        priority = sorted_queue[-1][1]
        # version 2
        if priority <= MIN_PRIORITY:
            break
        queue.pop(id_to_process)
        # 2- Add to crawled
        crawled_ids.append(id_to_process)
        # 3- check description
        repeat = True
        while repeat:
            try:
                screen_name, description = _get__name_description(id_to_process)
                repeat = False
            except tweepy.error.TweepError:
                repeat = True
                print 'Time limit exceeded. Waiting %s mins' % WAIT_MINS
                time.sleep(WAIT_MINS * 60)
        
        has_keyword = _check_for_keywords(description, keywords)
        if has_keyword or id_to_process in id_seeds:
            print ' - Processing %s with priority %s'% (screen_name, priority)
            print ' - Queue lenght: %s' % len(queue)
            print ' - Mined ids: %s' % len(collected_ids)
            #version 2
            accum_non_employee = 0 
            # 4- get new ids to crawl
            repeat = True
            while repeat:
                try:
                    relations = _get_relations(id_to_process)
                    repeat = False
                except tweepy.error.TweepError:
                    repeat = True
                    print 'Time limit exceeded. Waiting %s mins' % WAIT_MINS
                    time.sleep(WAIT_MINS * 60)
            
            collected_ids[id_to_process] = {'screen_name': screen_name, 'friends' : relations['friends'],'followers' : relations['followers']}
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
        else:
            # version 2
            accum_non_employee += 1
            
        json.dump(collected_ids, open(NETWORKS_FILEPATH + 'collected_ids.json', 'w'), indent=2)
                    
    print 'Crawl finished, number of mined ids: %s' % len(collected_ids)
    return collected_ids

# Builds the graph using the ids collected by crawl_organizations   
def build_graph(collected_ids):
    G = nx.DiGraph()
    for user in collected_ids:
        G.add_node(screen_name = collected_ids[user]['screen_name'])
        friends = collected_ids[user]['friends']        
        for friend in friends:
            if friend in collected_ids:
                G.add_edge(user, friend)
                
        followers = collected_ids[user]['followers']
        for follower in followers:
            if follower in friends:
                G.add_edge(follower, user)
                
                
    return G

        
if __name__ == '__main__':
    print 'Starting...'
    #for siemens
    account_seeds = set(['@MichaelStal', '@darko_anicic', '@geri_revay', '@janinakugel', 
                     '@dubey_harishch', '@RosaRiera', '@ericspiegel', '@PauloRStark',
                     '@Krejman', '@Juergen_Maier', '@PekkoL', '@EHelminen', '@KatarinaNurmi1',
                     '@MarkkuKarja', '@Janne_Ohman', '@BrianHolliday01', '@mc_donagh9',
                     '@mikeahoughton', '@gilesy24', '@JSL_Kelly', '@Stephen_Barker2', 
                     '@Zogiot', '@janeshart', '@SavvasVerdis', '@MarkGJenkinson', 
                     '@PeterEHarrison', '@redwind11', '@SteveCMartin', '@AWiggin', 
                     '@lisagwinnell', '@Juergen_Maier', '@SoutherlyBreeze', 
                     '@LollyB_123', '@annieroberts1', '@stenmic', '@HeatherMachado', 
                     '@swindellwork', '@GaryAmosSFS', '@valiollahi', '@amantykorpi', 
                     '@stenmic', '@hodike', '@RMFranke', '@SusanCinadr', '@BenjaminSchroed', 
                     '@InesGiovannini1', '@huber_rolf', '@chrisrgh', '@RebeccaOttmann', 
                     '@UlrichEberl1', '@Syeda_AaJ', '@TurtonDanielle', '@meterservices', 
                     '@GaryAmosSFS'])
    
    keywords = set(['Siemens', 'siemens', 'SiemensPLM', 'Siemens_Ptbo', 'SiemensPLM_BNL',
                'SiemensHealthIT', 'Siemens_Traffic', 'SiemensPLM_DE', 'SiemensPLM_JP', 
                'Siemens_Ptbo', 'Siemens_DT_US', 'SiemensMobility', 'RollingOnRails', 
                'SiemensStiftung', 'siemens_salud', 'SiemensII', 'SiemensCampus', 
                'Siemens_SFS', 'siemens_es', 'SiemensHealth', 'SiemensUSA', 'Siemens_Suomi', 
                'Siemens_Italia', 'Siemens_Sverige', 'Siemens_Russia', 'Siemens_stampa', 
                'SiemensinCanada', 'siemens_press', 'Siemens_ARG', 'Siemens_Austria', 
                'siemensindustry', 'SiemensUKNews', 'SiemensTurkiye', 'Siemens_Brasil'])
                
#    print 'Recovering ids...'
#    id_seeds = get_ids_from_screen_names(account_seeds)
#    print 'Saving seed ids...'
#    json.dump(list(id_seeds), open('seed_ids_siemens.json','w'), indent=2)
    id_seeds = json.load(open('seed_ids_siemens.json','r'))
    print 'Crawling twitter...'
    collected_ids = crawl_organization(id_seeds, keywords)
    print 'Building graph...'
    G = build_graph(collected_ids)
    print 'Saving graph...'
    nx.write_gexf(G, NETWORKS_FILEPATH + 'twitter.gexf')
    
