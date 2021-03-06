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
import sys

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
USER_TOKEN = ''
USER_SECRET = ''

CONFIG_FILEPATH = './confs/twitter.json'
NETWORKS_FILEPATH = './networks/'
STATUS_FILEPATH = './ongoing_crawls/'

MAX_NON_EMPLOYEES_PROCESSED = 4000
MIN_PRIORITY = 1
SEED_PRIORITY = 30
BASE_PRIORITY = 1
WAIT_MINS = 5

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
        if word.lower() in description.lower():
            has_keyword = True
            break
        
    return has_keyword
    
def _save_status(collected_ids, queue, crawled_ids):
    print 'Saving status...'
    json.dump(collected_ids, open(STATUS_FILEPATH + 'collected_ids_twitter.json', 'w'))
    json.dump(queue, open(STATUS_FILEPATH + 'queue_twitter.json', 'w'))
    json.dump(crawled_ids, open(STATUS_FILEPATH + 'crawled_ids_twitter.json', 'w'))
    print '...saved'
    
def _load_status():    
    collected_ids = json.load(open(STATUS_FILEPATH + 'collected_ids_twitter.json', 'r'))
    queue = json.load(open(STATUS_FILEPATH + 'queue_twitter.json', 'r'))
    crawled_ids = json.load(open(STATUS_FILEPATH + 'crawled_ids_twitter.json', 'r'))
    print 'Status loaded...'
    
    return collected_ids, queue, crawled_ids

def get_ids_from_screen_names(screen_names):
    ids = set()
    for screen_name in screen_names:
        print ' - %s' % screen_name
        try:
            user = api.get_user(screen_name.strip())
            ids.add(user.id_str)
        except:
            print 'Error recovering the id'
    return ids    
    
# Crawls twitter searching for organization X employees. 
# id_seeds: seed ids to start the search
# keywords: keywords to look for in the users' descriptions   
def crawl_organization(id_seeds, keywords, collected_ids, queue, crawled_ids):
    print "Starting crawl for (%s) seeds" % len(id_seeds)
    print "Keywords: %s"% keywords
    print "Already collected ids: %s" % len(collected_ids)
   
    #collected_ids = {}
    
    # 0- Initialize
    # queue of ids to be processed
    # already done outside the function
    #queue = _initialize_queue(id_seeds)
    # ids already processed
    #crawled_ids = []
    
    # version 2
    # keeps tracks of how many non-employees have been processed since the last
    # discovered employees
    accum_non_employee = 0    
    
    # version 1
    # while len(queue > 0):
        
    tot = 0
        
    # version 2
    while (accum_non_employee < MAX_NON_EMPLOYEES_PROCESSED):
        if tot == 20:
            _save_status(collected_ids, queue, crawled_ids)
            tot = 0            
        tot+=1
        # 1- dequeue
        sorted_queue = _short_queue(queue)
        id_to_process = sorted_queue[-1][0]        
        priority = sorted_queue[-1][1]
        # version 2
        if priority <= MIN_PRIORITY:
            break
        queue.pop(id_to_process)
        # 2- Add to crawled
        if id_to_process in crawled_ids:
            continue
        
        crawled_ids.append(id_to_process)
        # 3- check description
        repeat = True
        while repeat:
            try:
                screen_name, description = _get__name_description(id_to_process)
                repeat = False
            except tweepy.error.TweepError as e:
                _save_status(collected_ids, queue, crawled_ids)
                repeat = True
                print '(%s) Time limit exceeded. Waiting %s mins' % (time.ctime(), WAIT_MINS)
                print '\t', e
                sys.stdout.flush()
                try:
                    if e.args[0][0]['code'] == 88:
                        time.sleep(WAIT_MINS * 60)
                    else:
                        repeat = False
                except:
                        repeat = False
        
        has_keyword = _check_for_keywords(description, keywords)
        #if an id is in the seed it should always be added to the results
        if has_keyword or (id_to_process in id_seeds):
            print ' - Processing %s with priority %s'% (screen_name, priority)
            print ' - Queue lenght: %s' % len(queue)
            print ' - Mined ids: %s' % len(collected_ids)
            sys.stdout.flush()
            #version 2
            accum_non_employee = 0 
            # 4- get new ids to crawl
            repeat = True
            while repeat:
                try:
                    relations = _get_relations(id_to_process)
                    repeat = False
                except tweepy.error.TweepError as e:
                    _save_status(collected_ids, queue, crawled_ids)
                    repeat = True
                    print '(%s) Time limit exceeded. Waiting %s mins' % (time.ctime(), WAIT_MINS)
                    print '\t', e
                    sys.stdout.flush()
                    try:
                        if e.args[0][0]['code'] == 88:
                            time.sleep(WAIT_MINS * 60)
                        else:
                            repeat = False
                    except:
                        repeat = False
            
            collected_ids[id_to_process] = {'screen_name': screen_name, 'friends' : relations['friends'],'followers' : relations['followers']}
            unfiltered_relations = set(relations['friends'] + relations['followers'])
            new_ids = [id_str for id_str in unfiltered_relations if id_str not in crawled_ids]
            
            # 5- increase priority & 6- add new ids to queue     
            for id_str in new_ids:
                if not id_str in queue:
                    queue[id_str] = BASE_PRIORITY
                else:
                    queue[id_str] += 1
        else:
            # version 2
            accum_non_employee += 1
            print ' - Accumuluted non-employees: %s' % accum_non_employee
            sys.stdout.flush()
            
        json.dump(collected_ids, open(NETWORKS_FILEPATH + 'collected_ids.json', 'w'))
                    
    print 'Crawl finished, number of mined ids: %s' % len(collected_ids)
    return collected_ids

# Builds the graph using the ids collected by crawl_organizations   
def build_graph(collected_ids):
    print 'Total collected ids %s' % len(collected_ids)
    G = nx.DiGraph()
    
    #create nodes
    for user in collected_ids:
        G.add_node(user, screen_name = collected_ids[user]['screen_name'])
        
    #create edges   
    for user in collected_ids:    
        friends = collected_ids[user]['friends']        
        for friend in friends:
            if friend in collected_ids:
                G.add_edge(user, friend)
                
        followers = collected_ids[user]['followers']
        for follower in followers:
            if follower in collected_ids:
                G.add_edge(follower, user)                              
    return G

        
if __name__ == '__main__':
    print 'Starting...'
    
    #****************************** SIEMENS ***********************************
    
    account_seeds_siemens = set(['@MichaelStal', '@darko_anicic', '@geri_revay', '@janinakugel', 
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
                     '@GaryAmosSFS' , '@MarkSubramaniam', '@AlainIung', '@ospreybev', 
                     '@AndyDeo', ' @jawalke2', '@JasonNewellNET', '@total_plm', 
                     '@timegloff', '@AdamWesoly', '@evadinkel', '@ccliffor', 
                     '@Siemens_PLM_Ru', '@KrisKasprzak', '@Drei_Racker', '@rhidajat', 
                     '@mikerouman', '@jimphelan', '@AlRobertson', '@dorasmith', 
                     '@johnhfox', '@kesmit3', '@Bewator_Siemens', '@MollyHwa', 
                     '@mike_atkins', '@renezahradnik', '@susanpelley', '@BBoswell1', 
                     '@uk_dave', '@baumvogel', '@jsarfati', '@rajahc', '@martinnoskovic', 
                     '@JasonPeel2', '@annieroberts1', '@janeshart', '@SavvasVerdis', 
                     '@redwind11', '@SoutherlyBreeze', '@stenmic', '@RMFranke', 
                     '@SiemensSensors', '@SiemensAfrica', '@Siemens_DT_US', '@siemensindia', 
                     '@Siemens_CNC_US', '@SiemensHealthIT', '@SiemensStiftung', 
                     '@HeatherMachado', '@SiemensinCanada', '@Stephane_Chayer', 
                     '@Siemens_IA_NL', '@Robert_Hardt', '@caring_hands', '@sfoundation', 
                     '@SiemensHealth', '@SiemensBT', '@siemensindustry', '@TimG_SmartGrid', 
                     '@Siemens_SG', '@Siemens_Ptbo', '@SiemensAnswers', '@SiemensPLM', 
                     '@SiemensUKNews', '@siemens_press', '@USautomation', '@SiemensCities', 
                     '@Siemens', '@SiemensII', '@SiemensUSA', '@Siemens_Energy', 
                     '@SiemensSensors', '@SiemensPLM_JP', '@Siemens_ARG', 
                     '@siemensindia' ,'@VincentThornley', '@DBGeach', '@DarrenGarbett',
                     '@nigekirby', '@emmawhitaker', '@PhilBanksVSD', '@SueBagguley',
                     '@JoHensher', '@Siemens_RailUK', '@Mish_78', '@galling_nigel', 
                     '@Dan_L_Walker', '@MarkGJenkinson', '@silkethomson'])
    
    keywords_siemens = ['siemens']
    
    #****************************** UBISOFT ***********************************
    
    account_seeds_ubisoft = set(['@UbiGabe', '@UbiXander', '@aemond', '@matwillem', 
                                 '@Harpax', '@borbok', '@BDRAnneLewis', '@Mat_TheDuke', 
                                 '@Aleissia', '@Alex_Gnn', '@UbiJustin', '@UbiDii', 
                                 '@WarrenPrice', '@preshing', '@Sabre_FD', '@ChattyDM', 
                                 '@BagelofDeath', '@GarySteinman', '@Daze_FD', 
                                 '@pricefilms', '@TheBigWiazowski', '@danhaynow', 
                                 '@ChaseStraight', '@kkaspar', '@CeeCeePMS', 
                                 '@pascalblanche', '@VballinChick', '@PaoloPace', 
                                 '@jamese', '@BangBangClick', '@jedirobb', '@olliecoe', 
                                 '@N1tch', '@ymyrtil', '@cjnorris', '@jeffsimpsonkh', 
                                 '@Monobrowser', '@sfry2k', '@UbiNox84', '@NinjaSalatin', 
                                 '@UbiMiiSTY', '@Teut', '@patrick_plourde', '@bdljuce', 
                                 '@UbiParadise', '@JFStAmour', '@Jeff_Skalski', 
                                 '@BUK57', '@Danielle_CE', '@HiJayPosey', '@jgerighty', 
                                 '@dbed', '@potemkincityend', '@emilegauthier', 
                                 '@AnnickV', '@MegaMasao', '@e_michon', '@Cristina_garcia', 
                                 '@_SFalker', '@Mat_Stomp', '@colinj_graham', 
                                 '@TokyoLuv', '@kmoctezuma', '@lesley_phordtoy', 
                                 '@pakablog', '@Ubi_Wuigi', '@someUIguy', '@danjohncox', 
                                 '@Alyatirno', '@MechanicalR', '@l0vetemper', 
                                 '@davidtherio', '@SpaceTangent', '@fabianv', 
                                 '@liamwong', '@hholmes79', '@ThomasCurtis', 
                                 '@tynril', '@UbiJeff', '@MWSeverin', '@olafurw', 
                                 '@tobiaskoppen', '@R0U3L', '@BobbyAnguelov', 
                                 '@WBDN', '@mickaelgilabert', '@Effiluna', 
                                 '@YFanise', '@ennstamatic', '@keithoconor', 
                                 '@LucienSoulban', '@jmast', '@AggroFrogg', 
                                 '@Drusticeleague', '@Feloisa', '@PaoloPace', 
                                 '@thuantta', '@X0phe', '@Mat_TheDuke', '@D_M_Gregory', 
                                 '@LaurentMalville', '@Brenda_Puebla', '@nachoyague', 
                                 '@acrocircus', '@GameProjects', '@BillyMatjiunis', 
                                 '@NitaiB', '@RDansky', '@Doodlez22', '@Harpax', 
                                 '@fgrimaldi', '@richienieto', '@jamese', 
                                 '@iCDrums', '@matwillem', '@Fride_rik', '@Doobivoos', 
                                 '@UbiTish', '@UbiTrent', '@FredGotGame'])
    
    keywords_ubisoft = ['ubisoft']
    
    
    #********************************** luxoft *******************************
    
    account_seeds_luxoft = set(['@luxoft', '@TrainingLuxoft', '@Twister_Luxoft', 
                                '@Luxoft_Jobs', '@CakeFromLuxoft', '@Mariya_luxoft', 
                                '@pberendt', '@LuxoftPoland', '@leonid_efremov', 
                                '@dimafirsov', '@ayakima', '@dminkova1', '@Luxoftagency', 
                                '@TesterLuxoft', '@step2zero', '@Dr_P_Watson', 
                                '@ArtemKulyk', '@RIP212'])
    
    keywords_luxoft = ['luxoft']
    
    #****************************** Fiserv ***********************************
    
    account_seeds_fiserv = set(['@Fiserv', '@Fiserv_CU', '@FiservCareers', '@FISVPROUD', 
                                '@mobimelissa', '@jimtobincan', '@wadec2', '@jill_kuhlman', 
                                '@TheRaddonReport', '@OpenSolutionsCC', '@NZAndyParker', 
                                '@FPKohler', '@jaxsully', '@cherylnash2', '@tbo11', 
                                '@PeceJ', '@msieve', '@davidcarrNZ', '@ditispatrick', 
                                '@Fiserv_Amanda', '@FiservMk', '@Ujjwal_Fiserv', 
                                '@TirdadShojaie', '@DC_ClientServs', '@PaulSeamon', 
                                '@tally_hall', '@orthmpdx', '@NZAndyParker', 
                                '@MaryBrutovski', '@rahulgupta999', '@Cash_Logistics', 
                                '@jimtobincan'])
    
    keywords_fiserv = ['fiserv']
    
    
    #****************************** Accenture *********************************
    
    account_seeds_accenture = set(['@Accenture', '@AccentureSocial', '@ISpeakAnalytics', 
                                   '@AccentureIndia', '@Accenture_Jobs', '@AccentureHiTech', 
                                   '@MobilityWise', '@AccentureRetail', '@AccentureCloud', 
                                   '@AccentureDigi', '@accenturelabs', '@AccentureMedia', 
                                   '@AccentureComms', '@BankingInsights', '@AccentureFed', 
                                   '@AccentureAero', '@accenture_ca', '@accentureukjobs', 
                                   '@AccentureTech', '@AccentureStrat', '@AccentureHealth', 
                                   '@AccenturePubSvc', '@AccentureRugby', '@AccentureFrance', 
                                   '@prithbanerjee', '@mjbiltz', '@justinkeeble',
                                   '@JillHuntley1', '@jessicalongdev', '@arthurhanna', 
                                   '@AccentureJobsCr', '@SRaycroft', '@AccentureEnergy', 
                                   '@AccentureJobsCl', '@AccentureASEAN', '@Accenture_MX', 
                                   '@AccentureSAjobs', '@Accenture_DK', '@accenture_br', 
                                   '@AccentureDACH', '@Sandervtn', '@pauldaugh', 
                                   '@AccentureJobsAR', '@fjord', '@AccenturePubSvc', '@AccentureGrid', 
                                   '@Accenture_ID', '@AccentureDigi', '@AccentureSpain', 
                                   '@Accenture_TR', '@GarethDJWilson', '@GillyBryant', 
                                   '@michaelcostonis', '@AccentureStrat', '@AccentureCapMkt', 
                                   '@jill_inglis', '@jortpossel', '@markpmcdonald', 
                                   '@Hartmanglen', '@AccentureCloud', '@MikeSutcliff', 
                                   '@apabbatielloHR'])
    
    keywords_accenture  = ['accenture']
    
#    keywords = set(['Siemens', 'siemens', 'SiemensPLM', 'Siemens_Ptbo', 'SiemensPLM_BNL',
#                'SiemensHealthIT', 'Siemens_Traffic', 'SiemensPLM_DE', 'SiemensPLM_JP', 
#                'Siemens_Ptbo', 'Siemens_DT_US', 'SiemensMobility', 'RollingOnRails', 
#                'SiemensStiftung', 'siemens_salud', 'SiemensII', 'SiemensCampus', 
#                'Siemens_SFS', 'siemens_es', 'SiemensHealth', 'SiemensUSA', 'Siemens_Suomi', 
#                'Siemens_Italia', 'Siemens_Sverige', 'Siemens_Russia', 'Siemens_stampa', 
#                'SiemensinCanada', 'siemens_press', 'Siemens_ARG', 'Siemens_Austria', 
#                'siemensindustry', 'SiemensUKNews', 'SiemensTurkiye', 'Siemens_Brasil'])
                
#    print 'Recovering ids...'
#    id_seeds = get_ids_from_screen_names(account_sdeeds)
#    print 'Saving seed ids...'
#    json.dump(list(id_seeds), open('seed_ids_siemens.json','w'), indent=2)

    account_seeds = account_seeds_accenture
    keywords = keywords_accenture
        
    

    
    SEED_ID_FILE = 'seed_ids.json'
    try:
        print 'Loading ids...'
        id_seeds = json.load(open(SEED_ID_FILE,'r'))
    except:
        print 'Recovering ids...'
        id_seeds = get_ids_from_screen_names(account_seeds)
        print 'Saving seed ids...'
        json.dump(list(id_seeds), open(SEED_ID_FILE,'w'), indent=2)
        
    try:
        collected_ids, queue, crawled_ids = _load_status()
    except:
        collected_ids = {}
        # 0- Initialize
        # queue of ids to be processed
        queue = _initialize_queue(id_seeds)
        # ids already processed
        crawled_ids = []
        
    
    print 'Crawling twitter...'
    collected_ids = crawl_organization(id_seeds, keywords, collected_ids, queue, crawled_ids)
    print 'Building graph...'
    G = build_graph(collected_ids)
    print 'Saving graph...'
    nx.write_gexf(G, NETWORKS_FILEPATH + 'twitter.gexf')
    
