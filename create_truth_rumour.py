#JANANI KALYANAM
# CREATED: May 23, 2015.
# jkalyana@ucsd.edu

import sqlite3
import json, gzip
import os, sys
import imp

if __name__ == '__main__':
    
    connection_10_3 = sqlite3.connect('ebola_current.db');
    #connection_10_4 = sqlite3.connect('ebola_10_4.db');
    #connection_10_6 = sqlite3.connect('ebola_10_6.db');

    count_common = 0;

    twokenize = imp.load_source('twokenize','/mnt/spare/users/janani/Research/TweetNLP/ark-twokenize-py/twokenize.py');
    

    features = list(map(lambda x: x.strip(), open('../for_stats_test.txt','r').readlines()));
    len_features = len(features);
    features = ','.join(features);  # features separated by comma
    features = features + ',entities_hashtags_text';
    execute_string = 'SELECT ' + features + ' FROM tweet_attributes where entities_hashtags > 0';
    fid_rumour =     open('../rumour.txt','w');
    fid_truth =     open('../truth.txt','w');
    non_serious = list(map(lambda x: x.strip(), open('../non_serious_hashtags.txt','r').readlines()));
    non_serious = set(map(lambda x: x.lower(), non_serious));
    serious = list(map(lambda x: x.strip(), open('../serious_hashtags.txt','r').readlines()));
    serious = set(map(lambda x: x.lower(), serious));

    cursor = connection_10_3.cursor(); 
    R = cursor.execute(execute_string);
    print(execute_string)
    ii = 1;

    ## Create truth and rumour files for serious and non-serious hashtags
    common_hashtag_tweets_count = 0;
    for r in R:
        print(ii)
        ii += 1;
        #if(ii > 100000):
        #    break;
        # r[-1] is a string of hashtags separated by comma
        hashtags_r = list(r[-1].split(','));
        hashtags_r = set(map(lambda x: x.lower(), hashtags_r));

        common_serious = hashtags_r.intersection(serious);
        common_non_serious = hashtags_r.intersection(non_serious);
    
        # skip if the hashtags are in both serious and non-serious sets, or if they are in neither.
        if(((len(common_serious) > 0) and (len(common_non_serious) > 0))):
            common_hashtag_tweets_count += 1;

        if(((len(common_serious) <= 0) and (len(common_non_serious) <= 0))):
            continue;

        tweet_text = list(map(lambda x: x.lower(), twokenize.tokenizeRawTweetText(str(r[-2]))));
         
        myStr = '';
        for i in range(len(r)-2): # r[-1] = list of hashtags, r[-2] = text of tweet.
            if(r[i] == 'False' or r[i] == None):
                myStr = myStr + '0';
            elif(r[i] == 'True' or (i == 5 and r[i] != None)): # i = 5 corresponds to in_reply_to_status_id.
                myStr = myStr + '1';
            else:
                myStr = myStr + str(r[i]);

            

            if(i == len(r) - 3): # r[-1] = list of hashtags, r[-2] = text of tweets.
                myStr = myStr + '\n';
            else:  
                myStr = myStr + ' ';

        if(len(common_serious) > 0):
            fid_truth.write(myStr);
        elif(len(common_non_serious) > 0):
            fid_rumour.write(myStr);
        else:  
            continue;
