# JANANI KALYANAM
# CREATED: March 31st, 2015
# jkalyana@ucsd.edu

import sqlite3
import os,sys
import json, gzip
import unicodedata

globCount = 0;
globFileName = '';

#def myCode():
if __name__ == '__main__':
    connection = sqlite3.connect('ebola.db');
    myCursor = connection.cursor();
    lines = open('../column_names_new.txt','r').readlines();
    myCols = map(lambda x: x.strip().split(';')[0], lines);
    first_time = 1;
    dir_name = '../data/';
    file_names = ['Ebola.2014-10-06-1.gz','Ebola.2014-10-04-073901.gz','Ebola.2014-10-03-233901'];
    #file_names = ['Ebola.2014-10-03-233901'];
    col_tuple = ();
    for each_col in myCols:
        if(each_col.find(':') == -1):
            col_tuple = col_tuple + (each_col,);
        else:
            col_tuple = col_tuple + ('_'.join(each_col.split(':')),);


    col_retweeted_status = ['retweet_count','favorite_count','in_reply_to_status_id','possibly_sensitive','truncated','entities:urls'];
    col_retweeted_status = col_retweeted_status + ['entities:symbols','entities:hashtags','entities:hashtags:text'];




    execute_string_create_table = 'CREATE TABLE tweet_attributes ' + str(tuple(col_tuple));
    myCursor.execute(execute_string_create_table);
    connection.commit();

    execute_string_insert_row = 'INSERT INTO tweet_attributes VALUES (';
    for ii in range(len(myCols)):
        if(ii == len(myCols) - 1):
            execute_string_insert_row = execute_string_insert_row + '?)';
        else:
            execute_string_insert_row = execute_string_insert_row + '?,';


	
    for each_file1 in file_names:
        each_file = dir_name + each_file1;
        globFileName = each_file;
        if(each_file.find('.gz') == -1):
            fid = open(each_file,'r');
        else:
            fid = gzip.open(each_file,'r');
        ## Create table in the database
                
        globCount = 0;
        while(1):
            l = fid.readline();
            if(l == ''):
                break;
            if(type(l) != str):
                l = str(l.decode('utf-8'));
            #for l in lines_ebola:
            globCount = globCount + 1;
            print(globFileName);
            print(globCount);
            ## Check if the tweet is a JSON OBJECT
            try:	
                json_object = json.loads(l);
            except ValueError:
                continue;
            ## See if the text is not garbled.
            try:
                myStr = str(json_object['text']);
            except:
                continue;
            col_ii = 0;
            values = (); 

            retweeted_status = 0;
            while(1):
                if(col_ii == len(myCols)):
                    break;
                col = myCols[col_ii];
                if(col == 'coordinates'):
                    if json_object[col] is not None:
                        coordinates = str(json_object[col]['coordinates'][0])+';'+str(json_object[col]['coordinates'][1]);
                        values = values + (coordinates,);
                        col_ii = col_ii + 1;
                    else:
                        values = values + (json_object[col],);
                        col_ii = col_ii + 1;
                elif(col == 'retweeted_status'):
                    if('retweeted_status' in json_object.keys()):
                        retweeted_status = 1;
                        values = values + (1,);
                    else:
                        values = values + (0,);
                    col_ii = col_ii + 1;
                elif(col.find(':') == -1):
                    if((col in col_retweeted_status) and (retweeted_status == 1)):
                        if(type(json_object['retweeted_status'][col]) == str):
                            values = values + (str(unicodedata.normalize('NFKD',unicode(json_object['retweeted_status'][col].encode('ascii','ignore')))),);
                        else:
                            values = values + (json_object['retweeted_status'][col],);
                    else:
                        if(type(json_object[col]) == str):
                            values = values + (str(unicodedata.normalize('NFKD',unicode(json_object[col].encode('ascii','ignore')))),);
                        else:
                            values = values + (json_object[col],); 
                    col_ii = col_ii + 1;
                else:
                    if((col in col_retweeted_status) and retweeted_status == 1):
                        col_split = col.split(':');
                        if(col_split[0] == 'entities'):
                            values = values + ((len(json_object['retweeted_status']['entities']['urls'])),);
                            values = values + ((len(json_object['retweeted_status']['entities']['symbols'])),);
                            values = values + ((len(json_object['retweeted_status']['entities']['hashtags'])),);
                            all_hashtags = '';
                            if(len(json_object['retweeted_status']['entities']['hashtags']) > 0):
                                for ii in range(len(json_object['retweeted_status']['entities']['hashtags'])):
                                    S = unicodedata.normalize('NFKD', unicode(json_object['retweeted_status']['entities']['hashtags'][ii]['text'].encode('ascii','ignore')));
                                    S = str(S);
                                    all_hashtags = all_hashtags + S;
                                    if(ii != len(json_object['retweeted_status']['entities']['hashtags'])-1):
                                        all_hashtags = all_hashtags + ',';
                            values = values + (all_hashtags,);
                            col_ii = col_ii + 4;
                        if(col_split[0] == 'user'):
                            S = json_object['retweeted_status'][col_split[0]][col_split[1]];
                            if(type(json_object['retweeted_status'][col_split[0]][col_split[1]]) != int):
                                S = unicodedata.normalize('NFKD', unicode(json_object['retweeted_status'][col_split[0]][col_split[1]])).encode('ascii','ignore');
                            values = values + (S,);
                            col_ii = col_ii + 1;
                    else:
                        col_split = col.split(':');
                        if(col_split[0] == 'entities'):
                            values = values + ((len(json_object['entities']['urls'])),);
                            values = values + ((len(json_object['entities']['symbols'])),);
                            values = values + ((len(json_object['entities']['hashtags'])),);
                            all_hashtags = '';
                            if(len(json_object['entities']['hashtags']) > 0):   
                                for ii in range(len(json_object['entities']['hashtags'])):
                                    S = unicodedata.normalize('NFKD', unicode(json_object['entities']['hashtags'][ii]['text'].encode('ascii','ignore')));
                                    S = str(S);
                                    all_hashtags = all_hashtags + S;
                                    if(ii != len(json_object['entities']['hashtags'])-1):
                                        all_hashtags = all_hashtags + ',';
                            values = values + (all_hashtags,);
                            col_ii = col_ii + 4;
                        if(col_split[0] == 'user'):
                            S = json_object[col_split[0]][col_split[1]];
                            if(type(json_object[col_split[0]][col_split[1]]) != int):
                                S = unicodedata.normalize('NFKD', unicode(json_object[col_split[0]][col_split[1]])).encode('ascii','ignore');
                            values = values + (S,);
                            col_ii = col_ii + 1;

            try:
                myCursor.execute(execute_string_insert_row,values);
            except sqlite3.OperationalError, msg:
                print(msg);
                print('\n\n');
            connection.commit();
        connection.close(); 




#if __name__ == '__main__':
#    try:
#        myCode();
#        print('no errors');
#    except:
#        print(globFileName+' ' + str(globCount));
           
    
    
