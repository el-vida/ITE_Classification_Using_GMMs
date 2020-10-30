import pandas as pd
import numpy as np
import math
import Levenshtein

from tqdm import tqdm 
from multiprocessing import Process

def infer_ite_no_swype(log_process, process_id, OUTPUT_PATH):
    log_process = log_process.copy()

    thresholds_autocorr = [273,280,396,281,295,293,297,282,276,269,255,245,227,239,225,228,208,205,200,200]
    thresholds_predict = [396,367,593,442,417,395,397,376,363,367,331,316,291,285,266,266,245,225,300,300]

    log_index = [0 for index in range(log_process.shape[0])]

    for entry in range(log_process.shape[0]):
        log_index[entry] = log_process.index[entry]

    for row in range(len(log_index)):
        if(row + 1 < len(log_index) and row > 2):
            if log_process.loc[log_index[row], 'ite'] == 'none' and log_process.loc[log_index[row+1],'key'] == ' ' and log_process.loc[log_index[row],'user_type'] >= 15: 
                user_performance = log_process.loc[log_index[row], 'user_type']
                threshold_a = thresholds_autocorr[int(user_performance/5)-1]
                threshold_p = thresholds_predict[int(user_performance/5)-1]
                # 1. Infer Prediction
                if len(str(log_process.loc[log_index[row], 'key'])) > 1 and log_process.loc[log_index[row],'lev_dist'] >= 0 and log_process.loc[log_index[row],'iki'] >= threshold_p :
                    log_process.loc[log_index[row],'ite'] = 'predict' 
                    continue
                # 2. Infer Autocorrection - TO DO : Maybe cut out entry id
                if len(str(log_process.loc[log_index[row],'key'])) > 1 and log_process.loc[log_index[row],'entry_id'] == log_process.loc[log_index[row-1],'entry_id'] and log_process.loc[log_index[row],'lev_dist'] > 0 and log_process.loc[log_index[row],'iki'] <= threshold_a :
                    log_process.loc[log_index[row],'ite'] = 'autocorr'  
                    continue
                # Implement case-sensitive ITEs
                if(log_process.loc[log_process.index[entry],'lev_dist'] == 0):
                    #if(len(str(log_process.loc[log_process.index[entry],'text_field'])) == len(str(log_process.loc[log_process.index[entry],'text_field_prev']))):
                    if str(log_process.loc[log_process.index[entry],'text_field']).lower() == str(log_process.loc[log_process.index[entry],'text_field_prev']).lower():
                        if str(log_process.loc[log_process.index[entry],'text_field']) != str(log_process.loc[log_process.index[entry],'text_field_prev']):
                            log_process.loc[log_process.index[entry],'ite'] = infer_ite_no_swype_case_sensitive(log_process, log_process.index[entry])
    
    # Reset negative entries
    log_process.loc[log_process.entry_id < 0,'ite'] = 'none'

    # Infer swype
    log_process = infer_ite_swype(log_process)

    # save files
    # print('Saving files...')
    log_process.to_csv(OUTPUT_PATH + str(process_id) + '.csv', index=False)

    return log_process

def infer_ite_no_swype_case_sensitive(log_process, row):
    thresholds_autocorr = [273,280,396,281,295,293,297,282,276,269,255,245,227,239,225,228,208,205,200,200]
    thresholds_predict = [396,367,593,442,417,395,397,376,363,367,331,316,291,285,266,266,245,225,300,300]
    if row + 1 < log_process.shape[0]:
        if log_process.loc[row, 'ite'] == 'none' and log_process.loc[row+1,'key'] == ' ' and log_process.loc[row,'user_type'] >= 15: 
            user_performance = log_process.loc[row, 'user_type']
            threshold_a = thresholds_autocorr[int(user_performance/5)-1]
            threshold_p = thresholds_predict[int(user_performance/5)-1]
            # 1. Infer Prediction
            if len(str(log_process.loc[row, 'key'])) > 1 and log_process.loc[row,'iki'] >= threshold_p :
                return 'predict' 
            # 2. Infer Autocorrection
            if len(str(log_process.loc[row, 'key'])) > 1 and log_process.loc[row,'iki'] <= threshold_a :
                return 'autocorr'                
            else:
                return 'none'

def infer_ite_swype(log):
    log = log.copy()
    
    log['iki_norm'] = log.iki / log.key.str.len()

    # 1. Infer swype

    ## Case 1: Has leading spaces AND multiple characters
    mask = (log.key.str[0] == ' ') & (log.key.str.len() > 2)
    log.loc[mask,'ite'] = 'swype'

    ## Case 2: The first action of an entry is multicharacter (excl. spaces) AND there's multiple actions
    index_first = log.groupby(['ts_id','entry_id']).head(1).index
    mask = (log.index.isin(index_first)) & (log.key.str.strip(' ').str.len() > 1)
    mask &= (log.entry_id == log.entry_id.shift(-1))
    log.loc[mask,'ite'] = 'swype'

    ## Case 3: The first action of the very first entry has multiple characters (excluding spaces)
    index_first = log.groupby(['ts_id']).head(1).index
    mask = (log.index.isin(index_first)) & (log.key.str.strip(' ').str.len() > 1)
    log.loc[mask,'ite'] = 'swype'

    ## Case 3: The first action of a new word has multiple characters (excluding spaces) AND it's slow
    #mask = log.text_field.shift(1).str[-1] == ' '
    #index_first = log.loc[mask].groupby(['ts_id','entry_id']).head(1).index
    # = (log.index.isin(index_first)) & (log.key.str.strip(' ').str.len() > 1) & (log.iki_norm > 150)
    #log.loc[mask,'ite'] = 'swype'

    ## Case 4: The first action of a new word has multiple characters (excluding spaces) AND it's long
    mask = log.text_field.shift(1).str[-1] == ' '
    index_first = log.loc[mask].groupby(['ts_id','entry_id']).head(1).index
    mask = (log.index.isin(index_first)) & (log.key.str.strip(' ').str.len() > 1)
    mask &= (log.key.str.len() > 5)
    log.loc[mask,'ite'] = 'swype'

    ## Case 5: Fill in the same entry as a swype
    #log.set_index(['ts_id','entry_id'],inplace=True)
    #log.loc[log.loc[log.ite == 'swype'].index,'ite'] = 'swype'
    #log.reset_index(inplace=True)

    # TODO Case 3 could also mean a prediction
    # TODO what about swype followed by a prediction correction?
    # TODO what about backspace followed by a prediction?

    return log

def recalculate_levenshtein_distance(log):
    log = log.copy()

    log['new_lev_dist'] = -7

    for row in range(log.shape[0]):
        ## Beginning of new sentences where text_field_prev contains sentence from previous row 
        if (row > 0) and (log.loc[row, 'ts_id'] != log.loc[row-1, 'ts_id']) and (log.loc[row-1, 'text_field'] == log.loc[row, 'text_field_prev']):
            log.loc[row, 'text_field_prev'] = ' '
            #log.loc[row, 'new_lev_dist'] = stringdist.levenshtein(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
            log.loc[row, 'new_lev_dist'] = Levenshtein.distance(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
            log.loc[row, 'text_field_prev'] = ''
            continue
        
        ## Beginning of new sentences with other sentences than the one from the previous row 
        ## (could be due to removal of certain invalid sentences which contain cryptic letters)
        if (len(str(log.loc[row,'text_field'])) == 1) and (str(log.loc[row,'text_field_prev']) == ' ') and (len(str(log.loc[row,'key'])) == 1) and (log.loc[row,'key'] != ' '):
            log.loc[row, 'text_field_prev'] = ' '
            #log.loc[row, 'new_lev_dist'] = stringdist.levenshtein(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
            log.loc[row, 'new_lev_dist'] = Levenshtein.distance(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
            log.loc[row,'text_field_prev'] = ''
            continue
        
        ## Use of backspace until the whole sentence is erased led to false new_lev_dist calculations 
        ## because of nan value of empty text_field_prev or text_field  
        ## We solve this by temporarily changing the empty field ('') to a space (' '), recalculate the lev_dist and then emptying the field again 
        if (len(str(log.loc[row, 'text_field_prev'])) == 1) and (log.loc[row,'key'] == '_'):
            log.loc[row, 'text_field'] = ' '
            #log.loc[row, 'new_lev_dist'] = stringdist.levenshtein(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
            log.loc[row, 'new_lev_dist'] = Levenshtein.distance(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
            log.loc[row, 'text_field'] = '' 
            continue
        if (row > 0) and (log.loc[row,'text_field'] == log.loc[row, 'key']) and (log.loc[row-1,'key'] == '_'):
            log.loc[row, 'text_field_prev'] = ' '
            #log.loc[row, 'new_lev_dist'] = stringdist.levenshtein(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
            log.loc[row, 'new_lev_dist'] = Levenshtein.distance(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
            log.loc[row, 'text_field_prev'] = '' 
            continue
        #log.loc[row, 'new_lev_dist'] = stringdist.levenshtein(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))
        log.loc[row, 'new_lev_dist'] = Levenshtein.distance(str(log.loc[row, 'text_field']), str(log.loc[row, 'text_field_prev']))

    return log

def Run_E_1(LOG_ID):
    print(' ----- E1 -----------------------------------------------------------\n')
    global INPUT_PATH
    global OUTPUT_PATH 

    INPUT_PATH  = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_new_levenshtein.csv'
    OUTPUT_PATH = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\Corrected\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_new_levenshtein_corrected_after_process_'

    
    # print('Reading files...')
    log_comb_ite_with_participants = pd.read_csv(INPUT_PATH)

    # Correct ites
    # Add multi processing
    # print('Running processes...')
    processes = []
    logs = {}

    # create processes
    num_processes = 40
    step = math.ceil(log_comb_ite_with_participants.shape[0]/ num_processes )
    for i in range(num_processes):
        start = i * step
        if(i + 1 != num_processes):
            end = start + step
        else:
            end = log_comb_ite_with_participants.shape[0]
        logs[i] = pd.DataFrame(log_comb_ite_with_participants[start:end], columns=log_comb_ite_with_participants.columns)
        p = Process(target=infer_ite_no_swype, args=[logs[i], i, OUTPUT_PATH])
        processes.append(p)
    # start processes
    for p in tqdm(processes):
        p.start()
    # let processes join
    for p in processes:
        p.join()

    print('\nE1 DONE\n')