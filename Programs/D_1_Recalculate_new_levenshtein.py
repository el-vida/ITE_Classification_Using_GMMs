import pandas as pd
import numpy as np
import math
import Levenshtein

from tqdm import tqdm 
from multiprocessing import Process

def recalculate_levenshtein_distance(log_process, process_id, OUTPUT_PATH):
    log_process['new_lev_dist'] = -7

    for row in range(log_process.shape[0]):
        ## Very first row
        if (row == 0) and (len(str(log_process.loc[log_process.index[row], 'text_field_prev'])) > 5) and (log_process.loc[log_process.index[row], 'text_field'] == log_process.loc[log_process.index[row], 'key']):
            log_process.loc[log_process.index[row], 'text_field_prev'] = ' '
            log_process.loc[log_process.index[row], 'new_lev_dist'] = Levenshtein.distance(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'text_field_prev'] = ''
            continue
        ## Beginning of new sentences where text_field_prev contains sentence from previous row 
        if (row > 0) and (log_process.loc[log_process.index[row], 'ts_id'] != log_process.loc[log_process.index[row-1], 'ts_id']) and (log_process.loc[log_process.index[row-1], 'text_field'] == log_process.loc[log_process.index[row], 'text_field_prev']):
            log_process.loc[log_process.index[row], 'text_field_prev'] = ' '
            #log_process.loc[log_process.index[row], 'new_lev_dist'] = stringdist.levenshtein(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'new_lev_dist'] = Levenshtein.distance(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'text_field_prev'] = ''
            continue
        ## Beginning of new sentences where text_field contains one letter and text_field_prev contains a whole sentence, also ts_ids are different between row and row-1
        if (row > 0) and (log_process.loc[log_process.index[row], 'ts_id'] != log_process.loc[log_process.index[row-1], 'ts_id']) and (len(str(log_process.loc[log_process.index[row], 'text_field'])) == 1) and (len(str(log_process.loc[log_process.index[row], 'text_field_prev'])) > 5):
            log_process.loc[log_process.index[row], 'text_field_prev'] = ' '
            #log_process.loc[log_process.index[row], 'new_lev_dist'] = stringdist.levenshtein(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'new_lev_dist'] = Levenshtein.distance(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'text_field_prev'] = ''
            continue
        ## Beginning of new sentences where text_field contains one letter and text_field_prev contains a whole sentence, also ts_ids are different between row and row-1 and text_field is identical to key
        if (row > 0) and (log_process.loc[log_process.index[row], 'ts_id'] != log_process.loc[log_process.index[row-1], 'ts_id']) and (str(log_process.loc[log_process.index[row], 'text_field']) == str(log_process.loc[log_process.index[row], 'key'])) and (len(str(log_process.loc[log_process.index[row], 'text_field_prev'])) > 5):
            log_process.loc[log_process.index[row], 'text_field_prev'] = ' '
            #log_process.loc[log_process.index[row], 'new_lev_dist'] = stringdist.levenshtein(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'new_lev_dist'] = Levenshtein.distance(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'text_field_prev'] = ''
            continue
        
        ## Beginning of new sentences with other sentences than the one from the previous row 
        ## (could be due to removal of certain invalid sentences which contain cryptic letters)
        if (len(str(log_process.loc[log_process.index[row],'text_field'])) == 1) and (str(log_process.loc[log_process.index[row],'text_field_prev']) == ' ') and (len(str(log_process.loc[log_process.index[row],'key'])) == 1) and (log_process.loc[log_process.index[row],'key'] != ' '):
            log_process.loc[log_process.index[row], 'text_field_prev'] = ' '
            #log_process.loc[log_process.index[row], 'new_lev_dist'] = stringdist.levenshtein(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'new_lev_dist'] = Levenshtein.distance(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row],'text_field_prev'] = ''
            continue
        
        ## Use of backspace until the whole sentence is erased led to false new_lev_dist calculations 
        ## because of nan value of empty text_field_prev or text_field  
        ## We solve this by temporarily changing the empty field ('') to a space (' '), recalculate the lev_dist and then emptying the field again 
        if (len(str(log_process.loc[log_process.index[row], 'text_field_prev'])) == 1) and (log_process.loc[log_process.index[row],'key'] == '_'):
            log_process.loc[log_process.index[row], 'text_field'] = ' '
            #log_process.loc[log_process.index[row], 'new_lev_dist'] = stringdist.levenshtein(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'new_lev_dist'] = Levenshtein.distance(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'text_field'] = '' 
            continue
        if (row > 0) and (log_process.loc[log_process.index[row],'text_field'] == log_process.loc[log_process.index[row], 'key']) and (log_process.loc[log_process.index[row-1],'key'] == '_'):
            log_process.loc[log_process.index[row], 'text_field_prev'] = ' '
            #log_process.loc[log_process.index[row], 'new_lev_dist'] = stringdist.levenshtein(str(log.loc[log_process.index[row], 'text_field']), str(log.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'new_lev_dist'] = Levenshtein.distance(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
            log_process.loc[log_process.index[row], 'text_field_prev'] = '' 
            continue
        #log_process.loc[log_process.index[row], 'new_lev_dist'] = stringdist.levenshtein(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))
        log_process.loc[log_process.index[row], 'new_lev_dist'] = Levenshtein.distance(str(log_process.loc[log_process.index[row], 'text_field']), str(log_process.loc[log_process.index[row], 'text_field_prev']))

    # Save files
    # print('Saving files...')
    log_process.to_csv(OUTPUT_PATH + str(process_id) + '.csv', index=False)



def Run_D_1(LOG_ID):
    print(' ----- D1 -----------------------------------------------------------\n')
    global INPUT_PATH
    global OUTPUT_PATH 

    INPUT_PATH  = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted.csv'
    OUTPUT_PATH = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\New_Levenshtein\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_new_levenshtein_after_process_'

    # print('Reading files...')
    log_comb_ite_with_participants = pd.read_csv(INPUT_PATH)

    # # Recalculate levenshtein distance

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
        p = Process(target=recalculate_levenshtein_distance, args=[logs[i], i, OUTPUT_PATH])
        processes.append(p)
    # start processes
    for p in tqdm(processes):
        p.start()
    # let processes join
    for p in processes:
        p.join()

    print('\nD1 DONE\n')