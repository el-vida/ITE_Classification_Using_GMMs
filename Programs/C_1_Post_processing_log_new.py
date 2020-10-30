import pandas as pd
import numpy as np
import ftfy
import math
from tqdm import tqdm
from multiprocessing import Process

def infer_ite_no_swype_case_sensitive(log_process, row):
    log_process = log_process.copy()

    thresholds_autocorr = [273,280,396,281,295,293,297,282,276,269,255,245,227,239,225,228,208,205,200,200]
    thresholds_predict = [396,367,593,442,417,395,397,376,363,367,331,316,291,285,266,266,245,225,300,300]
    
    if row+1 < log_process.index[log_process.shape[0]-1]:
        if log_process.loc[row, 'ite'] == 'none' and log_process.loc[row+1,'key'] == ' ' and log_process.loc[row,'user_type'] >= 15: 
            user_performance = log_process.loc[row, 'user_type']
            threshold_a = thresholds_autocorr[int(user_performance/5)-1]
            threshold_p = thresholds_predict[int(user_performance/5)-1]
            # 1. Infer Prediction
            if len(log_process.loc[row, 'key']) > 1 and log_process.loc[row,'lev_dist'] >= 0 and log_process.loc[row,'iki'] >= threshold_p :
                log_process.loc[row,'ite'] = 'predict' 
            # 2. Infer Autocorrection
            if len(log_process.loc[row, 'key']) > 1 and log_process.loc[row,'iki'] <= threshold_a :
                log_process.loc[row,'ite'] = 'autocorr'      

    # Reset negative entries
    #log_process.loc[log_process.entry_id < 0,'ite'] = 'none'

    return log_process

def Apply_post_processing_steps(log_process, process_id, OUTPUT_PATH):
    ts_ids_cryptic = [0 for index in range(100)]
    count = 0
    for entry in tqdm(range(log_process.text_field.shape[0])):
        # Edit encoding errors
        log_process.loc[log_process.index[entry],'text_field_prev'] = ftfy.fix_text(str(log_process.loc[log_process.index[entry],'text_field_prev']))
        log_process.loc[log_process.index[entry],'text_field_prev'] = ftfy.fix_encoding(str(log_process.loc[log_process.index[entry],'text_field_prev']))
        log_process.loc[log_process.index[entry],'text_field'] = ftfy.fix_text(str(log_process.loc[log_process.index[entry],'text_field']))
        log_process.loc[log_process.index[entry],'text_field'] = ftfy.fix_encoding(str(log_process.loc[log_process.index[entry],'text_field']))
        log_process.loc[log_process.index[entry],'key'] = ftfy.fix_text(str(log_process.loc[log_process.index[entry],'key']))
        log_process.loc[log_process.index[entry],'key'] = ftfy.fix_encoding(str(log_process.loc[log_process.index[entry],'key']))

        # Remember ts_ids containing 'â€'
        if(log_process.loc[log_process.index[entry],'key'] == 'â€') or (log_process.loc[log_process.index[entry],'key'] == 'ĂŸ') or (log_process.loc[log_process.index[entry],'key'] == 'â€\uf19d') or (log_process.loc[log_process.index[entry],'key'] == 'Ă—'):
            ts_ids_cryptic[count] = log_process.loc[log_process.index[entry],'ts_id']
            count = count + 1

        # Implement case-sensitive autocorrections
        if(log_process.loc[log_process.index[entry],'ite'] == 'none') and (log_process.loc[log_process.index[entry],'lev_dist'] == 0):
            if(len(str(log_process.loc[log_process.index[entry],'text_field'])) == len(str(log_process.loc[log_process.index[entry],'text_field_prev']))):
                if str(log_process.loc[log_process.index[entry],'text_field']).lower() == str(log_process.loc[log_process.index[entry],'text_field_prev']).lower():
                    if str(log_process.loc[log_process.index[entry],'text_field']) != str(log_process.loc[log_process.index[entry],'text_field_prev']):
                        infer_ite_no_swype_case_sensitive(log_process, log_process.index[entry])

    # Mark sentences containing 'â€','ĂŸ' or 'â€\uf19d' as invalid
    for ts_id in range(len(ts_ids_cryptic)):
        for row in range(log_process.shape[0]):
            if log_process.loc[log_process.index[row],'ts_id'] == ts_ids_cryptic[ts_id]:
                log_process.loc[log_process.index[row],'ts_id'] = np.nan
    
    # Drop all invalid ts_id's 
    log_process = log_process.loc[log_process.ts_id != np.nan]

    # Reset index
    log_process = log_process.reset_index(drop=True)

    # Drop unnecessary columns
    log_process = log_process.drop(['tmp','ite2','p_wpm'],axis=1)

    # Save files
    # print('Saving files...')
    log_process.to_csv(OUTPUT_PATH + str(process_id) + '.csv', index=False)



def Run_C_1(LOG_ID):
    print(' ----- C1 -----------------------------------------------------------\n')
    global INPUT_PATH
    global OUTPUT_PATH 
    
    INPUT_PATH  = r'~\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_processed_new_with_participants_and_user_groups.csv'
    OUTPUT_PATH = r'~\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\Post_Processing_Decryption\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_after_process_'

    
    # print('Reading files...')
    log_comb_ite_with_participants = pd.read_csv(INPUT_PATH)

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
        p = Process(target=Apply_post_processing_steps, args=[logs[i], i, OUTPUT_PATH])
        processes.append(p)
    # start processes
    for p in processes:
        p.start()
    # let processes join
    for p in processes:
        p.join()
    
    print('\nC1 DONE\n')
