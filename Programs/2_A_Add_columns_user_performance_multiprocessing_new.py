import pandas as pd
import numpy as np
import math
from tqdm import tqdm
from multiprocessing import Process

LOG_ID = 3

INPUT_PATH  = r'~\\Desktop\\VB_Schnittstelle\\Dataset\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_processed_new_with_participants.csv'
OUTPUT_PATH = r'~\\Desktop\\VB_Schnittstelle\\Dataset\\Log' + str(LOG_ID) + '\\User_Performance\\log_' + str(LOG_ID) + '_valid_processed_new_with_user_groups_after_process_'

def switch_5(argument):
    switcher = {
        1: "5",
        2: "10",
        3: "15",
        4: "20",
        5: "25",
        6: "30",
        7: "35",
        8: "40",
        9: "45",
        10: "50",
        11: "55",
        12: "60",
        13: "65",
        14: "70",
        15: "75",
        16: "80",
        17: "85",
        18: "90",
        19: "95",
        20: "99"
    }
    return switcher.get(argument, "0")

def Prepare_datasets(log_comb_ite_with_participants_process, process_id):
    # set iki = iki_prev for cases of ite occurrence in order to visualize the graphs correctly
    #for row in range(log_comb_ite_with_participants_process.shape[0]):
    #    if(log_comb_ite_with_participants_process.iloc[row]['ite_prev'] != 'none'):
    #        log_comb_ite_with_participants_process.loc[log_comb_ite_with_participants_process.index[row],'iki'] = log_comb_ite_with_participants_process.iloc[row]['iki_prev']

    #log_combined_ite_1 = pd.read_csv('log_processed_autocorr_and_predict_combined.csv')
    #log_combined_ite = log_combined_ite_1.loc[log_combined_ite_1['ite'] == 'autocorr_or_predict']

    # Add user group column to dataset
    log_comb_ite_with_participants_process['user_type'] = 0

    for row in range(log_comb_ite_with_participants_process.shape[0]):
        #user_performance_10 = round(log_comb_ite_with_participants.loc[log_comb_ite_with_participants.index[row], 'p_wpm'] / 10)
        #user_performance_5 = round(log_comb_ite_with_participants.loc[log_comb_ite_with_participants.index[row], 'p_wpm'] / 5)
        log_comb_ite_with_participants_process.loc[log_comb_ite_with_participants_process.index[row],'user_type'] = switch_5(round(log_comb_ite_with_participants_process.iloc[row]['p_wpm'] / 5))

    #log_comb_ite_with_participants_spaces = log_comb_ite_with_participants_process.loc[log_comb_ite_with_participants_process['ite_prev'] == 'none']
    #log_comb_ite_with_participants_ites = log_comb_ite_with_participants_process.loc[log_comb_ite_with_participants_process['ite_prev'] != 'none']

    # save files
    print('Saving files...')
    log_comb_ite_with_participants_process.to_csv(OUTPUT_PATH + str(process_id) + '.csv', index=False)
    #log_comb_ite_with_participants_ites.to_csv('log_comb_ite_with_participants_ites.csv')
    #log_comb_ite_with_participants_spaces.to_csv('log_comb_ite_with_participants_spaces.csv')

if(__name__ == "__main__"):
    print('Reading files...')
    log_comb_ite_with_participants = pd.read_csv(INPUT_PATH)

    # Add multi processing
    print('Running processes...')
    processes = []
    logs = {}

    # create processes
    num_processes = 40
    step = math.ceil(log_comb_ite_with_participants.shape[0]/ num_processes )
    for i in tqdm(range(num_processes)):
        start = i * step
        if(i + 1 != num_processes):
            end = start + step
        else:
            end = log_comb_ite_with_participants.shape[0]
        logs[i] = pd.DataFrame(log_comb_ite_with_participants[start:end], columns=log_comb_ite_with_participants.columns)
        p = Process(target=Prepare_datasets, args=[logs[i], i])
        processes.append(p)
    # start processes
    for p in tqdm(processes):
        p.start()
    # let processes join
    for p in processes:
        p.join()
    print('DONE')

