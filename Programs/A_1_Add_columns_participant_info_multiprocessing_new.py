import pandas as pd
import math
import argparse
from tqdm import tqdm
from multiprocessing import Process

def get_participants_from_(log):
    # get all sampled participants
    log_raw = pd.read_csv(r'~\Desktop\VB_Schnittstelle\Dataset\log_raw.csv')
    participants = pd.read_csv(r'~\Desktop\VB_Schnittstelle\Dataset\participants_processed.csv')
    participants = participants.set_index('PARTICIPANT_ID')
    participants = participants.loc[log_raw.participant_id.unique()]
    # mark participants contained in log from function argument as sampled
    participants['is_sampled'] = False
    participants.loc[log.participant_id.unique(), 'is_sampled'] = True
    sampled_participants = pd.DataFrame(participants.loc[participants.is_sampled == True], columns=participants.columns)
    return sampled_participants


def add_participant_columns_to_(log, participants, process_id, OUTPUT_PATH):
    # Add columns 'p_gender','p_age','p_device','p_wpm','p_error_rate' to log
    #log['p_gender'] = 'nan'
    #log['p_age'] = 0
    #log['p_device'] = 'nan'
    log['p_wpm'] = 0.0
    #log['p_error_rate'] = 0.0
    #log['p_took_typing_course'] = 0

    for row in range(log.shape[0]):
        index = log.index[row]
        p_id = log.iloc[row]['participant_id']
        p = participants.loc[participants.index == p_id]
        #log.loc[index, 'p_gender'] = p.iloc[0]['GENDER'] 
        #log.loc[index, 'p_age'] = p.iloc[0]['AGE']
        #log.loc[index, 'p_device'] = p.iloc[0]['DEVICE']
        log.loc[index, 'p_wpm'] = p.iloc[0]['WPM']
        #log.loc[index, 'p_error_rate'] = p.iloc[0]['ERROR_RATE']
        #log.loc[index, 'p_took_typing_course'] = p.iloc[0]['HAS_TAKEN_TYPING_COURSE']

    log.to_csv(OUTPUT_PATH + str(process_id) + '.csv', index=False)
    return


def Run_A_1(LOG_ID):
    print(' ----- A1 -----------------------------------------------------------\n')
    global INPUT_PATH
    global OUTPUT_PATH 

    INPUT_PATH  = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_processed_new.csv'
    OUTPUT_PATH = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\Participant_info\\log_' + str(LOG_ID) + '_valid_processed_new_with_participants_after_process_' 


    # print('Reading file...')
    # log = pd.read_csv(args.path, sep=',')
    log = pd.read_csv(INPUT_PATH)
    log_participants=get_participants_from_(log)

    # print('Running processes...')
    # Add multi processing
    processes = []
    logs = {}

    # create processes
    num_processes = 40
    step = math.ceil(log.shape[0]/ num_processes )
    for i in range(num_processes):
        start = i * step
        if(i + 1 != num_processes):
            end = start + step
        else:
            end = log.shape[0]
        logs[i] = pd.DataFrame(log[start:end], columns=log.columns)
        p = Process(target=add_participant_columns_to_, args=[logs[i], log_participants, i, OUTPUT_PATH])
        processes.append(p)
    # start processes
    for p in tqdm(processes):
        p.start()
    # let processes join
    for p in processes:
        p.join()
    print('\nA1 DONE\n')
