import pandas as pd
import os

num_processes = 40

def Merge_datasets():
    logs = {}
    for i in range(num_processes):
        logs[i] = pd.read_csv(INPUT_PATH + str(i) + '.csv')
    # concatenate data frames
    log_space_full = pd.concat([logs[0],logs[1],logs[2],logs[3],logs[4],logs[5],logs[6],logs[7],logs[8],logs[9],
                                logs[10],logs[11],logs[12],logs[13],logs[14],logs[15],logs[16],logs[17],logs[18],logs[19],
                                logs[20],logs[21],logs[22],logs[23],logs[24],logs[25],logs[26],logs[27],logs[28],logs[29],
                                logs[30],logs[31],logs[32],logs[33],logs[34],logs[35],logs[36],logs[37],logs[38],logs[39]])
    
    
    # print("Dropping duplicates (if ever)...")
    duplicates = log_space_full.drop_duplicates(keep=False)

    if(duplicates.empty == True):
        print('There are duplicate entries, you need to have a deeper look at that!')
    #else:
    #    print('No duplicate entries!')

    # save file
    # print("Saving file...")
    log_space_full.to_csv(OUTPUT_PATH, index=False)

    # deleting subfiles
    for i in range(num_processes):
        INPUT_PATH_I = INPUT_PATH + str(i) + '.csv'
        if os.path.exists(INPUT_PATH_I):
            os.remove(INPUT_PATH_I)
        else:
            print("The file does not exist")

def Run_D_2(LOG_ID):
    print(' ----- D2 -----------------------------------------------------------\n')
    global INPUT_PATH
    global OUTPUT_PATH

    INPUT_PATH  = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\New_Levenshtein\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_new_levenshtein_after_process_'
    OUTPUT_PATH = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_new_levenshtein.csv'

    # print("Merging datasets...")
    Merge_datasets()

    print('\nD2 DONE\n')
