import pandas as pd

LOG_ID = 3

INPUT_PATH  = r'~\\Desktop\\VB_Schnittstelle\\Dataset\\Log' + str(LOG_ID) + '\\Corrected\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_new_levenshtein_corrected_after_process_'
OUTPUT_PATH = r'~\\Desktop\\VB_Schnittstelle\\Dataset\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_new_levenshtein_corrected.csv'

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
    
    
    print("Dropping duplicates (if ever)...")
    duplicates = log_space_full.drop_duplicates(keep=False)

    if(duplicates.empty == True):
        print('There are duplicate entries, you need to have a deeper look at that!')
    else:
        print('No duplicate entries!')

    # save file
    print("Saving file...")
    log_space_full.to_csv(OUTPUT_PATH, index=False)

if(__name__ == "__main__"):
    print("Merging datasets...")
    Merge_datasets()

    print("DONE")
