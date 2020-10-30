import pandas as pd
import os

log_ids = 23

def Merge_datasets():
    logs = {}
    for i in range(log_ids+1):
        INPUT_PATH  = r'~\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(i+1) + '\\log_' + str(i+1) + '_valid_processed_slim.csv'
        logs[i] = pd.read_csv(INPUT_PATH)
    # concatenate data frames
    log_space_full = pd.concat([logs[0],logs[1],logs[2],logs[3],logs[4],logs[5],logs[6],logs[7],logs[8],logs[9],
                                logs[10],logs[11],logs[12],logs[13],logs[14],logs[15],logs[16],logs[17],logs[18],logs[19],
                                logs[20],logs[21],logs[22],logs[23]])
    
    
    # print("Dropping duplicates (if ever)...")
    duplicates = log_space_full.drop_duplicates(keep=False)

    if(duplicates.empty == True):
        print('There are duplicate entries, you need to have a deeper look at that!')
    #else:
    #    print('No duplicate entries!')

    # save file
    # print("Saving file...")
    OUTPUT_PATH = r'~\\Desktop\\VB_Schnittstelle\\Dataset\\log_valid_processed_slim.csv'
    log_space_full.to_csv(OUTPUT_PATH, index=False)

if (__name__ == "__main__"):
    print(' ----- FINAL MERGE OF SLIM FILES -----------------------------------\n')
    
    # print("Merging datasets...")
    Merge_datasets()

    print('\nFINAL MERGE DONE\n')