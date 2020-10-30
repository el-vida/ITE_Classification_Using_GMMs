import pandas as pd
import numpy as np
import ftfy
import math
from tqdm import tqdm
from multiprocessing import Process

# TO DO
# - transfer values from new_lev_dist to lev_dist (basically drop lev_dist and rename new_lev_dist)
# - drop columns: 'len_diff','is_rep','is_forward','entry_id','user_type', 'iki_norm', 'lev_dist'

def Run_F(LOG_ID):
    print(' ----- F  -----------------------------------------------------------\n')
    global INPUT_PATH
    global OUTPUT_PATH 

    INPUT_PATH  = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_post_processed_new_decrypted_new_levenshtein_corrected.csv'
    OUTPUT_PATH = r'C:\\Users\\eu_el\\Desktop\\VB_Schnittstelle\\Dataset\\Data\\Log' + str(LOG_ID) + '\\log_' + str(LOG_ID) + '_valid_processed_slim.csv'


    print('Reading file...')
    log = pd.read_csv(INPUT_PATH)

    # Drop columns 'len_diff','is_rep','is_forward','entry_id','user_type', 'iki_norm', 'lev_dist'
    log = log.drop(['len_diff','is_rep','is_forward','entry_id','user_type', 'iki_norm', 'lev_dist'], axis=1)

    # Renaming column 'new_lev_dist' --> 'lev_dist'
    log = log.rename(columns={"new_lev_dist": "lev_dist"})

    # Save file
    #print('Saving file...')
    log.to_csv(OUTPUT_PATH, index=False)

    print('\nF DONE\n')



