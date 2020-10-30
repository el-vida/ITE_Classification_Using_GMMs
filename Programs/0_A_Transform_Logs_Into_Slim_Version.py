# The __init__.py file contains all programs and their functions. The aim here was 
# to run all 11 programs dedicated to a specific task in one main program step by step.
from __init__ import *

if(__name__ == "__main__"):
    print('----- CREATING SLIM LOG FILES OF TYPING DATASET ---------------------\n')
    # I have split the log_valid file into 24 files due to hardware restrictions and
    # for better oversight.The program iterates through all 24 sub-logs
    for LOG_ID in tqdm(range(1,25)):
        Run_A_1(LOG_ID)
        Run_A_2(LOG_ID)
        Run_B_1(LOG_ID)
        Run_B_2(LOG_ID)
        Run_C_1(LOG_ID)
        Run_C_2(LOG_ID)
        Run_D_1(LOG_ID)
        Run_D_2(LOG_ID)
        Run_E_1(LOG_ID)
        Run_E_2(LOG_ID)
        Run_F(LOG_ID)
    print('\n----- FINISHED ------------------------------------------------------\n')