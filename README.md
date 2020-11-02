## ITE_Classification_Using_GMMs

These programs were developed in the course of a research exchange semester at AIT Lab, which is part of the Department of Computer Science of ETH Zurich (https://ait.ethz.ch/),
in cooperation with RWTH Aachen University, supervised by Dr. Anna Maria Feit (https://annafeit.de/).

The overall subject was the analysis of the world's largest online typing study organized by ETH Zurich, Cambridge University and Aalto University. Detailed information can be found here: https://userinterfaces.aalto.fi/typing37k/.

The classification of ITEs using GMMs has been done in succession to another students work on the effectiveness of predictions for increased productivity. For more information, please refer to his github repository: https://github.com/itko/typing_automation. Parts of those programs will also be used in this repository for data preprocessing.


If you are interested in the topic and you want to participate in the online typing study, please refer to the following link: https://typingtest.aalto.fi/
Furthermore, if you happen to be a German speaker, there is also a German typing test available here: https://tipp-test.de/


A detailed description as well as instructions on how to use the programs will be updated shortly.

## Instructions

The aim of this repository is to transfer the raw data of the typing study into a slimmer version containing all essential information for further analysis.

1. Git clone repository https://github.com/itko/typing_automation.
2. Run the data processing steps from the readme-file of the 'typing_automation' repository in order to extract the necessary log_valid.csv file (includes downloading the typing dataset and running necessary SQL-files).
3. Copy the program 'processing_new.py' from this repository under the 'preprocessing'-folder into the 'typing_automation' repository. Modify the file '01_processing_log.ipynb' by substituting all 'processing.py' references with references of the file 'processing_new.py'. 
4. Run the notebook '01_processing_log.ipynb', but skip the third and fourth cell (saving lab participants and optional sub-sampling). After finishing the run of '01_processing_log.ipynb', you should have the file 'log_valid_processed.csv'
5. Modify the following paths in this repository:
5.1 In 'A_1': modify the path to the file 'log_raw.csv' and 'aprticipants_processed.csv' obtained from step 1. Additionally, set the variable INPUT_PATH to the path of your recently obtained 'log_valid_processed.csv'-file.
5.2 OUTPUT_PATH from '(Letter)_2_...'-file should be the same path as INPUT_PATH from '(Letter)_1_..' file. For instance, OUTPUT_PATH of 'A_2_Merge_new_logs' is the INPUT_PATH for the next program 'B_1_Add_columns_user_performance_multiprocessing', and so on. 
6. When all respective paths are set, run file 'A_0_Transform_Logs_Into_Slim_Version'. In the end you will receive the final log-file 'log_valid_processed_slim.csv'.
7. Optionally, if you have split 'log_valid_processed.csv' into multiple smaller files due to hardware limitations (like me), you can use and run 'A_2_Merge_Slim_Files' to merge your sub-log-files.

