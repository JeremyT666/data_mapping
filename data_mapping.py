import logging
import numpy as np
import os
import pandas as pd

# Change directory to current working, mapping data should in Hedging_Detection Folder
os.chdir(os.path.dirname(__file__))
current_path = os.path.join(os.getcwd() + os.sep + 'Hedging_Detection')

# This function is for file and sheet mapping. Return the different list value.


def cross_comparsion(source, target):
    list_a = [f for f in source if f not in target]
    list_b = [f for f in target if f not in source]
    result_list = list_a + list_b
    return result_list


# Log setting - format, log file...etc
mapping_logger = logging.getLogger(name='Mapping_Result')
mapping_logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(message)s - %(asctime)s', datefmt='%Y/%d/%m %H:%M:%S')
result_handler = logging.FileHandler('mapping_result.log')
result_handler.setFormatter(formatter)
mapping_logger.addHandler(result_handler)

success_logger = logging.getLogger(name='success')
success_logger.setLevel(logging.INFO)
success_handler = logging.FileHandler('success_mapping.log')
success_handler.setFormatter(formatter)
success_logger.addHandler(success_handler)

if __name__ == '__main__':
    print('Start data mapping')
    for subdir in os.listdir(current_path):
        # Source file directory and target file directory
        source_file_dir = os.path.join(
            current_path + os.sep + subdir + os.sep + 'Old')
        target_file_dir = os.path.join(
            current_path + os.sep + subdir + os.sep + 'New')
        # Check the file directory(path) is correct
        if os.path.isdir(source_file_dir) and os.path.isdir(target_file_dir):
            # Get the file list in the path and sort it.
            source_file_list = sorted(os.listdir(source_file_dir))
            target_file_list = sorted(os.listdir(target_file_dir))
            # Check file exists in the path, then do file name mapping
            if len(source_file_list) != 0 and len(target_file_list) != 0:
                diff_file = cross_comparsion(
                    source_file_list, target_file_list)
                # File name mapping is successful, then do sheet name mapping
                if len(diff_file) == 0:
                    for file in source_file_list:
                        print('%s - %s is processing...' % (subdir, file))
                        # Check file format is xlsx
                        if file.split('.')[1] == 'xlsx':
                            # Get sheet name list and do cross comparsion
                            source_sheet = pd.ExcelFile(os.path.join(
                                source_file_dir + os.sep + file)).sheet_names
                            target_sheet = pd.ExcelFile(os.path.join(
                                target_file_dir + os.sep + file)).sheet_names
                            diff_sheet = cross_comparsion(
                                source_sheet, target_sheet)
                            # Sheet mapping is successful,  then do value mapping
                            if len(diff_sheet) == 0:
                                # Read all data in excel by Pandas dataframe
                                df1 = pd.read_excel(os.path.join(
                                    source_file_dir + os.sep + file), sheet_name=None)
                                df2 = pd.read_excel(os.path.join(
                                    target_file_dir + os.sep + file), sheet_name=None)
                                counter = 0  # This counter is for successful mapping
                                # Get all values in specific sheet by Pandas dataframe
                                for sheet in source_sheet:
                                    source_df = df1.get(sheet)
                                    target_df = df2.get(sheet)
                                    # Compare rows and columnsthen
                                    if source_df.shape == target_df.shape:
                                        # Convert dataframe into ndarray, then do value compare
                                        comparsion_values = source_df.to_numpy() == target_df.to_numpy()
                                        # Get the different data
                                        if False in comparsion_values:
                                            rows, cols = np.where(
                                                comparsion_values == False)
                                            for item in zip(rows, cols):
                                                # Due to Nan>Nan will be False, this conditionals is for avoiding Nan error
                                                if not (pd.isnull(source_df.iloc[item[0], item[1]]) and pd.isnull(target_df.iloc[item[0], item[1]])):
                                                    mapping_logger.info('[Data Mapping Error]%s - %s row:%s col:%s is different %s ---> %s' % (
                                                        file, sheet, item[0], item[1], source_df.iloc[item[0], item[1]], target_df.iloc[item[0], item[1]]))
                                        else:
                                            counter = counter + 1
                                            # Once all data and sheets mapping is successful, log the successful message in log file
                                            if counter == len(source_sheet):
                                                success_logger.info(
                                                    '%s - %s >>> mapping is successful' % (subdir, file))
                                    else:
                                        mapping_logger.info(
                                            '[Column/Row Error] Column or Row in %s is not equal.' % (sheet))
                            else:
                                mapping_logger.info(
                                    '[Sheet Mapping Error] %s - %s is different in sheet %s' % (subdir, file, diff_sheet))
                        else:
                            mapping_logger.info(
                                '[File Format Error] %s - %s' % (subdir, file))
                else:
                    mapping_logger.info('[File Mapping Error] %s is different in %s' %
                                        (subdir, diff_file))
            else:
                mapping_logger.info(
                    '[Empty File Error] There is empty folder in %s' % (subdir))
        else:
            print('error, there are no files in this directory - %s',
                  os.path.join(current_path + os.sep + subdir))
    print('Data mapping is finished')
