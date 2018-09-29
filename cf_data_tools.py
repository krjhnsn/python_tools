#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 26 15:32:58 2017

@author: kjohnson

Name: cF Data Tools
Desc:
    A module containing commonly used functions for data exploration, summary,
    visualization, and input/output.
Change log:
    5/26/17 KJ - initial creation
    6/12/17 KJ - added cross tab function
    7/13/17 KJ - added functionality for reading in bulk downloads (q, e, a, k, alt-sets, exports)
    7/27/17 KJ - added the k-field parser function
    8/22/17 KJ - added documentation to all functions, finished create_export_spec function
    10/26/17 KJ - added the file concatenation function
    10/27/17 KJ - k-field parser now returns k-fields within the k-field formulas
    2/10/18 KJ - added survey XML parser code (need to turn into a function)
    4/11/18 KJ - functions that read in bulk files now automatically strip extra whitespace and newline characters
    4/18/18 KJ - added survey xml parser function and added functionality to return survey question text in 'creat_export_spec' function

TODO:
    - retrieve episode conditions in exports file
    - update k-field parser to look for k-fields in k-fields
    - change the parser function to take paths instead of dataframes
    - ensure all variables and other stuffs are descriptively named
    
"""

# development area

# loop through each column in dataframe creating a sesries
# use value_count() on the series to get the frequency counts
# use Bokeh to create a bar chart or return a list of frequency counts
# you can use show(row(chart1,chart2) to have them appear side by side

#==============================================================================
# export spec creator
#==============================================================================

def create_export_spec(q_path, e_path, k_path, alt_set_path, export_path, export_name, survey_xml_path, delim = '\t'):
    '''
    Desc:
        Reads in bulk download files of system fields and export definitions
        to convert them into a format useful for client facing documentation.
        Note: choose survey xml that contains the same fields that are in
        the export spec you want to create.
        
    Returns:
        Dataframe
    
    Params:
        q_path                  (string)    : path to the q-field text file   
        e_path                  (string)    : path to the q-field text file
        k_path                  (string)    : path to k-field text file
        alt_set_path            (string)    : path to alt-set text file
        export_path             (string)    : path to exports bulk download text file
        export_name             (string)    : name of export to process
        survey_xml_path         (string)    : path to survey xml file
        delim                   (string)    : delimeter used in all text files (tab recommneded)
    
    Example:
        create_export_spec(q_path = 'Documents/q_field.txt'
                           ,e_path = 'Documents/e_field.txt'
                           ,k_path = 'Documents/k_field.txt'
                           ,alt_set_path = 'Documents/alt_set.txt'
                           ,export_path = 'Documents/exports.txt'
                           ,export_name = 'Toyota PLS Export'
                           ,survey_xml_path = 'Documents/survey.xml'
                           ,delim = '\t')
    '''
    
    
    try:
        import pandas as pd
    except:
        print("python package 'pandas' must be installed before using 'create_export_spec' function")
        return
    
    try:
        # read in all inputs          
        q_path = "tms_Q-Fields_Q-Field Details_bulk.txt"
        e_path = "tms_E-Fields.txt"
        k_path = "tms_K-Fields_K-Field Details_bulk.txt"
        alt_set_path = "tms_Alt Sets.txt"
        export_path = "tms_Exports_Export Details_bulk.txt"
        export_name = "ECSC Daily File - PLS (with survey data flag)"
        survey_xml_path = '/Users/kjohnson/Documents/Projects/TMNA/2018_01_16_Survey_XML_Parser/TMNA_PLS.xml'
        delim = '\t'
        
        df_q = read_q_fields(q_path, delim)
        df_e = read_e_fields(e_path, delim)
        df_k = read_k_fields(k_path, delim)
        df_alt = read_alt_sets(alt_set_path, delim)
        df_export = read_exports(export_path, export_name, delim)
    
    except:
        print("failure while reading in text files for function 'create_export_spec'. Ensure the paths are valid and the files use the correct delimeter")
    
    # combine fields into one dataframe
    # key, label, alt-set
    df_q_subset = df_q[['# Key'
                        ,'Name'
                        ,'AlternativeSet']]
    
    df_e_subset = df_e[['# Key'
                        ,'Name'
                        ,'AlternativeSet']]
    
    df_k_subset = df_k[['# Key'
                        ,'Name'
                        ,'AlternativeSet']]
    
    combined = [df_q_subset, df_e_subset, df_k_subset]
    
    # combine field data frames
    df_combined = pd.concat(combined)
    
    # strip whitespace from field names to ensure a clean join to export field names
    df_combined = strip_cols(df_combined)
    df_export = strip_cols(df_export)
    
    # join to export df to associate alt-set number to a field
    df_joined = pd.merge(df_export, df_combined, how = 'left', left_on = ['Export Fields'], right_on = ['# Key'])
    
    # convert alt set number to an int so it joins successfully
    df_alt_set = organize_alt_sets(df_alt)
    df_alt_set['alt_set_number'] = pd.to_numeric(df_alt_set['alt_set_number'], downcast='integer')
    
    # join alt sets onto field list
    df_alt_set_joined = pd.merge(df_joined, df_alt_set, how = 'left', left_on = ['AlternativeSet'], right_on = ['alt_set_number'])
    
    # create dataframe containing survey build info
    df_survey_spec = create_survey_spec(survey_xml_path, q_path)
    df_survey_spec = df_survey_spec[['field', 'survey_question_text']]
    
    # rename column so it can be used for join
    df_survey_spec.columns = ['# Key', 'survey_question_text']

    # join export spec df to survey spec df
    df_merged = pd.merge(df_alt_set_joined, df_survey_spec, on = '# Key', how = 'left', suffixes=('export', 'survey'))
    df_merged = df_merged.fillna('NA')
                                                                                     
    # select the final set of columns we are interested in
    df_export_spec = df_merged[['Column Number'
                                ,'Export Number'
                                ,'Export Name'
                                ,'survey_question_text'
                                ,'Name'
                                ,'Export Fields'
                                ,'alt_set_name'
                                ,'alt_set_labels'
                                ,'alt_set_values']]

    # rename columns as needed
    df_export_spec.rename(columns={'Name': 'Field Name', 'survey_question_text': 'Survey Question Text'}, inplace=True)
    
    return df_export_spec


#==============================================================================
# read in q-fields directly (they are organized nicely)
#==============================================================================

def read_q_fields(path, delim = '\t'):
    '''
    Desc:
        Reads in q-fields text file from system bulk download and returns
        a dataframe object
        
    Returns:
        Dataframe
    
    Params:
        path    (string)    : path to the q-field text file   
        delim   (string)    : delimiter used in text file ('\t' recommended)
    
    Example:
        read_q_fields(path = '/Users/kjohnson/Documents/q_field.txt'
        ,delim = '\t')
    '''
    
    # import packages
    try:
        import pandas as pd
        import os.path
    except:
        print("python package 'pandas' and 'os' must be installed before using 'read_q_fields' function")
        return

    if os.path.isfile(path):
    
        # process text file to dataframe
        try:
            #path = "tms_Q-Fields_Q-Field Details_bulk.txt"    
            #delim = '\t'               
            df_q_field = pd.read_csv(path, sep=delim, skiprows=1, encoding = 'latin-1').reset_index()
            
            # strip extra whitespace and newline characters
            col_list = df_q_field.columns.values.tolist()
            for col_name in col_list:
                try:
                    df_q_field[col_name] = df_q_field[col_name].str.strip()
                except:
                    pass
            
            # return final output
            return df_q_field
        
        except:
            print("error occurred while reading in the q-field text file, check the file path and integrity of data")
            return
    else:
        print("please check the q-field text file path, it appears to be invalid")
        return

#==============================================================================
# read in e-fields (they are organized nicely)
#==============================================================================

def read_e_fields(path, delim = '\t'):
    '''
    Desc:
        Reads in e-fields text file from system bulk download and returns
        a dataframe object
        
    Returns:
        Dataframe
    
    Params:
        path    (string)    : path to the e-field text file   
        delim   (string)    : delimiter used in text file ('\t' recommended)
    
    Example:
        read_e_fields(path = '/Users/kjohnson/Documents/e_field.txt'
                      ,delim = '\t')
    '''
    
    # import packages
    try:
        import pandas as pd
        import os.path
    except:
        print("python package 'pandas' and 'os' must be installed before using 'read_e_fields' function")
        return
    
    # process text file to dataframe
    if os.path.isfile(path):
        try:
            #path = "tms_E-Fields.txt"
            df_e_field = pd.read_csv(path, sep=delim, skiprows=1, encoding = 'latin-1').reset_index()
            
            # strip extra whitespace and newline characters
            col_list = df_e_field.columns.values.tolist()
            for col_name in col_list:
                try:
                    df_e_field[col_name] = df_e_field[col_name].str.strip()
                except:
                    pass
                
            # return final output
            return df_e_field
        
        except:
            print("error occurred while reading in the e-field text file, check the file path and integrity of data")
            return
    else:
        print("please check the e-field file path, it appears to be invalid")
        return

#==============================================================================
# read in a-fields (they are organized nicely)
#==============================================================================

def read_a_fields(path, delim = '\t'):
    '''
    Desc:
        Reads in a-fields text file from system bulk download and returns
        a dataframe object
        
    Returns:
        Dataframe
    
    Params:
        path    (string)    : path to the a-field text file   
        delim   (string)    : delimiter used in text file ('\t' recommended)
    
    Example:
        read_a_fields(path = '/Users/kjohnson/Documents/a_field.txt'
                      ,delim = '\t')
    '''
    
    try:
        import pandas as pd
        import os.path
    except:
        print("python package 'pandas' and 'os' must be installed before using 'read_a_fields' function")
        return
    
    # process text file to dataframe
    if os.path.isfile(path):
        try:
            #path = "tms_E-Fields.txt"
            df_a_field = pd.read_csv(path, sep=delim, skiprows=1, encoding = 'latin-1').reset_index()
            
            # strip extra whitespace and newline characters
            col_list = df_a_field.columns.values.tolist()
            for col_name in col_list:
                try:
                    df_a_field[col_name] = df_a_field[col_name].str.strip()
                except:
                    pass
            
            # return final output
            return df_a_field
        
        except:
            print("error occurred while reading in the a-field text file, check the file path and integrity of data")
            return
    else:
        print("please check the a-field file path, it appears to be invalid")
        return

#==============================================================================
# reading in k-fields (need some processing to read in appropriate rows)
#==============================================================================

def read_k_fields(path, delim = '\t'):
    '''
    Desc:
        Reads in k-fields text file from system bulk download and returns
        a dataframe object. Uses hard-coded text within k-field text
        file to parse the file correctly (check Medallia hasn't changed
        the format of the k-field bulk download if things are breaking)
        
    Returns:
        Dataframe
    
    Params:
        path    (string)    : path to the q-field text file   
        delim   (string)    : delimiter used in text file ('\t' recommended)
    
    Example:
        read_k_fields(path = '/Users/kjohnson/Documents/k_field.txt'
                      ,delim = '\t')
    '''
    
    # import packages
    try:
        import pandas as pd
        import os.path
    except:
        print("python package 'pandas' and 'os' must be installed before using 'read_k_fields' function")
        return

    # process text file to dataframe
    if os.path.isfile(path):
        try:
            #path = "tms_K-Fields_K-Field Details_bulk.txt"
    
            # find the start of the k-field table
            row_count = 0
            stopping_text = "%%CalculatedSurveyField"
            with open(path, 'r') as input_file:
                for line in input_file:
                    row_count = row_count + 1
                    if stopping_text in line:
                        break
        
            df_k_field = pd.read_csv(path, sep='\t', skiprows = row_count, encoding = 'latin-1').reset_index()
            
            # strip extra whitespace and newline characters
            col_list = df_k_field.columns.values.tolist()
            for col_name in col_list:
                try:
                    df_k_field[col_name] = df_k_field[col_name].str.strip()
                except:
                    pass
            
            # return final output
            return df_k_field
        
        except:
            print("error occurred while reading in the k-fields text file, check the file path and integrity of data")
            return
    else:
        print("please check the k-fields file path, it appears to be invalid")
        return

#==============================================================================
# reading in alt-sets (need some processing to get read in appropriate rows)
#==============================================================================

def read_alt_sets(path, delim = '\t'):
    '''
    Desc:
        Reads in alt-sets text file from system bulk download and returns
        a dataframe object. Uses hard-coded text within alt-set text
        file to parse the file correctly. Also uses hard-coded column
        names because Medallia text file doesn't contain them.
        (check Medallia hasn't changed
        the format of the alt-set bulk download if things are breaking).
        
        Each row returned contains alt-set alternative and associated
        name, number etc. (see alt_set_headers list below)
        
    Returns:
        Dataframe
    
    Params:
        path    (string)    : path to the alt-set text file   
        delim   (string)    : delimiter used in text file ('\t' recommended)
    
    Example:
        read_alt_sets(path = '/Users/kjohnson/Documents/alt-set.txt'
                      ,delim = '\t')
    '''
    
    try:
        import pandas as pd
        import os.path
    except:
        print("python package 'pandas' and 'os' must be installed before using 'read_alt_sets' function")
        return

    # process alt-sets into dataframe format
    if os.path.isfile(path):
        try:
            #path = "tms_Alt Sets.txt"
        
            # headers for alt set names 
            # (this list will need to be modified if the Medallia bulk download changes)
            alt_set_headers = ['AltSetNumber'
                               ,'AlternativeNumber'
                               ,'Name'
                               ,'InSurvey'
                               ,'InMobile'
                               ,'InReport'
                               ,'ShortForm'
                               ,'Description'
                               ,'Visibility'
                               ,'SequenceNumber'
                               ,'NumericValue'
                               ,'ExportValue'
                               ,'PriorityRaw'
                               ,'RIColumn'
                               ,'RIColSpan'
                               ,'BoxColor'
                               ,'FontColor'
                               ,'IsOtherOption'
                               ,'TranslationExplanation']
            
            # get the list of alt-set names and their attributes
            line_list = []
            flag = False
            starting_text = "%%AlternativeDb"
            with open(path, 'r') as input_file:
                for line in input_file:
                    if flag:
                        line_list.append(line)
                    if starting_text in line:
                        flag = True
            
            # drop the first element in the list (contains useless header values)
            line_list.pop(0)
            
            # create empty dataframe to hold alt sets
            df_alt_set = pd.DataFrame(columns=alt_set_headers)
            
            # loop through each line and retrieve values to create final dataframe
            for i in range(len(line_list)):
                
                # split line into individual values
                split_values = line_list[i].split('\t')
                trimmed_values = [x.strip(' ') for x in split_values]
                trimmed_values = [x.strip('\n') for x in trimmed_values]
                
                # get the number associated with this alt set
                alt_set_num = trimmed_values[0].split('_')[0]
                trimmed_values.insert(0, alt_set_num)
                
                # add 'NA' values to list if needed to ensure it has the same # of columns as dataframe
                if len(trimmed_values) < len(alt_set_headers):
                    for i in range(len(trimmed_values), len(alt_set_headers)):
                        trimmed_values.append('NA')
                
                # create a dataframe of the parsed alt set list
                df_alt_set_row = pd.DataFrame(data=[trimmed_values], columns = alt_set_headers)
                
                # append the row to the master alt set file
                df_alt_set = df_alt_set.append(df_alt_set_row, ignore_index = True)
                
            # return final dataframe
            return df_alt_set
        
        except:
            print("error occurred while reading in the alt-sets text file, check the file path and integrity of data")
            return
    else:
        print("please check the alt-sets file path, it appears to be invalid")
        return        


#==============================================================================
# alt-set organizer - formats alt-set data according to client specification
#==============================================================================

def organize_alt_sets(dataframe, alt_name = 'Name', alt_number = 'AltSetNumber', alt_label = 'InSurvey', alt_value = 'ExportValue'):
    '''
    Desc:
        Reads in the alt-set dataframe and organizes the alternative values
        and lebls into semi-colon separated list.
        
        *** This can only be called once alt-set text file has been processed
        using 'read_alt_sets' function. Run that function first before attempting
        to run this one ***
        
        User can pass in column names that they are interested in returning
        because alt-set file contains different types of values can can
        be substituted depending on the application.
        
    Returns:
        Dataframe
    
    Params:
        dataframe    (Dataframe)    : dataframe object returned from 'read_alt_sets' function
        alt_name     (string)       : dataframe column name containing the alt-set name
        alt_number   (string)       : dataframe column name containing the alt-set number
        alt_label    (string)       : dataframe column name containing the alt-set labels (e.g. seen in survey)
        alt_value    (string)       : dataframe column name containing the alt-set values (e.g. export values)
    
    Example:
        organize_alt_sets(dataframe = df_alt_set
                          ,alt_name = 'Name'
                          ,alt_number = 'AltSetNumber'
                          ,alt_label = 'InSurvey'
                          ,alt_value = 'ExportValue')
    '''
    
    # testing code
#    dataframe = df_alt
#    alt_name = 'Name'
#    alt_number = 'AltSetNumber'
#    alt_label = 'InSurvey'
#    alt_value = 'ExportValue'
    
    try:
        import pandas as pd
    except:
        print("python package 'pandas' must be installed before using 'organize_alt_sets' function")
        return
  
    try:
        # initialize lists to hold alt set info
        alt_set_name_list = []
        alt_set_number_list = []
        alt_set_value_list = []
        alt_set_label_list = []
        alt_set_dict = {}
        
        # getting list of alt set names (will later be added to dictionary)
        df_alt_set_names = dataframe.loc[:, alt_name]
        alt_set_name_list = df_alt_set_names.unique().tolist()
           
        # select data relevant to one alt set at at time, collect the data
        for i in range(len(alt_set_name_list)):
            
            df_alt_set = dataframe[dataframe[alt_name]==alt_set_name_list[i]].reset_index()
            
            # variables to hold concatenated values
            alt_set_number = ""
            alt_set_value = ""
            alt_set_label = ""
        
            # concatenate values associated to the alt set
            for index, row in df_alt_set.iterrows():
                
            #todo: test for nan values here and handle appropriately, do nothing if found
            # add additional if/else statements
                
                # populate value for alt set number
                if alt_set_number == "":
                    if pd.isnull(row[alt_number]):
                        pass
                    else:
                        alt_set_number = row[alt_number]
                        
                # populate alt set value
                if alt_set_value == "":
                    if pd.isnull(row[alt_value]):
                        pass
                    else:
                        try:
                            value = str(int(row[alt_value])) # don't remember why 'int()' is used here, probably to maintain some number formatting thing
                        except:
                            value = str(row[alt_value])
                    alt_set_value = value
                
                # start creating a semicolon separated list
                else:
                    try:
                        value = str(int(row[alt_value])) # don't remember why 'int()' is used here, probably to maintain some number formatting thing
                    except:
                        value = str(row[alt_value])
                    alt_set_value = str(alt_set_value) + "; " + value
                
                # populate alt set label
                if alt_set_label == "":
                    if pd.isnull(row[alt_label]):
                        pass
                    else:
                        alt_set_label = str(row[alt_label])
            
                # start creating a semicolon separated list
                else:
                    alt_set_label = str(alt_set_label) + "; " + str(row[alt_label])
        
            # add data to final lists
            alt_set_number_list.append(alt_set_number)
            alt_set_value_list.append(alt_set_value)
            alt_set_label_list.append(alt_set_label)
        
        # create dictionary of aggregated data
        alt_set_dict['alt_set_name'] = alt_set_name_list
        alt_set_dict['alt_set_number'] = alt_set_number_list
        alt_set_dict['alt_set_labels'] = alt_set_label_list
        alt_set_dict['alt_set_values'] = alt_set_value_list
        
        # create final dataframe
        df_alt_set_final = pd.DataFrame.from_dict(alt_set_dict)
        
        # arrange the columns in correct order
        df_alt_set_output = df_alt_set_final[['alt_set_name'
                                             ,'alt_set_number'
                                             ,'alt_set_labels'
                                             ,'alt_set_values']]
        
        return df_alt_set_output
    
    except:
        print("and error occured while organizing the alt-set data, please check the integrity of the dataframe being passed in and that column names have been specified as parameters if different than Medallia's system defaults")
        return

#==============================================================================
# read in export definitions (hacky way until better solution can be found)
#==============================================================================

def read_exports(path, export_name, delim = '\t'):
    '''
    Desc:
        Returns information about Medallia export configuration including
        the Export Number, Name, and fields being exported. The intent
        is to return a nicely formatted dataframe that then can be used
        to create export specification documents without all the copy/paste
        labor involved.
        
    Returns:
        Dataframe
    
    Params:
        path          (string)    : path to the export bulk download text file   
        export_name   (string)    : name of the export to return information for
    
    Example:
        read_alt_sets(path = '/Users/kjohnson/Documents/alt-set.txt'
                      ,delim = '\t')
    '''
    
    try:
        import pandas as pd
        import os.path
    except:
        print("python package 'pandas' and 'os' must be installed before using 'read_exports' function")
        return
    
    # note, this will need to change if we decide to include other information from the export
    col_headers = ['Column Number'
                   ,'Export Number'
                   ,'Export Name'
                   ,'Export Fields']
    
    # create dataframe to hold final results
    # df_exports = pd.DataFrame(columns = col_headers)
    
    # process exports into dataframe format
    #export_name = 'Ad Hoc - Chris Verges - Consolidated Rank Test'
    #path = 'tms_Exports_Export Details_bulk.txt'
    if os.path.isfile(path):
        try:
            # read in rows up to the stopping point
            exports_lines = []
            stopping_text = "%%EpisodeCondition"
            with open(path, 'r') as input_file:
                for line in input_file:
                    if stopping_text in line:
                        break
                    exports_lines.append(line)
                       
            # parse the exports_lines list into a dataframe (start from second row to avoid headers)
            for i in range(2, len(exports_lines)):
                
                # split line into individual values
                split_values = exports_lines[i].split('\t')
                trimmed_values = [x.strip(' ') for x in split_values]
                trimmed_values = [x.strip('\n') for x in trimmed_values]
                
                # check if the current line is the export we are interested in returning info for
                if trimmed_values[1] == export_name:
                    # the row we want has ' : ' separating the fields. Try splitting on that sequence of characters.
                    # note: the exported fields sometimes end up in different rows
                    # so we check two locations in the trimmed_values list
                    split_fields = trimmed_values[17].split(' : ')    
                    split_fields = split_fields + trimmed_values[18].split(' : ')
                    split_fields = list(filter(None, split_fields))
                    
                    final_values = []
                    for i in range(0, len(split_fields)):
                        values_list = [str(i+1), trimmed_values[0], trimmed_values[1], split_fields[i]]
                        final_values.append(values_list)
                    
                    # create a dataframe of the export info
                    df_exports = pd.DataFrame(data=final_values, columns = col_headers)
                
            return df_exports
        
        except:
            print("error occurred while reading in the exports text file, the structure of the data may have been changed since downloading from Medallia or this function needs an update")
            return
    else:
        print("please check the exports text file path, it appears to be invalid")
        return        

# this code is put on hold until the exports bulk download is organized more nicely
# the problem is that using regex to pull out the field names sometimes results
# in junk data that must be tested against a list of known good values
# that means this design of the exports reader needs to have the q, e, a field
# lists passed in as parameters.
# redesiging this to only return information relevant for one export at a time
#def read_exports(path, delim = '\t'):
#
#    try:
#        import pandas as pd
#        import os.path
#        import re
#    except:
#        print("python package 'pandas', 'os', and 're' must be installed before using this function")
#        return
#    
#    # process exports into dataframe format
#    if os.path.isfile(path):
#        try:
#            path = "tms_Exports_Export Details_bulk.txt"
#    
#            # read in rows up to the stopping point
#            exports_lines = []
#            stopping_text = "%%EpisodeCondition"
#            with open(path, 'r') as input_file:
#                for line in input_file:
#                    if stopping_text in line:
#                        break
#                    exports_lines.append(line)
#            
#            col_headers = ['Export Number'
#                           ,'Export Name'
#                           ,'Export Fields']
#            
#            # create dataframe to hold final results
#            df_exports = pd.DataFrame(columns = col_headers)
#            
#            # parse the exports_lines list into a dataframe (start from second row to avoid headers)
#            for i in range(2, len(exports_lines)):
#                
#                # split line into individual values
#                split_values = exports_lines[i].split('\t')
#                trimmed_values = [x.strip(' ') for x in split_values]
#                trimmed_values = [x.strip('\n') for x in trimmed_values]
#                
#                # use regex to pull out exported fields
#                # note: the exported fields sometimes end up in different rows
#                # so we check two locations in the trimmed_values list
#                try:
#                    export_string = str(trimmed_values[17])
#                except:
#                    pass
#                try:
#                    export_string = export_string + str(trimmed_values[18])
#                    # use regex to get q_fields and e_fields
#                    q_matches = re.findall("q_\w+", export_string)
#                    e_matches = re.findall("e_\w+", export_string)
#                    k_matches = re.findall("k_\w+", export_string)
#                    
#                    all_matches = q_matches + e_matches + k_matches
#                    string_thing = ' : '.join(all_matches)
#                    
#                    final_values = [trimmed_values[0], trimmed_values[1], string_thing]
#                    
#                    # create a dataframe of the parsed alt set list
#                    df_export_row = pd.DataFrame(data=[final_values], columns = col_headers)
#                    
#                    # append the row to the master exports dataframe
#                    df_exports = df_exports.append(df_export_row, ignore_index = True)
#                except:
#                    pass
#                
#            return df_exports
#        
#        except:
#            print("error occurred while reading in the data, the structure of the data may have been changed since downloading from Medallia or this function needs an update")
#            return
#    else:
#        print("please check the file path, it appears to be invalid")
#        return        

#==============================================================================
# k-field parser
#==============================================================================

def parse_k_fields(df_k_fields
                   ,df_q_fields
                   ,df_e_fields
                   ,df_a_fields):
    '''
    Desc:
        Takes k-field dataframe and parses it using regex to find
        references to other fields (e.g. q-fields, e-fields, k-fields) in the k-field
        calculation.
        
    Returns:
        Dataframe
    
    Params:
        df_k_fields     (Dataframe)     : Dataframe output from read_k_fields function
        df_q_fields     (Dataframe)     : Dataframe output from read_q_fields function
        df_a_fields     (Dataframe)     : Dataframe output from read_a_fields function
        
    Example:
        parse_k_fields(df_k_fields = k_dataframe
                       ,df_q_fields = q_dataframe
                       ,df_e_fields = e_dataframe
                       ,df_a_fields = a_dataframe)
    '''

    try:
        import pandas as pd
        import re
    except:
        print("python package 'pandas' and 're' must be installed before using 'parse_k_fields' function")
        return

    try:
        # initialize lists to hold fields info
        k_field_dict = {}
        q_field_list = []
        e_field_list = []
        a_field_list = []
        k_field_list = []
        df_output = pd.DataFrame(columns=['k-field', 'components'])
        
        # capture fields used in each k-field calculation
        for index, row in df_k_fields.iterrows():
            k_field_name = str(row['# Key']).strip(' ')
            k_field_calculation = str(row['Calculation'])
            
            # use regex to get q_fields and e_fields
            q_matches = re.findall("q_\w+", k_field_calculation)
            e_matches = re.findall("e_\w+", k_field_calculation)
            a_matches = re.findall("a_\w+", k_field_calculation)
            k_matches = re.findall("k_\w+", k_field_calculation)
        
            all_matches = q_matches + e_matches + a_matches + k_matches
        
            # store results in the k-field dict
            k_field_dict[k_field_name] = all_matches
        
        # create list of q-fields, e-fields, a-fields
        # for some reason the q-fields have extra white spaces, use strip() to remove
        df_q_field_names = df_q_fields.loc[:, "# Key"]
        q_field_list = df_q_field_names.unique().tolist()
        q_field_list = [i.strip(' ') for i in q_field_list]
        
        df_e_field_names = df_e_fields.loc[:, "# Key"]
        e_field_list = df_e_field_names.unique().tolist()
        e_field_list = [i.strip(' ') for i in e_field_list]
        
        df_a_field_names = df_a_fields.loc[:, "# Key"]
        a_field_list = df_a_field_names.unique().tolist()
        a_field_list = [i.strip(' ') for i in a_field_list]
        
        df_k_field_names = df_k_fields.loc[:, "# Key"]
        k_field_list = df_k_field_names.unique().tolist()
        k_field_list = [i.strip(' ') for i in k_field_list]
        
        all_list = e_field_list + q_field_list + a_field_list + k_field_list
        
        # test fields collected from k-field formulas for valid q-fields and e-fields
        for key in k_field_dict:
            k_field_matches = k_field_dict[key]
            
            for field in all_list:
                
                if field in k_field_matches:
                    df_output = df_output.append({'k-field':key
                                                  ,'components':field}
                                                  ,ignore_index = True)
        # return the final output
        return df_output
    
    except:
        print("error occurred while parsing k-fields, please verify the integrity of the inputs to 'parse_k_fields' function")
        return
 
# =============================================================================
# create survey spec from survey xml file
# =============================================================================

def create_survey_spec(survey_xml_path, q_field_path, delim = '\t'):
    '''
    Desc:
        Reads survey xml and q-field bulk download file and parses the
        xml structure to extract information that can be used to create
        a survey specification.
        
    Returns:
        Dataframe
    
    Params:
        survey_xml_path    (string)    : path to the survey build xml   
        q_field_path       (string)    : path to the bulk download q-field text file
        delim              (string)    : delimiter used in text file ('\t' recommended)
    
    Example:
        create_survey_spec(survey_xml_path = '/Users/kjohnson/Documents/survey.xml'
                           ,q_field_path = '/Users/kjohnson/Documents/q_field.txt'
                           ,delim = '\t')
    '''

    # =============================================================================
    # import packages
    # =============================================================================
    try:
        import pandas as pd
        import os.path
        from xml.etree.ElementTree import iterparse
    except:
        print("python package 'pandas', 'xml', and 'os' must be installed before using this function")
        return

    # =============================================================================
    # inputs, flags, temp variables
    # =============================================================================
    
    # testing variables (should be commented out if using in production)
    # q_field_path = '/Users/kjohnson/Documents/Projects/TMNA/2018_01_16_Survey_XML_Parser/q.txt'
    # survey_xml_path = '/Users/kjohnson/Documents/Projects/TMNA/2018_01_16_Survey_XML_Parser/TMNA_PLS.xml'
    # output_path = '/Users/kjohnson/Documents/Projects/TMNA/2018_01_16_Survey_XML_Parser/'
    # testing variables
        
    # if one of the question types we want, get info
    #old_types = ['CHOOSE_MANY', 'GRID', 'DROP_DOWN', 'EXPLANATION', 'SIMPLE_QUESTION'] #deprecated
    types = ['CHOOSE_MANY', 'GRID', 'EXPLANATION'] # this is sufficient for the current XML structure. Do not edit this list without understanding the code below.
    
    # flags and temp variables
    page_num = 1 # starting survey page number   
    xml_node_num = 0
    group_node_num = 0
    xml_nestingroup_depth = 0
    xml_group_flag = False

    # dataframe to collect the output
    output = {'xml_node_number': []
            ,'xml_node_depth': []
            ,'survey_page_number': []
            ,'group_name': []
            ,'group_text': []
            ,'group_condition': []
            ,'type': []
            ,'name': []
            ,'field': []
            ,'question_text': []
            ,'condition': []}
    
    # =============================================================================
    # parse xml
    # =============================================================================
    
    if os.path.isfile(survey_xml_path):
        try:   
            # takes xml row by row and tracks start/end of each nested xml node
            for (event, node) in iterparse(survey_xml_path, events=['start', 'end']):
                
                # keeping track of node depth (xml nesting)
                if event == 'start':
                    xml_nestingroup_depth += 1
                if event == 'end':
                    xml_nestingroup_depth -= 1
                
                # for every node collect these attributes if they exist, else use 'NA'
                if event == 'start':
                    node_text = node.attrib.get('text', 'NA')
                    node_type = node.attrib.get('type', 'NA')
                    node_field = node.attrib.get('field', 'NA')
                    node_condition = node.attrib.get('condition', 'NA')
                    node_name = node.attrib.get('name', 'NA')
                
                # collect survey page number
                if node.attrib.get('type', 'NA') == 'PAGE_BREAK' and event == 'start':
                    page_num += 1
                
            # =============================================================================
            # logic for handling 'group' type xml nodes 
            # =============================================================================
               
                # if a group node is started in the xml, capture it's attributes
                if node.attrib.get('type', 'NA') == 'GROUP' and event == 'start':
                    group_name = node.attrib.get('name', 'NA')
                    group_condition = node.attrib.get('condition', 'NA')
                    group_node_num = xml_node_num
                    group_type = node.attrib.get('type', 'NA')
                    group_text = node.attrib.get('text', 'NA')
                    group_depth = xml_nestingroup_depth
                    xml_group_flag = True
                    
                # if not within an xml group node on this iteration, use default values
                elif xml_group_flag == False:
                    group_name = 'NA' 
                    group_condition = 'NA' 
                    group_node_num = 'NA' 
                    group_type = 'NA' 
                    group_text = 'NA'
                    question_text = 'NA'
                    
                # check to see if current iteration is the end of a group
                elif node.attrib.get('type', 'NA') == 'GROUP' and xml_group_flag and event == 'end':
                    xml_group_flag = False
                    question_text = 'NA'
            
            # =============================================================================
            # logic for collecting survey questions information
            #
            # collecting survey question text - this is highly customized to Medallia's XML,
            # if it changes, omg, we dead. The logic below extracts the question text
            # for various different types of xml nodes. Difference question types have
            # the question text placed in different locations in the XML and thus the messy
            # code below.
            #
            # the sequence of the if statements below matter. Question text will be updated
            # to whatever the last 'true' if statement is. So, if one takes priority over the 
            # others, then make sure to add it at the end.
            #
            # this isn't perfect, some places have text nodes which contain survey
            # question text. TBD on the best way to capture all of this information.
            # =============================================================================
            
                if node.attrib.get('type', 'NA') in types and event == 'start':
                    question_text = node.attrib.get('text', 'NA')
                    
                if node.attrib.get('type', 'NA') in types and event == 'end':
                    
                    # for 'EXPLANATION' types we don't want to reset the question_text
                    if not node.attrib.get('type', 'NA') == 'EXPLANATION':
                        question_text = 'NA'
                    
                if node.attrib.get('type', 'NA') == 'ALT_ENTRY' and event == 'start':
                    question_text = node.attrib.get('name', 'NA')
                 
                # if 'SIMPLE_QUESTION' is followed by 'ALT_COLUMN' then use question text from q-field in the 'SIMPLE_QUESTION'
                if node.attrib.get('type', 'NA') == 'ALT_COLUMN' and output['type'][(xml_node_num-1)] == 'SIMPLE_QUESTION':
                    question_text = 'q-field text - ' + output['field'][(xml_node_num-1)]
                
            
            # =============================================================================
            # collecting the final output for each iteration of the xml parser function
            # =============================================================================
                # output dict
                if event == 'start':
                    output['xml_node_number'].append(xml_node_num)
                    output['xml_node_depth'].append(xml_nestingroup_depth)
                    output['survey_page_number'].append(page_num)
                    output['group_name'].append(group_name)
                    output['group_text'].append(group_text)
                    output['group_condition'].append(group_condition)
                    output['type'].append(node_type)
                    output['name'].append(node_name)
                    output['field'].append(node_field)
                    output['question_text'].append(question_text)
                    output['condition'].append(node_condition)
                
                    # increment node counter
                    xml_node_num += 1
            
            # =============================================================================
            # add survey question text from q-fields bulk download to survey spec
            # =============================================================================
            
            # create dataframe from output collected from survey XML
            df_output = pd.DataFrame.from_dict(output, orient='columns')
            df_output = df_output[['xml_node_number'
                                   ,'xml_node_depth'
                                   ,'survey_page_number'
                                   ,'group_name'
                                   ,'group_text'
                                   ,'group_condition'
                                   ,'type'
                                   ,'name'
                                   ,'field'
                                   ,'question_text'
                                   ,'condition']]
            
            # read q-field data into a dataframe
            df_q_field = read_q_fields(q_field_path)
            df_q_field = df_q_field[['# Key', 'In survey']]
                                     
            # rename column so it can be used for joining
            df_q_field.columns = ['field', 'In survey']
                                     
            # join q-field dataframe with XML output dataframe
            df_merged = pd.merge(df_output, df_q_field, on='field', how='left', suffixes=('xml', 'q_field'))
            df_merged = df_merged.fillna('NA')
            
            # function that determines whether to use xml survey question text or text from q-fields
            def select_question_text(df_column_1, df_column_2):
                if (df_column_1 == 'NA' and df_column_2 != 'NA'):
                    return df_column_2
                else:
                    return df_column_1    
            
            # apply the function to each row in the dataframe
            df_merged['survey_question_text'] = df_merged.apply(lambda row: select_question_text(row['question_text'], row['In survey']), axis=1)
            
            # drop columns no longer needed
            df_survey_spec_output = df_merged.drop(['question_text', 'In survey'], axis = 1)
        
            return df_survey_spec_output
        
        except:
            print("error occurred while parsing XML, please verify the file contains a valid xml structure and/or contact your local friendly python enthusiast")
            return
        
    else:
        print("please check the xml file path, it appears to be invalid")
        return
  
#==============================================================================
# utility: strip white space and newlines from column(s) of pandas dataframe
#==============================================================================
    
def strip_cols(dataframe, col_name = None):
    '''
    Desc:
        Removes extra white space and newline characters from dataframe columns
        
    Returns:
        Dataframe
    
    Params:
        dataframe              (Dataframe)     : Dataframe object
        col_name (optional)    (string)        : Column name to be stripped. If blank, all columns are stripped.
    
    Example:
        strip_cols(dataframe = df, col_name = 'column_a')
    '''
    
    if col_name:
        try:
            dataframe[col_name] = dataframe[col_name].str.strip()
        except:
            pass
    else:
        col_list = dataframe.columns.values.tolist()
        for col_name in col_list:
            try:
                dataframe[col_name] = dataframe[col_name].str.strip()
            except:
                pass
    
    return dataframe

#==============================================================================
# cross tab function
#==============================================================================

def cross_tab (dataframe
               ,columns
               ,rows
               ,fillna = None
               ,dropna = False
               ,normalize = False
               ,margins = True):
    '''
    Desc: 
        creates a cross tab between two fields using pandas. The cross tab
        can be used to understand how similar two fields are across a large dataset.
        For example, comparing how well two flags for "Yes/No" agree with eachother.
    
    Returns: cross tab object
    
    Params:
        dataframe   (dataFrame)         : Dataframe object
        columns     (list)              : list containing field names to be shown across top of cross tab (will nest if multiple fields provided)
        rows        (list)              : list containing field names to be shown down rows of cross tab (will nest if multiple fields provided)
        fillna      (string, number)    : if provided, the value will be substituted for null values in the dataframe
        dropna      (boolean)           : if True, null values are exluded from cross tab
        normalize   (boolean)           : if True, cross tab will be normalized (e.g. converted to percentage)
        margins     (boolean)           : if True, margins (totals) are included in cross tab
    
    Example:
        cross_tab(dataframe = DataFrame({'predicted_values': [1, 2, 1],'actual_values': [0, 2, 1]})
                  ,columns = ['predicted_values']
                  ,rows = ['actual_values']
                  ,fillna = 0
                  ,dropna = False
                  ,normalize = False
                  ,margins = True
                  )
    '''
#    ### troubleshooting code
#    dataframe = pd.DataFrame({'predicted_values': [1, 2, 1],'actual_values': [0, 2, 1]})
#    columns = ['actual_values']
#    rows = ['predicted_values']
#    fillna = 0
#    dropna = False
#    normalize = True
#    margins = False
#    
#    ### troubleshooting code
    try:
        import pandas as pd
    except:
        print("python package 'pandas' must be installed before using 'cross_tab' function")
        return
    
    # fill null values if necessary
    if fillna is not None:
        dataframe = dataframe.fillna(fillna)
    
    # create lists of series objects from the dataframe to be cross tabbed
    col_tabs = []
    row_tabs = []
    for column in columns:
        col_tabs.append(dataframe[column])
    for row in rows:
        row_tabs.append(dataframe[row])
    
    # create the crosstab
    ct = pd.crosstab(index = row_tabs
                     ,columns = col_tabs
                     ,margins = margins
                     ,dropna = dropna
                     ,normalize = normalize)
        
    #print(ct)        
    return ct

#==============================================================================
# concatenate files function
#==============================================================================

def concatenate_files(path, file_type, delim ='\t', drop_dupes = None, filename_list = None):
    '''
    Desc:
        Takes contents of separate files and concatenates the contents
        into one dataframe. Useful for generating one file out of many
        small individual files. Assumes that columns are the same
        between all the files.
        
    Returns:
        Dataframe
    
    Params:
        path            (string)    : path to directory holding files  
        file_type       (string)    : looks for files of only a certain type 
        delim           (string)    : delimiter used in the files
        drp_dupes       (string)    : optional parameter to drop duplicates out of final dataframe
        filename_list   (list)      : optional parameter if you only want a subset of files in the directory
    
    Example:
        concatenante_files(path = '/Users/kjohnson/Documents/'
                           ,file_type = '.txt'                   
                           ,delim = '\t')
    '''
    
# test code
#    path = '/Users/kjohnson/Documents/Projects/ToyotaCare_Matching_Study/test_file_delete_me/'
#    file_names = ['toyota_care_iterative_matching_2017-10-20.txt'
#                  ,'toyota_care_iterative_matching_2017-10-23.txt']
#    delim = '\t'
#    file_type = '.txt'
#    drop_dupes = 'Y'
# end test code
    
    # import packages
    try:
        import pandas as pd
        import os.path
    except:
        print("python package 'pandas' and 'os' must be installed before using 'concatenate_files' function")
        return 
    
    # function that actually does the concatenation of the files
    def dataframe_concat(path, filename_list):
        df_output = None
        for file in filename_list:
            full_path = str(path+file)
            df_temp = pd.read_csv(full_path, sep = delim, encoding = 'latin-1').reset_index()
            df_temp['source_file'] = file
            if df_output is not None:
                df_output = pd.concat([df_output, df_temp])
            else:
                df_output = df_temp
        
        # get rid of the residual index column
        df_output.drop('index', axis=1, inplace = True)
        
        return df_output
    
    # error handling then perform concatenation
    try:
        
        # check that the last character in the path is a forward slash '/'
        if path[-1:] != '/':
            path = path + '/'
        
        # verify the directory exists before starting
        if os.path.isdir(path):
            
            # if list of filenames passed in, then use it instead of entire directory
            if filename_list is not None:
                
                # perform file concatenation
                df_output = dataframe_concat(path, filename_list)
                    
            else:
                filename_list = []
                for file in os.listdir(path):
                    if file.endswith(file_type):
                        filename_list.append(file)
                
                # perform file contatenation
                df_output = dataframe_concat(path, filename_list)
                
            # drop duplicates
            if drop_dupes == 'Y':
                df_output = df_output.drop_duplicates()
                
            return df_output
        
        else:
            print("please check the path to the directory holding the input files, it appears to be invalid")
            return
        
    except:
        print("error occurred while concatenating files, check the file path and the number and labeling of columns")
        return




##==============================================================================
## read in r-fields # this doesn't work because of inconsistent data structure in the export
##==============================================================================
#
#export_path = "tms_R-Fields.txt"
#
## read in rows up to the stopping point
#rfield_lines = []
#flag = False
#starting_text = "%%CalculatedReportingField"
#with open(export_path, 'r') as input_file:
#    for line in input_file:
#        row_count = row_count + 1
#        if flag:
#            rfield_lines.append(line)
#        if starting_text in line:
#            flag = True
#
## first line contains column headers, get them
#col_headers = rfield_lines[0].split('\t')
#trimmed_headers = [x.strip(' ') for x in col_headers]
#trimmed_headers = [x.strip('\n') for x in trimmed_headers]
#
#test_split = rfield_lines[1].split('\t')
#test_split2 = rfield_lines[0].split('\t')
#
#
## drop the headers from the list (no longer needed)
#rfield_lines.pop(0)
#
## create dataframe to hold final results
#df_rfield = pd.DataFrame(columns = trimmed_headers)
# 
## parse the list into a dataframe
#for i in range(len(rfield_lines)):
#    
#    # split line into individual values
#    split_values = rfield_lines[i].split('\t')
#    trimmed_values = [x.strip(' ') for x in split_values]
#    trimmed_values = [x.strip('\n') for x in trimmed_values]
#    
#    # add 'NA' values to list if needed to ensure it has the same # of columns as dataframe
#    if len(trimmed_values) < len(trimmed_headers):
#        for i in range(len(trimmed_values), len(trimmed_headers)):
#            trimmed_values.append('NA')
#    
#    # create a dataframe of the parsed alt set list
#    df_rfield_row = pd.DataFrame(data=[trimmed_values], columns = trimmed_headers)
#    
#    # append the row to the master alt set file
#    df_rfields = df_rfield.append(df_rfield_row, ignore_index = True)

#==============================================================================
# Frequency chart (not in use)
#==============================================================================

#def frequency_chart (dataframe
#                     ,field_list
#                     ,file_path=''
#                     ,show_charts):                   
#    '''
#    Desc:
#        used for data exploration by generating frequency count charts
#        for fields of interest in a dataframe. Requires bokeh package
#        to be installed.
#    
#    Returns: nothing
#    
#    Params:
#        dataframe       (dataframe)     : dataframe object
#        field_list      (list)          : list containing field names to plot, each field will be a seperate plot
#        file_path       (string)        : if the user wants to specify a path for the charts
#        show_charts     (boolean)       : if True the charts will display in a web browser
#        
#    Example:
#        frequency_chart(dataframe = DataFrame({'predicted_values': [1, 2, 1],'actual_values': [0, 2, 1]})
#        ,field_list = ['predicted_values', 'actual_values']
#        ,file_path = 'User\charts\
#        ,show_charts = True)
#    '''
#    
#    try:
#        from bokeh.charts import Bar, output_file, show
#    except:
#        print("python package 'bokeh' must be installed before using this function")
#    
#    import bokeh
#    dir(bokeh.charts)
#    dir(bokeh.plotting.figure)
#    
#    data = {
#    'sample': ['1st', '2nd', '1st', '2nd', '1st', '2nd'],
#    'interpreter': ['python', 'python', 'pypy', 'pypy', 'jython', 'jython'],
#    'timing': [-2, 5, 12, 40, 22, 30]
#}
#
#    # x-axis labels pulled from the interpreter column, stacking labels from sample column
#    bar = Bar(data, values='timing', label='interpreter', stack='sample', agg='mean',
#              title="Python Interpreter Sampling", legend='top_right', width=400)
#
#    output_file("stacked_bar2.html")
#         
#    show(bar)    
      
# functions


        
    
