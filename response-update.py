#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 11:05:13 2020

@author: keir_johnson
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description:
Purpose: update values of embedded data fields in Qualtrics responses

Inputs:
- API token
- Qualtrics data center
- .csv containing responses to update
    Required columns: 'SurveyId', 'ResponseId', at least one embedded data field name to update
    Optional columns: additional names of embedded data fields to update and other columns for context. 
        Be sure to exclude columns that should not be part of the update in the 'parameters' section below

Outputs:
- .csv containing original data and the results of each API request

Change log:
2020-08-21 KJ: updated to accept multiple surveys in one input file
2019-12-16 KJ: initial creation

To do:
- add logic to confirm # of rows to be updated before proceeding, also, survey name
"""

import requests
from datetime import datetime
import json
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

# =============================================================================
# function to call response update API
# =============================================================================

def make_update(response_id, base_url, token, survey_id, ed_fields):
    result = {}
    try:
        response = requests.put(
            url=base_url+response_id,
            headers={
    			'X-API-TOKEN':token,
    			'Content-Type':'application/json'
    		},
    		data = json.dumps({
    			"surveyId":survey_id,
                "resetRecordedDate":False,
    			"embeddedData":ed_fields
    		})
    	)
        result['Detail'] = "Status: {0}, Body: {1}, ResponseId: {2}, Request Body: {3} ".format(response.status_code, response.content, response_id, ed_fields)
        result['HTTP Status'] = "Status: {0}".format(response.status_code)
        print(result['Detail']+"\n")
    except:
        result['Detail'] = "Error: SurveyId: {0}, ResponseId: {1}, Request Body: {2}".format(survey_id, response_id, ed_fields)
        result['HTTP Status'] = "Function Error"
        print(result['Detail']+"\n")
    
    return result

# =============================================================================
# WAIT!! Before running, verify parameters below!! #
# =============================================================================
DC = "az1"
base_url = "https://{0}.qualtrics.com/API/v3/responses/".format(DC)
token = os.environ.get("fmcna-api-token")

input_file = "/Users/keir_johnson/Downloads/azura-azura.csv"
errList = {}
# flag to generate processing report (yes/no)
generate_report_yn = 'yes'

# if there are columns in the incoming file that are not ED fields and should not be updated, list them here
excluded_cols = ['SurveyId', 'ResponseId']

# =============================================================================
# Main code runs from here
# =============================================================================

# read in input file
df = pd.read_csv(input_file)

# get unique list of survey IDs to update
survey_ids = set(df['SurveyId'].tolist())
col_list = df.columns.tolist()

# remove excluded columns
col_list = [col for col in col_list if col not in excluded_cols]

# counters and temp storage
total_rows = len(df)
count = 0
ed_fields = {}

# loop through survey ids to update
for survey_id in survey_ids:
    
    # get rows associated with single survey ID at a time
    rows_to_update = df[df['SurveyId'] == survey_id]
    
    # loop through each row (response) and make update
    for index, row in rows_to_update.iterrows():
        count+= 1
        for col in col_list:
            ed_fields[col] = row[col]
        print("Updating row {0}/{1}, SurveyId: {2}, ResponseId: {3}".format(count, total_rows, survey_id, row["ResponseId"]))
        
        # store result of API request for processing report results
        result = make_update(row["ResponseId"], base_url, token, survey_id, ed_fields)
        df.at[index, 'Result Detail'] = result['Detail']
        df.at[index, 'HTTP Status'] = result['HTTP Status']

# create processing report (report placed in current python working directory)
if generate_report_yn == 'yes':
    now = datetime.today().strftime('%Y-%m-%d-%H.%M.%S')
    filename = "update-responses-processing-report-"+now+".csv"
    df.to_csv(filename)