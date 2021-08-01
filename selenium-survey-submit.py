#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 13:22:28 2021

@author: keir_johnson
"""

from selenium.webdriver import Firefox, FirefoxProfile
import pandas as pd
import time

# =============================================================================
# The purpose of this is to be able to automatically submit survey responses
# using Qualtrics anonymous links
# =============================================================================

# =============================================================================
# Get file with survey links
# =============================================================================

path = '/Users/keir_johnson/Downloads/in-center-test-file-7-28-21-Q_LOAD.csv'
df_survey_links = pd.read_csv(path)
survey_links = list(df_survey_links['SurveyURL'])
survey_links_subset = survey_links[20:]

# =============================================================================
# Open survey links in web browser, complete survey
# =============================================================================

browser = Firefox()

for survey_link in survey_links_subset:
    browser.get(survey_link)
    time.sleep(2)
    browser.find_element_by_id('NextButton').click()
    time.sleep(1)