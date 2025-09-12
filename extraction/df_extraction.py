import pandas as pd
import glob
import re
import requests
from datetime import datetime

class extDf:
    def __init__(self, route, sep):
        self.route = route
        self.sep = sep

    def getDf(self):
        try: 
            dfFolder = glob.glob(self.route)

            if len(dfFolder) == 0:
                return {
                    'ok': False,
                    'type_error': 'FileNotFound',
                    'msg': 'The file could not be found',
                    'data': None
                }
            
            df = pd.read_csv(dfFolder[0], sep = self.sep)
            return {
                'ok': True,
                'type_error': None,
                'msg': 'The extraction was succesfull',
                'data': df
            }

        except Exception as e: 
            return {
                    'ok': False,
                    'type_error': 'UknownError',
                    'msg': f'An error has happened. Erorr info: {e}',
                    'data': None
                }
        
    def getCols(self):
        try:
            funcGetDf = self.getDf()

            if funcGetDf['ok'] == False:
                return funcGetDf
            
            df = funcGetDf['data']
            
            dfFiltered = df[[
                'index',
                'Date', 
                'Location', 
                'Operator', 
                'Type',
                'Aboard', 
                'Fatalities', 
                'Ground'
                ]]
            
            cols = ['Aboard', 'Fatalities', 'Ground']
            df[cols] = df[cols].apply(lambda x: pd.to_numeric(x, errors='coerce').astype('Int64'))
            
            return {
                'ok': True,
                'type_error': None,
                'msg': 'The extraction was succesfull',
                'data': dfFiltered
            }
        
        except Exception as e:
            return {
                    'ok': False,
                    'type_error': 'UknownError',
                    'msg': f'An error has happened. Erorr info: {e}',
                    'data': None
                } 
    
class dimValidation:
    def __init__(self, df):
        self.df = df

    def dateFormat(self):
        try:
            pattern = r'^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/(\d{2}|\d{4})$'
            rowsMal = []
            for idx, i in enumerate(self.df['Date']):
                if re.search(pattern, str(i).strip()) is None:
                    rowsMal.append(idx)

            return {
                'ok': True,
                'type_error': None,
                'msg': 'The validation of the date format was succesfully',
                'data': f'There where in total {len(rowsMal)} malformed dates. The rows where {rowsMal}'
                }
        
        except Exception as e:
            return {
                'ok': False,
                'type_error': 'UknownError',
                'msg': f'An error has happened. Erorr info: {e}',
                'data': None
                }
        
    def fatalitiesValidation(self):
        try:
            
            ilogicalValues = self.df.loc[
                (self.df['Fatalities'] > self.df['Aboard']) |
                (self.df['Ground'] > self.df['Fatalities']), ['index']
                ]
            
            return {
                'ok': True,
                'type_error': None,
                'msg': 'The validation of the date format was succesfully',
                'data': f'Total inconsistencies found: {len(ilogicalValues)}. Problematic rows (indices): {list(ilogicalValues.index)}. Check Fatalities vs Aboard and Ground values.'
                }
        
        except Exception as e: 
            return {
                'ok': False,
                'type_error': 'UknownError',
                'msg': f'An error has happened. Erorr info: {e}',
                'data': None
                }
    
    def uniquenessValidation(self):
        try:
            repitedIndex = len(self.df['index'].unique())
            duplicatedRows = self.df.duplicated().sum()
            
            return {
                    'ok': True,
                    'type_error': None,
                    'msg': 'The validation of the uniqueness were succesfully',
                    'data': f'Total repited rows: {duplicatedRows}. Total unique indexes: {repitedIndex}.'
                    }
        
        except Exception as e:
            return {
                'ok': False,
                'type_error': 'UknownError',
                'msg': f'An error has happened. Erorr info: {e}',
                'data': None
                }
        
    def validityCountry(self):
        try:
            countrysGet = requests.get(
                'https://restcountries.com/v3.1/all?fields=name'
                ).json()
        

            commas = list(
            map(lambda x: (x, [i for i, letter in enumerate(x) if letter == ',']),
            self.df['Location'].dropna().unique())
            )
        
            doubleCommas = [[loc, index] for loc, index in commas if len(index) > 1]
            return {
                    'ok': True,
                    'type_error': None,
                    'msg': 'The validation of the uniqueness were succesfully',
                    'data': countrysGet
                    }
        except Exception as e:
            return {
                'ok': False,
                'type_error': 'UknownError',
                'msg': f'An error has happened. Erorr info: {e}',
                'data': None
                }
