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
            
            df = pd.read_csv(dfFolder[0], sep=self.sep)
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

    def dateValidation(self):
        try:
            self.df['Date'] = pd.to_datetime(self.df['Date'], format='%m/%d/%Y', errors='coerce')
            self.df = self.df.loc[self.df['Date'] >= '2000-01-01'].copy()
            return self.df
        
        except Exception as e:
            return {
                'ok': False,
                'type_error': 'UknownError',
                'msg': f'An error has happened. Erorr info: {e}',
                'data': self.df
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
            
            countrys = {
                'commom name': [],
                'official name': []
            }

            for countryName in countrysGet:
                countrys['commom name'].append(countryName['name']['common'])
                countrys['official name'].append(countryName['name']['official'])

            allLocations = self.df['Location'].dropna().unique()
            invalidCountrys = []

            for loc in allLocations:
                names = [n.strip() for n in re.split(r'[,\;/\-]', loc)]
                for name in names:
                    if name not in countrys['commom name'] and name not in countrys['official name']:
                        invalidCountrys.append(name)

            def clean_location(location):
                if not isinstance(location, str):
                    return location
                
                parts = [p.strip() for p in location.replace(';', ',').replace('/', ',').replace('-', ',').split(',')]
                parts = [p for p in parts if p not in invalidCountrys]
                return ', '.join(parts)

            self.df['Location'] = self.df['Location'].apply(clean_location)

            return {
                'ok': True,
                'type_error': None,
                'msg': 'The validation of the uniqueness were succesfully',
                'data': self.df
            }
        
        except Exception as e:
            return {
                'ok': False,
                'type_error': 'UknownError',
                'msg': f'An error has happened. Erorr info: {e}',
                'data': None
            }
