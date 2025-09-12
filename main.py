from extraction.df_extraction import extDf, dimValidation
import requests

def main():
    classExtraction = extDf('data/dataset/Airplane_Crashes_and_Fatalities_Since_1908.csv', ',')
    dfCrashesAirplane = classExtraction.getCols()['data']

    classValidation = dimValidation(dfCrashesAirplane)
    datesValidation = classValidation.dateFormat()
    fatalitiesValidation = classValidation.fatalitiesValidation()
    uniquennesValidation = classValidation.uniquenessValidation()
    countrysValidation = classValidation.validityCountry()
    #print(countrysValidation['data'])
    
    for countryName in countrysValidation['data']:
        print({
            'common name': countryName['name']['common'],
            'oficcial name': countryName['name']['official']
        })
    
    
if __name__ == '__main__':
    main()