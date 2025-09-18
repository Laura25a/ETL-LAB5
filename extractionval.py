from extraction.df_extraction import extDf, dimValidation

def extraction():
    classExtraction = extDf('data/dataset/Airplane_Crashes_and_Fatalities_Since_1908.csv', ',')
    dfCrashesAirplane = classExtraction.getCols()['data']
   

    classValidation = dimValidation(dfCrashesAirplane)
    datesValidation = classValidation.dateValidation()
    fatalitiesValidation = classValidation.fatalitiesValidation()
    uniquennesValidation = classValidation.uniquenessValidation()
    dfCrashesAirplane = classValidation.validityCountry()
    df = classValidation.df
    
    return df 
    

