from extraction.df_extraction import extDf, dimValidation
import re

def main():
    classExtraction = extDf('data/dataset/Airplane_Crashes_and_Fatalities_Since_1908.csv', ',')
    dfCrashesAirplane = classExtraction.getCols()['data']

    classValidation = dimValidation(dfCrashesAirplane)
    datesValidation = classValidation.dateFormat()
    fatalitiesValidation = classValidation.fatalitiesValidation()
    uniquennesValidation = classValidation.uniquenessValidation()


if __name__ == '__main__':
    main()