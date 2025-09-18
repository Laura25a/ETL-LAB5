# transform.py
from extraction.df_extraction import extDf, dimValidation
import pandas as pd  
import requests

# Extracción de datos
#######################
classExtraction = extDf('data/dataset/Airplane_Crashes_and_Fatalities_Since_1908.csv', ',')
dfCrashesAirplane = classExtraction.getCols()['data']

classValidation = dimValidation(dfCrashesAirplane)
datesValidation = classValidation.dateValidation() 
fatalities_check = classValidation.fatalitiesValidation()
uniqueness_check = classValidation.uniquenessValidation()
country_check = classValidation.validityCountry()

df = classValidation.df

# Imprimir resultados de validación inicial
print("=== Fatalities Validation ===")
print(fatalities_check['data'])
print("=== Uniqueness Validation ===")
print(uniqueness_check['data'])
print("=== Country Validation ===")
print(country_check['data'][:10])

# ###########################
# Limpieza de datos
########################
print("================ CLEAN =================")


# Reemplazo de nulos de manera lógica

# Columnas de texto
text_cols = ['Location', 'Operator', 'Type']
for col in text_cols:
    df[col] = df[col].fillna('Unknown')
    df[col] = df[col].replace(r'^\s*$', 'Unknown', regex=True)

# Columnas numéricas
# Aboard
if df['Aboard'].isna().sum() > 0:
    median_aboard = df['Aboard'].median()
    df['Aboard'] = df['Aboard'].fillna(median_aboard)

# Fatalities
if df['Fatalities'].isna().sum() > 0:
    df['Fatalities'] = df.apply(
        lambda row: 0 if pd.isna(row['Fatalities']) and not pd.isna(row['Aboard']) else row['Fatalities'], axis=1
    )
    df['Fatalities'] = df['Fatalities'].fillna(df['Fatalities'].median())

# Ground
df['Ground'] = df.apply(lambda row: min(row['Ground'], row['Fatalities']) if pd.notna(row['Ground']) else row['Ground'], axis=1)

# Reemplazar valores excesivos (> 500) por mediana
median_ground = df['Ground'].median()
df['Ground'] = df['Ground'].apply(lambda x: median_ground if pd.notna(x) and x > 500 else x)

# Reemplazar los valores nulos de Ground por 0 (o usar otra lógica)
df['Ground'] = df['Ground'].fillna(0)


# Validación post-transformación
validation = dimValidation(df)

date_check = validation.dateValidation()
fatalities_check = validation.fatalitiesValidation()
uniqueness_check = validation.uniquenessValidation()
country_check = validation.validityCountry()



print("=== Date Validation post transformacion ===")
print(date_check)
print("=== Fatalities Validation post transformacion ===")
print(fatalities_check['data'])
print("=== Uniqueness Validation post transformacion ===")
print(uniqueness_check['data'])
print("=== Country Validation post transformacion ===")
print(country_check['data'][:10])
print(df.columns)

# Quality report
###############################
def column_quality_report(df):
    report = []

    subset_cols = ['Date', 'Location', 'Operator', 'Type']  

    for col in df.columns:
        col_data = df[col]
        col_type = col_data.dtypes
        n_nulls = col_data.isna().sum()
        pct_nulls = (n_nulls / len(df) * 100).round(2)
        n_unique = col_data.nunique()
        
        # Duplicados
        duplicated_rows = df.duplicated(subset=subset_cols, keep=False).sum() if col in subset_cols else 0
        
        # Ejemplos problemáticos
        problem_values = []
        if pd.api.types.is_numeric_dtype(col_data):
            if col == 'Fatalities':
                problem_values = df[df['Fatalities'] > df['Aboard']][col].tolist()
            elif col == 'Ground':
                problem_values = df[df['Ground'] > df['Fatalities']][col].tolist()
            else:
                problem_values = col_data[col_data < 0].tolist()
        else:
            problem_values = col_data[col_data.isna() | (col_data.astype(str).str.strip() == '')].tolist()[:5]

        report.append({
            'Column': col,
            'DataType': col_type,
            'N_Nulls': n_nulls,
            'Pct_Nulls': pct_nulls,
            'Unique_Values': n_unique,
            'Duplicated_Rows': duplicated_rows,
            'Problematic_Examples': problem_values
        })

    return pd.DataFrame(report)

# Generar reporte
quality_df = column_quality_report(df)
print(quality_df)
