import pandas as pd
import tqdm
import os

FILES = []

def parseFloat(df, column):
    df[column] = (df[column].astype(str).str.replace(',', '.', regex=False))
    df[column] = pd.to_numeric(df[column], errors='coerce')
    df[column] = df[column].fillna(0)

    return df

def removingColumns(df):
    colsToRemove = [
        'identifiant_de_document', 
        'reference_document',
        'b_t_q',
        'identifiant_local',
        'nature_culture_speciale'
    ]

    df = df.drop(columns=colsToRemove)

    df = df.drop(list(df.filter(regex='articles_cgi')), axis=1)
    df = df.drop(list(df.filter(regex='_lot')), axis=1)

    return df

def extract_vcf_clean(input_file: str, output_file: str):

    df = pd.read_csv(input_file, sep='|')
    df.drop_duplicates(inplace=True)

    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_').str.replace('/','_')

    df['valeur_fonciere'] = df['valeur_fonciere'].fillna(0)

    df = removingColumns(df)

    df = df.drop(df[(df['surface_reelle_bati'] == 0.0) & (df['surface_terrain'] == 0.0)].index)

    floatCols = ['valeur_fonciere', 'surface_reelle_bati', 'surface_terrain']
    for col in floatCols:
        df = parseFloat(df,col)

    df = df.drop(df[df['type_local'] == 'Local industriel. commercial ou assimilé'].index).drop(df[df['type_local'].isna()].index)
    df = df.drop(df[(df['valeur_fonciere'] < 30000.0) | (df['valeur_fonciere'] > 1000000.0)].index)

    df.reset_index(drop=True, inplace=True)
    df.to_csv(output_file, index=False, mode='a', header=not pd.io.common.file_exists(output_file))


files = os.walk('./data/')

for dirpath, dirnames, filenames in files:
    for filename in filenames:
        if filename.endswith('.txt') and filename.startswith('ValeursFoncieres'):
            print(os.path.join(dirpath, filename))
            FILES.append(os.path.join(dirpath, filename))

os.remove('output/valeurs_foncieres_cleaned.csv') if os.path.exists('output/valeurs_foncieres_cleaned.csv') else None

for file in tqdm.tqdm(FILES):
    extract_vcf_clean(file, 'output/valeurs_foncieres_cleaned.csv')