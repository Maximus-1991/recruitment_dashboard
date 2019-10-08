import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None

def create_df(dictionary):
    '''
    Convert dictionary into pandas DataFrame
    Inputs:

    df_dict: dictionary containing candidate data
    Outputs:

    DataFrame containing the same candidate data as the input
    '''
    df = pd.DataFrame.from_dict(dictionary, orient='columns')
    return df

def merge_df(df1, df2, how='left', on=['id']):
    '''
    Function to create candidate activity dictionary
    Inputs:

    df_dict: dictionary containing candidate data
    Outputs:

    DataFrame containing the same candidate data as the input
    '''
    df = pd.merge(df1, df2, how=how, on=on)
    return df


def transform_df(df):
    '''
    Function to transform DataFrame
    Inputs:

    df_dict: dictionary containing candidate data
    Outputs:

    '''
    # Rename duplicate column name to prevent error when creating SQL database
    df = df.rename(columns={'sourced': 'is_sourced'})

    # Replace and Delete columns
    df['To schedule'][(df['Inplannen 1e gesorek'].isnull() == False) & (df['To schedule'].isnull() == True)] = \
    df['Inplannen 1e gesorek'][(df['Inplannen 1e gesorek'].isnull() == False) & (df['To schedule'].isnull() == True)]
    df.drop('Inplannen 1e gesorek', axis=1, inplace=True)
    df['To schedule'][(df['Inplannen 1e gesprek'].isnull() == False) & (df['To schedule'].isnull() == True)] = \
    df['Inplannen 1e gesprek'][(df['Inplannen 1e gesprek'].isnull() == False) & (df['To schedule'].isnull() == True)]
    df.drop('Inplannen 1e gesprek', axis=1, inplace=True)
    df['1st Interview'][(df['inplannen 2e gesprek'].isnull() == False) & (df['1st Interview'].isnull() == True)] = \
    df['inplannen 2e gesprek'][(df['inplannen 2e gesprek'].isnull() == False) & (df['1st Interview'].isnull() == True)]
    df.drop('inplannen 2e gesprek', axis=1, inplace=True)
    df['1st Interview'][(df['1e gesprek'].isnull() == False) & (df['1st Interview'].isnull() == True)] = \
    df['1e gesprek'][(df['1e gesprek'].isnull() == False) & (df['1st Interview'].isnull() == True)]
    df.drop('1e gesprek', axis=1, inplace=True)
    df['1st Interview'][(df['Interview 1'].isnull() == False) & (df['1st Interview'].isnull() == True)] = \
    df['Interview 1'][(df['Interview 1'].isnull() == False) & (df['1st Interview'].isnull() == True)]
    df.drop('Interview 1', axis=1, inplace=True)
    df['2nd Interview'][(df['Interview 2'].isnull() == False) & (df['2nd Interview'].isnull() == True)] = \
    df['Interview 2'][(df['Interview 2'].isnull() == False) & (df['2nd Interview'].isnull() == True)]
    df.drop('Interview 2', axis=1, inplace=True)
    df['2nd Interview'][(df['Assessment'].isnull() == False) & (df['2nd Interview'].isnull() == True)] = \
    df['Assessment'][(df['Assessment'].isnull() == False) & (df['2nd Interview'].isnull() == True)]
    df.drop('Assessment', axis=1, inplace=True)
    df['2nd Interview'][(df['2e gesprek'].isnull() == False) & (df['2nd Interview'].isnull() == True)] = \
    df['2e gesprek'][(df['2e gesprek'].isnull() == False) & (df['2nd Interview'].isnull() == True)]
    df.drop('2e gesprek', axis=1, inplace=True)
    df['Offer'][(df['Aanbieding'].isnull() == False) & (df['Offer'].isnull() == True)] = df['Aanbieding'][
        (df['Aanbieding'].isnull() == False) & (df['Offer'].isnull() == True)]
    df.drop('Aanbieding', axis=1, inplace=True)

    df['Hired'][(df['Aangenomen'].isnull() == False) & (df['Hired'].isnull() == True)] = df['Aangenomen'][
        (df['Aangenomen'].isnull() == False) & (df['Hired'].isnull() == True)]
    df.drop('Aangenomen', axis=1, inplace=True)

    # Delete Columns
    df.drop('Test Fase', axis=1, inplace=True)
    df.drop('intern evalueren', axis=1, inplace=True)
    df.drop('Plan 1', axis=1, inplace=True)
    df.drop('Plan 2', axis=1, inplace=True)
    df.drop('Vergaarbak', axis=1, inplace=True)

    # Remove only for now (will be used for source of candidate)
    df.drop('tags', axis=1, inplace=True)

    # Replace np.nan with None, as None is accepted if df is written to a DB using df.to_sql
    # Note that None will only be converted to NULL in SQL if df.to_sql is used, not using executemany
    # NaT is converted as None if using to_sql
    df = df.where((pd.notnull(df)), None)

    # Convert None to 'nan' if getting errors when inserting into MySQL DB
    # df.fillna(value='nan', inplace=True)

    # Convert date columns into DATE columns with specified format
    date_cols = [
        'hired_at',
        'Sourced',
        'Applied',
        'Shortlisted',
        'Talentpool',
        'Review',
        'To schedule',
        '1st Interview',
        '2nd Interview',
        'Offer',
        'Hired',
        'disqualified_at'
    ]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d'))
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.encode('ascii', 'ignore').str.decode('ascii')
            df[col][df[col].isnull() == False] = df[col][df[col].isnull() == False].apply(lambda x: x.lower())
    return df