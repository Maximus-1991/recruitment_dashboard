"""
Functions to create candidate data DataFrames
"""

import pandas as pd
pd.options.mode.chained_assignment = None

def create_df(dictionary):
    '''
    Functions that converts dictionary into pandas DataFrame

    Args:
        dictionary: dictionary to be converted into pandas DataFrame

    Returns:
        created_df: pandas DataFrame
    '''
    created_df = pd.DataFrame.from_dict(dictionary, orient='columns')
    return created_df

def merge_df(df1, df2, how='left', merge_on='id'):
    '''
    Function to create candidate activity dictionary

    Args:
        df1: left DataFrame to merge
        df2: right DataFrame to merge
        how: Type of merge to be performed
        merge_on: Column or index level names to join on (single label or list)

    Returns:
        merged_df: merged DataFrame
    '''
    merged_df = pd.merge(df1, df2, how=how, on=merge_on)
    return merged_df


def transform_df(df_to_trans):
    '''
    Function to transform DataFrame

    Args:
        df_transform: DataFrame to be transformed

    Returns:
        transformed_df: Transformed DataFrame

    '''
    # Rename duplicate column name to prevent error when creating SQL database
    df_to_trans = df_to_trans.rename(columns={'sourced': 'is_sourced'})

    # Replace and Delete columns
    change_col = 'Inplannen 1e gesorek'
    keep_col = 'To schedule'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = 'Inplannen 1e gesprek'
    keep_col = 'To schedule'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = 'inplannen 2e gesprek'
    keep_col = '1st Interview'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = '1e gesprek'
    keep_col = '1st Interview'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = 'Interview 1'
    keep_col = '1st Interview'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = 'Interview 2'
    keep_col = '2nd Interview'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = 'Assessment'
    keep_col = '2nd Interview'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = '2e gesprek'
    keep_col = '2nd Interview'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = 'Aanbieding'
    keep_col = 'Offer'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    change_col = 'Aangenomen'
    keep_col = 'Hired'
    df_to_trans[keep_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())] = \
    df_to_trans[change_col][(df_to_trans[change_col].notnull()) & (df_to_trans[keep_col].isnull())]
    df_to_trans.drop(change_col, axis=1, inplace=True)

    # Delete Columns
    df_to_trans.drop('Test Fase', axis=1, inplace=True)
    df_to_trans.drop('intern evalueren', axis=1, inplace=True)
    df_to_trans.drop('Plan 1', axis=1, inplace=True)
    df_to_trans.drop('Plan 2', axis=1, inplace=True)
    df_to_trans.drop('Vergaarbak', axis=1, inplace=True)

    # Remove only for now (will be used for source of candidate)
    df_to_trans.drop('tags', axis=1, inplace=True)

    # Replace np.nan with None, as None is accepted if df is written to a DB using df.to_sql
    # None will only be converted to NULL in SQL if df.to_sql is used, not using executemany
    # NaT is converted as None if using to_sql
    df_to_trans = df_to_trans.where((pd.notnull(df_to_trans)), None)

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
        df_to_trans[date_col] = pd.to_datetime(pd.to_datetime(df_to_trans[date_col]).dt.strftime('%Y-%m-%d'))
    for col in df_to_trans.columns:
        if df_to_trans[col].dtype == 'object':
            df_to_trans[col] = df_to_trans[col].str.encode('ascii', 'ignore').str.decode('ascii')
            df_to_trans[col][df_to_trans[col].notnull()] = \
            df_to_trans[col][df_to_trans[col].notnull()].apply(lambda x: x.lower())
    transformed_df = df_to_trans
    return transformed_df
