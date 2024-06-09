import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["variable"] != 'milestones']
    df = df.drop(['e', 't'], axis=1)
    df = df.pivot(index='fecha', columns='variable', values='valor')
    df = df.reset_index()
    df['fecha'] = pd.to_datetime(df['fecha'])
    df2 = df[df["fecha"] >= '2024-01-01']
    return df2
