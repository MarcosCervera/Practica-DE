import pandas as pd
import hashlib


def anonymize(value):
    if pd.isnull(value):
        return value
    if isinstance(value, (int, float)):
        value = str(value)
    return hashlib.sha256(value.encode()).hexdigest()[:12]


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["variable"] != 'milestones']
    df = df.drop(['e', 't'], axis=1)
    df = df.pivot(index='fecha', columns='variable', values='valor')
    df = df.reset_index()
    df['fecha'] = pd.to_datetime(df['fecha'])
    df2 = df[df["fecha"] >= '2024-01-01']

    cols_anonimizar = ["usd","usd_of","usd_of_minorista"]

    for col in cols_anonimizar:
        df2[f"{col}_hash"] = df2[col].apply(anonymize)

    return df2
