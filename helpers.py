import pyspark
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

def createDFFromFileAndSchema(sparkSession, filePath, schemaPath):
    print(f"File path: {filePath}, schema path: {schemaPath}")
    ### load schema
    dtypes = pd.read_csv(schemaPath).to_records(index=False).tolist()
    print(f"Types from schema: {dtypes}")
    fields = [StructField(dtype[0], globals()[f'{dtype[1]}Type']()) for dtype in dtypes]
    schema = StructType(fields)
    created_df = sparkSession.read.option('header', 'true').csv(filePath, header=True, schema=schema)
    return created_df


def clean_special_letters(df, column):
    df = df.withColumn(column, translate(column, 'íîìïīį', 'i'))
    df = df.withColumn(column, translate(column, 'ÎÏÍĪĮÌ', 'I'))
    df = df.withColumn(column, translate(column, 'àáâäæãåā', 'a'))
    df = df.withColumn(column, translate(column, 'ÀÁÂÄÆÃÅĀ', 'A'))
    df = df.withColumn(column, translate(column, 'èéêëēėę', 'e'))
    df = df.withColumn(column, translate(column, 'ÈÉÊËĒĖĘ', 'E'))
    df = df.withColumn(column, translate(column, 'ûüùúū', 'u'))
    df = df.withColumn(column, translate(column, 'ÛÜÙÚŪ', 'U'))
    df = df.withColumn(column, translate(column, 'ÔÖÒÓŒØŌÕ', 'O'))
    df = df.withColumn(column, translate(column, 'Ÿ', 'Y'))

    return df

def clean_special_character(df, column):
    df = df.withColumn(column,translate(column , '\";:\}\{\~\/', ''))
    return df