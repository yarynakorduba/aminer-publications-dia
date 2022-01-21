"""This file contains the helper functions """
import glob
import shutil
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

def createDFFromFileAndSchema(sparkSession, filePath, schemaPath, delimiter=',', header=True):
    """
    This function is used to write the csv into a schema
    :param sparkSession:
    :param filePath: the path of the csv file
    :param schemaPath: the path of the schema
    :return: dataframe
    """
    print(f"File path: {filePath}, schema path: {schemaPath}")
    # load schema
    dtypes = pd.read_csv(schemaPath).to_records(index=False).tolist()
    print(f"Types from schema: {dtypes}")
    fields = [StructField(dtype[0], globals()[f'{dtype[1]}Type']()) for dtype in dtypes]
    schema = StructType(fields)
    created_df = sparkSession.read \
        .option('header', 'true') \
        .option('delimiter', delimiter) \
        .csv(filePath, header=header, schema=schema)
    return created_df

def saveDFIntoCSVFolder(df, folderName, pathToFolder):
    # Save data to csv file
    df.coalesce(1) \
        .write.format("com.databricks.spark.csv") \
        .option("header", "true") \
        .save(f'{pathToFolder}{folderName}')

def moveFileToCorrectFolder(folderName, pathToFolder):
    filename = glob.glob(f'{pathToFolder}{folderName}/*.csv')[0]
    shutil.move(filename, f'{pathToFolder}{folderName}.csv')


def clean_special_letters(df, column):
    """
    This function is used to clean columns of the dataframe from special letters
    :param df: dataframe
    :param column: column_name
    :return: cleaned dataframe
    """
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
    """
    This function is used to clean columns of the dataframe from special characters
    :param df: dataframe
    :param column: column_name
    :return: cleaned dataframe
    """
    df = df.withColumn(column,translate(column , '\";:\}\{\~\/%)(&#\'$?`-.', ''))
    return df

def clean_numbers_from_text(df, column):
    """
    This function is used to clean specific columns of the dataframe from numbers
    :param df: dataframe
    :param column: column_name
    :return: cleaned dataframe
    """
    df = df.withColumn(column, translate(column , '0123456789', ''))
    return df

