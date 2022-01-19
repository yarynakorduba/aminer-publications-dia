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