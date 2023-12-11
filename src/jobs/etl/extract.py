import os
# from pathlib import Path

def read_file(spark_session, file_name, cols_name=None, format="csv", delimiter="::",):
    if cols_name is None:
        raise Exception("cols_name argument is empty")
    pwd = os.getcwd()
    file_fullpath = os.path.join(pwd, "data", file_name)
    
    return spark_session.read.format(format).option("delimiter", delimiter).load(file_fullpath).toDF(*cols_name)

