import pandas as pd

import sys
sys.path.insert(0, '..')

# from gremlin import gremlin_instance
# from mysql import mysql_instance
# from utils.log import logger
from gremlin.gremlin import gremlin_instance
from mysql.mysql import mysql_instance
from load.utils.log import logger



class TableDataSet:
    """Processing table data of MySQL"""
    def __init__(self, table):
        self.table = table
        self.df = self.load()

    def load(self):
        """Load table data of MySQL"""
        logger.info(f"Load table of {self.table}")
        df = pd.read_sql_query(f'select * from {self.table}', mysql_instance.engine, parse_dates=['str'])
        df = df.fillna('')  # Replace empty field
        return df

    def __iter__(self):
        for i in range(len(self.df)):
            yield self.df.loc[i].to_dict()  # return dict type


class VertexDataSet:
    """Processing vertex list"""

    def __init__(self, label):
        self.label = label
        self.vertex_list = self.get_vertex_list()

    def get_vertex_list(self):
        """Query vertex"""
        return gremlin_instance.query_vertex(label=self.label)

    def __iter__(self):
        for vertex in self.vertex_list:
            yield gremlin_instance.vertex_to_dict(vertex)   # return dict type
