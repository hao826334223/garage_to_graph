from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# from utils.log import logger
from load.utils.log import logger

class MySQLHelper:
    """MySQL object"""

    def __init__(self):
        self.engine = None
        self.session = None
        self.host = None
        self.port = None
        self.user = None
        self.pwd = None
        self.database = None

    def connect(self):
        """
        Connect to MySQL
        """
        logger.info("Connect MYSQL server")
        self.engine = create_engine(
            f"mysql+pymysql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?charset=utf8mb4")
        session = sessionmaker(bind=self.engine)  # Create session type
        self.session = session()  # Crete session object

    def close(self):
        """Close connect"""
        logger.info("Close the connection to the MYSQL server")
        self.session.close()

    def fetchone(self, sql):
        """
        :param sql: sql statement, type str
        :return: query result, type list
        """
        result = self.session.execute(sql)
        return result.fetchone()

    def fetchall(self, sql):
        """
        :param sql: sql statement, type str
        :return: query result, type list
        """
        result = self.session.execute(sql)
        return result.fetchall()


mysql_instance = MySQLHelper()    # Object instantiation


def get_tables():
    """Get all tables of the database"""
    sql = f"select table_name from information_schema.tables where table_schema='{mysql_instance.database}'"
    return [value[0] for value in mysql_instance.fetchall(sql)]


def get_table_references(table):
    """Get the primary key, foreign key and constraint of the table"""
    sql = f"""
            select
            TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
            from INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            where CONSTRAINT_SCHEMA ='{mysql_instance.database}' AND
            REFERENCED_TABLE_NAME = '{table}'
            """
    return mysql_instance.fetchall(sql)
