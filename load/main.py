import os
import yaml
import sys
sys.path.insert(0, '..')

# from utils.util import get_local_time, get_files_path, get_root_dir
# from utils.log import logger
# from utils.data_loader import TableDataSet, VertexDataSet
# from gremlin import gremlin_instance
# from mysql import mysql_instance, get_tables, get_table_references


# from utils.util import get_local_time, get_files_path, get_root_dir
# from utils.log import logger
# from utils.data_loader import TableDataSet, VertexDataSet
# from gremlin import gremlin_instance
# from mysql import mysql_instance, get_tables, get_table_references

# from load.utils.util import get_local_time, get_files_path, get_root_dir
from gremlin.gremlin import gremlin_instance
from load.utils.log import logger
from load.utils.data_loader import TableDataSet, VertexDataSet
from mysql.mysql import mysql_instance, get_tables, get_table_references



def add_vertex_mysql():
    """Add mysql data into gremlin"""
    for table in get_tables():
        logger.info(f"Star to add vertex from {table} table")
        for item in TableDataSet(table):    # Traverse add vertex
            gremlin_instance.add_vertex(table, item)

        logger.info("Succeeded")


def add_edge_mysql():
    """Add mysql data into gremlin"""

    for table in get_tables():  # Traversing tables in the database
        for table_name, column, constraint_name, refferenced_table, refferenced_column in get_table_references(table):

            if table_name != refferenced_table:  # Filter inner join table
                logger.info(f"Start to add edge of label name {constraint_name}")
                for vertex_dict in VertexDataSet(table_name):

                    print("circulation")
                    #  The query is slow, taking 9 seconds
                    to_vertex_list = gremlin_instance.query_vertex(label=refferenced_table, properties=[
                        {refferenced_column: vertex_dict['properties'][column]}])  # query to_vertex


                    if to_vertex_list:
                        for to_vertex in to_vertex_list:
                            gremlin_instance.add_edge(constraint_name, vertex_dict['id'], to_vertex.id)  # add edge



env_dict = os.environ   # Get environment variables

# # Connect to Gremlin Server
# gremlin_instance.host = env_dict.get('GREMLIN_HOST') if env_dict.get('GREMLIN_HOST') else click.prompt(
#     "Enter Gremlin Host")
# gremlin_instance.port = env_dict.get('GREMLIN_PORT') if env_dict.get('GREMLIN_PORT') else click.prompt(
#     'Enter Gremlin Port')
# gremlin_instance.connect()

# # Connect to MySQL server
# mysql_instance.host = env_dict.get('MYSQL_HOST') if env_dict.get('MYSQL_HOST') else click.prompt("Enter MYSQL Host")
# mysql_instance.port = env_dict.get('MYSQL_PORT') if env_dict.get('MYSQL_PORT') else click.prompt("Enter MYSQL Port")
# mysql_instance.user = env_dict.get('MYSQL_USER') if env_dict.get('MYSQL_USER') else click.prompt("Enter MYSQL User")
# mysql_instance.pwd = env_dict.get('MYSQL_PASSWORD') if env_dict.get('MYSQL_PASSWORD') else click.prompt("Enter MYSQL Password")
# mysql_instance.database = env_dict.get('MYSQL_DATABASE') if env_dict.get('MYSQL_DATABASE') else click.prompt("Enter MYSQL Database")


with open("config.yaml", "r") as stream:
    try:
        conf = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

conf["private_ip"] = '10.0.0.40'
# Connect to Gremlin Server
gremlin_instance.host = conf["GREMLIN_HOST"]
gremlin_instance.port = conf["GREMLIN_PORT"]
gremlin_instance.connect()

# Connect to MySQL server
mysql_instance.host = conf["MYSQL_HOST"]
mysql_instance.port = conf["MYSQL_PORT"]
mysql_instance.user = conf["MYSQL_USER"]
mysql_instance.pwd = conf["MYSQL_PASSWORD"]
mysql_instance.database = conf["MYSQL_DATABASE"]


# # Connect to Gremlin Server
# gremlin_instance.host = '10.0.0.40'
# gremlin_instance.port = '8182'
# gremlin_instance.connect()
#
# # Connect to MySQL server
# mysql_instance.host = '10.0.0.40'
# mysql_instance.port = '6603'
# mysql_instance.user = 'root'
# mysql_instance.pwd = 'pwd'
# mysql_instance.database = 'db'

mysql_instance.connect()

try:
    # Add vertex
    logger.info("Add {} vertex".format(get_tables()))
    add_vertex_mysql()

    # Add edge
    logger.info("Multiple labels that already exist for Gremlin Server include: {}".format(get_tables()))
    add_edge_mysql()
    logger.info("Succeeded")

    # # Export XML
    # logger.info("Start to export xml")
    # if not os.path.exists('export'):
    #     os.makedirs('export')
    #
    # file_name = get_local_time() + '.xml'
    # gremlin_instance.export_to_graphml(os.path.join('export', file_name))
    # gremlin_instance.close()
    # mysql_instance.close()
    #
    # # Upload file to bucket
    # file_path, file_name = get_files_path(os.path.join(get_root_dir(), 'export'), 'xml')
    # while len(file_path) == 0:
    #     pass
    # logger.info("Succeeded")

except Exception as e:
    logger.error(e)
    gremlin_instance.close()
    mysql_instance.close()
    sys.exit()
