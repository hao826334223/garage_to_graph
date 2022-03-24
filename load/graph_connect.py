import os
import yaml
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

with open("config.yaml", "r") as stream:
    try:
        conf= yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

# connect graph
connect = DriverRemoteConnection(f'ws://{conf["private_ip"]}:{conf["port"]}/gremlin', 'g')
g = traversal().withRemote(connect)
