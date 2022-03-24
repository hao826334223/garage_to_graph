# import numpy as np
# import pandas as pd
# import itertools
# import warnings
# warnings.filterwarnings("ignore")

import sys
sys.path.insert(0, '..')

# from steps_in_df.df_step1_1_synth_data.gen_data import gen_data
# from util.car import Car

# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt
# import networkx as nx


## pip install gremlinpython

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# gremlin_conn = {
#     "Host": "localhost",
#     "Port": "8182",
# }
gremlin_conn = {
    "Host": "10.0.0.40",
    "Port": "8182",
}
print("1. Connect to Gremlin Server")
for key in gremlin_conn.keys():
    if gremlin_conn[key] is None or len(gremlin_conn[key]) == 0:
        print("Enter {}".format(key))
        gremlin_conn[key] = input()

try:
    conn_gremlin = DriverRemoteConnection('ws://{}:{}/gremlin'.format(gremlin_conn["Host"], gremlin_conn["Port"], 'g'))
    g = traversal().withRemote(conn_gremlin)
except Exception as e:
    print("Connect to Gremlin Server failed, {}".format(e))
    exit()

# print(g.V().limit(1).elementMap().toList())

print(g.V().count().next())
# exit()
# i = 0
# for node in g.V().elementMap():
#     if i < 10:
#         print(node)
#     else:
#         break
#     i += 1

# G = nx.DiGraph()
# sg = g.V().subgraph('sg').cap('sg').next()
# print(type(sg))
# exit()
# for e in sg['@value']['edges']:
#     G.add_edge(e.outV.id, e.inV.id, elabel=e.label)

# print(G.nodes())
# print(G.edges())

# g.io("graph.xml").write().iterate()  # save to graph.xml

conn_gremlin.close()
