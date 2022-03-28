import os
import sys
# sys.path.insert(0, '..')
from load.utils.log import logger
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# from utils.log import logger

# from load.utils.log import logger

class GremlinHelper:
    """Gremlin object"""

    def __init__(self):
        self.conn = None
        self.graph = None
        self.host = None
        self.port = None

    def connect(self):
        """Connect to Gremlin server"""
        logger.info("Connect Gremlin Server")
        self.conn = DriverRemoteConnection(f'ws://{self.host}:{self.port}/gremlin', 'g')
        self.graph = traversal().withRemote(self.conn)

    def close(self):
        """Close connect of Gremlin server"""
        logger.info("Close the connection to the gremlin server")
        self.conn.close()

    def add_vertex(self, label, properties=None):
        """
        add vertext
        :param label: labe, type: str
        :param properties: property dict, like {'p1': 'value1', 'p2':'value2'}
        :return: vertex, Vertex(id, labe)
        """

        vert = self.graph.addV(label)
        if properties:
            for key in properties.keys():
                vert.property(key, str(properties[key]))
            # vert.property(key, properties.get(key))

        return vert.next()

    def add_edge(self, label, v_from, v_to, properties=None):
        """
        add edge
        :param label: label, type: str
        :param v_from: long vertex id or Vertex(id, label) of from
        :param v_to: long vertex id or Vertex(id, label) of to
        :param properties: properties: property dict, like {'p1': 'value1', 'p2': 'value2'}
        :return: None
        """
        # print(f"label:{label}")
        # print(f"V_from:{v_from}")
        # print(f"v_to:{v_to}")
        if isinstance(v_from, int):
            v_from = self.graph.V().hasId(v_from).next()
        if isinstance(v_to, int):
            v_to = self.graph.V().hasId(v_to).next()
        edge = self.graph.V(v_from).addE(label).to(v_to)
        if properties:
            for key in properties.keys():
                edge.property(key, properties.get(key))
        edge.next()

    def query_vertex(self, v_id=None, label=None, properties=None):
        """
        query graph vertex (value) list
        :param v_id: long vertex id or Vertex(id, label)
        :param label: label, type: str
        :param properties: property list, like ['p1', 'p2', {'p3': 'value'}]
        :return: vertex list
        """
        if isinstance(v_id, int):
            print(f"v_id:{v_id}")
            v_id = self.graph.V().hasId(v_id).next()
        travel = self.graph.V(v_id) if v_id else self.graph.V()
        if label:
            travel = travel.hasLabel(label)
        if properties:
            for p in properties:
                if isinstance(p, dict):
                    key = list(p.keys())[0]
                    travel = travel.has(key, p.get(key))
                else:
                    travel = travel.has(p)
        return travel.toList()

    def query_vertex_value(self, v_id=None, label=None, properties=None):
        """
        query graph vertex (value) list
        :param v_id: long vertex id or Vertex(id, label)
        :param label: label, type: str
        :param properties: property list, like ['p1', 'p2', {'p3': 'value'}]
        :return: vertex value list
        """
        if isinstance(v_id, int):
            v_id = self.graph.V().hasId(v_id).next()
        travel = self.graph.V(v_id) if v_id else self.graph.V()
        if label:
            travel = travel.hasLabel(label)
        if properties:
            for p in properties:
                if isinstance(p, dict):
                    key = list(p.keys())[0]
                    travel = travel.has(key, p.get(key))
                else:
                    travel = travel.has(p)
        return travel.valueMap().toList()

    def query_edge(self, e_id=None, label=None, properties=None):
        """
        query graph edge value list
        :param e_id: edge id, type str
        :param label: label, type: str
        :param properties: property list, like ['p1', 'p2', {'p3': 'value'}]
        :return: valueMap list
        """
        travel = self.graph.E(e_id) if e_id else self.graph.E()
        if label:
            travel = travel.hasLabel(label)
        if properties:
            for p in properties:
                if isinstance(p, dict):
                    key = list(p.keys())[0]
                    travel = travel.has(key, p.get(key))
                else:
                    travel = travel.has(p)
        return travel.valueMap().toList()

    def query_near_vertex(self, v_id):
        """
        query near vertices of vertex
        :param v_id: v_id: long vertex id or Vertex(id, label)
        :return: vertex list
        """
        if isinstance(v_id, int):
            v_id = self.graph.V().hasId(v_id).next()
        result = []
        out_v = self.graph.V(v_id).out().toList()
        in_v = self.graph.V(v_id).in_().toList()
        result.extend(out_v)
        result.extend(in_v)
        return result

    def vertex_to_dict(self, vertex):
        """
        transfer Vertex's info to dict
        :param vertex: vertex, Vertex(id, label)
        :return: vertex info dict
        """
        properties = self.graph.V(vertex).valueMap().toList()[0]
        for key in properties.keys():
            properties[key] = properties.get(key)[0]
        return {
            'id': vertex.id,
            'label': vertex.label,
            'properties': properties
        }

    def check_vertex_in_graph(self, vertex_dict):
        """
        check a vertex whether in graph
        :param vertex_dict: vertex dict, like {'label': 'value1', 'properties': {'p1': 'v1', ...}}
        :return: None or Vertex(id,label)
        """
        label = vertex_dict.get('label')
        properties = vertex_dict.get('properties')
        travel = self.graph.V()
        if label:
            travel = travel.hasLabel(label)
        if properties:
            for k in properties.keys():
                travel = travel.has(k, properties.get(k))
        if travel.hasNext():
            return travel.next()
        return None

    def export_to_graphml(self, file_name):
        """
        Export an XML (graphml) file to server.
        This means that if you use remote mode, the exported files are on a remote server
        :param file_full_path: File name when exporting, like 'my-graph.xml', './export/my-graph.xml'
        """
        try:
            self.graph.io(file_name).write().iterate()
            logger.info("Export succeeded")
        except Exception as e:
            logger.error(f"Export failed. {e}")
            sys.exit()

    def import_from_graphml(self, file_full_path):
        """
        Import an XML (graphml) file from server.
        The file must be accessible to the server.
        :param file_full_path: Full path to the file, like 'my-graph.xml', './import/my-graph.xml'
        """
        try:
            if os.path.exists(file_full_path):
                try:
                    self.graph.io(file_full_path).read().iterate()
                    logger.info("Import succeeded")
                except Exception as e:
                    logger.error(f"Import failed. {e}")
            else:
                logger.error(file_full_path + " is not exist")
        except Exception as e:
            logger.error(e)
            sys.exit()


gremlin_instance = GremlinHelper()  # Object instantiation
