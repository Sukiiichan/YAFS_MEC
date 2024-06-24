import yafs.application
from typing import List
import networkx as nx

class AGNode:
    def __init__(self, module_id, module_type, consumptions, parent_nodes: list, packet_size_to_parent_dict: dict,
                 child_nodes: list):
        self.module_id = module_id
        # module, source, user
        self.module_type = module_type
        self.consumptions = consumptions
        # self.parent_node = parent_node
        self.parent_nodes = parent_nodes
        self.packet_size_to_parent_dict = packet_size_to_parent_dict
        self.child_nodes = child_nodes

    def append_child(self, child):
        self.child_nodes.append(child)

    def append_parent(self, parent):
        self.parent_nodes.append(parent)

    def __str__(self):
        parent_node_ids = "NULL"
        packet_sizes_to_parent = "NULL"
        # parents can be multiple
        if self.parent_nodes:
            parent_node_ids = ','.join([n.module_id for n in self.parent_nodes])
            packet_sizes_to_parent = ','.join(
                [str(self.packet_size_to_parent_dict[n.module_id]) for n in self.parent_nodes])
        child_node_repr = "NULL"
        if self.child_nodes:
            child_node_repr = ",".join([f"<{n.module_id}>" for n in self.child_nodes])
        return f"<{parent_node_ids}>,s={packet_sizes_to_parent}\n" \
               f"<AGNode {self.module_id}, c={self.consumptions}, t={self.module_type}>\n" \
            + child_node_repr


class AppGraph:
    def __init__(self, app_name, node_set: List[dict], edge_set: List[dict]):
        self.app_name = app_name

        self.root_node = None

        """
        Node: dict {"module_id":1, "consumptions": 100}
        """
        self._raw_node_set = node_set
        """
        Edge: dict {"parent_id":1, "child_id": 2, "packet_size":10}
        """
        self._raw_edge_set = edge_set
        self.node_obj_set = []
        self.nx_graph = None
        self.construct_nx_graph()
        self.construct()

    def construct_nx_graph(self):
        self.nx_graph = nx.DiGraph()
        for n in self._raw_node_set:
            self.nx_graph.add_node(n['module_id'], consumptions=n['consumptions'])
        for e in self._raw_edge_set:
            self.nx_graph.add_edge(e["child_id"], e["parent_id"], packet_size=e["packet_size"])

    def create_node_obj_and_append_to_set(self, node_data: dict) -> AGNode:
        node = AGNode(node_data["module_id"], node_data["type"], node_data["consumptions"], [], {}, [])
        self.node_obj_set.append(node)
        return node

    def find_module_type_by_id(self, module_id: str):
        for n in self._raw_node_set:
            if n["module_id"] == module_id:
                return n["type"]
        raise ValueError(f"module id {module_id} not found")

    def find_user_id(self):
        for n in self._raw_node_set:
            if n["type"] == "user":
                return n["module_id"]
        raise ValueError(f"no user found")

    def find_source_ids(self):
        source_ids = []
        for n in self._raw_node_set:
            if n["type"] == "source":
                source_ids.append(n["module_id"])
        return source_ids

    def find_source_edges(self):
        source_edges = []
        for e in self._raw_edge_set:
            if e["child_id"] in self.find_source_ids():
                source_edges.append(e)
        return source_edges

    def find_sink_edges(self):
        sink_edges = []
        for e in self._raw_edge_set:
            if e["parent_id"] == self.find_user_id():
                sink_edges.append(e)
        return sink_edges

    def find_module_consumption_by_id(self, module_id: str):
        for n in self._raw_node_set:
            if n["module_id"] == module_id:
                return n["consumptions"]
        raise ValueError(f"module id {module_id} not found")

    def find_source_edge_id_set_by_source_id(self, source_id: str):
        source_edge_id_set = []
        for e in self._raw_edge_set:
            if e["child_id"] == source_id:
                # return e["edge_id"]
                source_edge_id_set.append(e["edge_id"])
        if len(source_edge_id_set) == 0:
            raise ValueError(f"edge not found")
        return source_edge_id_set

    def find_edge_by_src_and_dst_id(self, src_module_id, dst_module_id):
        for e in self._raw_edge_set:
            if e["child_id"] == src_module_id and e["parent_id"] == dst_module_id:
                return e
            elif e["parent_id"] == src_module_id and e["child_id"] == dst_module_id:
                return e
        raise ValueError(f"edge not found")

    def construct(self):
        # first convert node_set to a dict
        raw_node_dict = {}
        for n in self._raw_node_set:
            raw_node_dict[n["module_id"]] = n
        obj_node_dict = {}
        # then traverse all edges to create node obj and construct graph
        for e in self._raw_edge_set:
            parent_id = e["parent_id"]
            child_id = e["child_id"]
            packet_size = e["packet_size"]
            edge_id = e["edge_id"]

            if parent_id not in obj_node_dict:
                node = self.create_node_obj_and_append_to_set(raw_node_dict[parent_id])
                obj_node_dict[parent_id] = node
            if child_id not in obj_node_dict:
                node = self.create_node_obj_and_append_to_set(raw_node_dict[child_id])
                obj_node_dict[child_id] = node

            child_node = obj_node_dict[child_id]
            parent_node = obj_node_dict[parent_id]
            child_node.append_parent(parent_node)
            parent_node.append_child(child_node)
            # child_node.packet_size_to_parent = packet_size
            child_node.packet_size_to_parent_dict[parent_id] = packet_size

        # find the root and set self.root_node
        node = next(iter(obj_node_dict.values()))
        while node.parent_nodes:
            node = node.parent_nodes[0]  # DFS
        self.root_node = node

    def debug_print(self):
        bfs = [self.root_node]
        while bfs:
            n = bfs.pop()
            print(n)
            print("----------")
            for n_n in n.child_nodes:
                bfs.append(n_n)

    def convert_to_yafs_app(self, emission_interval=None) -> yafs.application.Application:
        """returns the YAFS application object"""

        app = yafs.application.Application(name=self.app_name)
        modules = []

        for node in self._raw_node_set:
            if node["type"] == "source":
                modules.append({node["module_id"]: {"Type": yafs.application.Application.TYPE_SOURCE}})
            elif node["type"] == "module":
                modules.append({node["module_id"]: {"RAM": 10, "Type": yafs.application.Application.TYPE_MODULE}})
            elif node["type"] == "user":
                modules.append({node["module_id"]: {"Type": yafs.application.Application.TYPE_SINK}})

        app.set_modules(modules)

        # set messages and add service modules
        source_messages = []
        service_messages = []
        for edge in self._raw_edge_set:
            t_instructions = self.find_module_consumption_by_id(edge["parent_id"])
            t_message = yafs.Message(edge["edge_id"], edge["child_id"], edge["parent_id"], instructions=t_instructions,
                                     bytes=edge["packet_size"])
            if self.find_module_type_by_id(edge["child_id"]) == "source":
                app.add_source_messages(t_message)
                source_messages.append(t_message)
            else:
                service_messages.append(t_message)

        for node in self._raw_node_set:
            t_in_messages = []
            # t_out_message = None
            t_out_messages = []
            if node["type"] == "module":
                for t_service_message in service_messages:
                    if t_service_message.dst == node["module_id"]:
                        t_in_messages.append(t_service_message)
                    elif t_service_message.src == node["module_id"]:
                        # t_out_message = t_service_message
                        t_out_messages.append(t_service_message)

                    for t_source_message in source_messages:
                        if t_source_message.dst == node["module_id"]:
                            t_in_messages.append(t_source_message)

                app.add_service_module(node["module_id"], t_in_messages, t_out_messages,
                                           yafs.application.fractional_selectivity, threshold=1.0)
            elif node["type"] == "source":
                distribution = yafs.distribution.deterministic_distribution(name="Deterministic",
                                                                            time=emission_interval)
                # distribution = yafs.distribution.oneoffDistribution(name="Deterministic", time=10)
                for t_service_message in service_messages:
                    if t_service_message.src == node["module_id"]:
                        t_out_messages.append(t_service_message)
                app.add_service_source(node["module_id"], distribution, message_out_list=t_out_messages)
            # for t_in_message in t_in_messages:
            #     for t_out_message in t_out_messages:
            #         app.add_service_module(node["module_id"], t_in_message, t_out_message,
            #                                yafs.application.fractional_selectivity, threshold=1.0)
            #         #  check the fraction?

        return app


if __name__ == "__main__":
    # testing
    raw_node_set = [
        {"module_id": "U1", "type": "user", "consumptions": 60},
        {"module_id": "O1", "type": "module", "consumptions": 20},
        {"module_id": "O2", "type": "module", "consumptions": 40},
        {"module_id": "O3", "type": "module", "consumptions": 40},
        {"module_id": "O4", "type": "module", "consumptions": 80},
        {"module_id": "D1", "type": "source", "consumptions": 0},
        {"module_id": "D2", "type": "source", "consumptions": 0},
    ]
    raw_edge_set = [
        {"edge_id": "Result", "parent_id": "U1", "child_id": "O1", "packet_size": 4},
        {"edge_id": "Intermediate4", "parent_id": "O1", "child_id": "O2", "packet_size": 8},
        {"edge_id": "Intermediate3", "parent_id": "O1", "child_id": "O3", "packet_size": 10},
        {"edge_id": "Intermediate2", "parent_id": "O2", "child_id": "O4", "packet_size": 20},
        {"edge_id": "Data1", "parent_id": "O3", "child_id": "D1", "packet_size": 8},
        {"edge_id": "Data2", "parent_id": "O4", "child_id": "D2", "packet_size": 2}
    ]
    g = AppGraph("test_app", raw_node_set, raw_edge_set)
    g.debug_print()
