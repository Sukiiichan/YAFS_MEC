import random
import networkx as nx
import argparse
from pathlib import Path
import time
import numpy as np

from yafs.core import Sim
from yafs.application import Application, Message
from yafs.population import *

from yafs.topology import Topology

from yafs.stats import Stats
from mec_simulations import mec_environment
from mec_simulations.simpleSelection import MinimunPath
from yafs.distribution import deterministic_distribution
from yafs.application import fractional_selectivity
import placement_collection, mec_application

def create_toy_mec_app():
    # manually configure the application
    app_name = "vid_case"
    raw_node_set = [
        {"module_id": "user", "type": "user", "consumptions": 10},
        {"module_id": "service_a", "type": "module", "consumptions": 20},
        {"module_id": "service_b", "type": "module", "consumptions": 20},
        {"module_id": "service_c", "type": "module", "consumptions": 30},
        {"module_id": "service_d", "type": "module", "consumptions": 20},
        {"module_id": "data_1", "type": "source", "consumptions": 0},
        {"module_id": "data_2", "type": "source", "consumptions": 0},
    ]
    raw_edge_set = [
        {"edge_id": "M.1.A", "parent_id": "service_a", "child_id": "data_1", "packet_size": 100},
        {"edge_id": "M.2.C", "parent_id": "service_c", "child_id": "data_2", "packet_size": 50},
        {"edge_id": "M.A.B", "parent_id": "service_b", "child_id": "service_a", "packet_size": 100},
        {"edge_id": "M.A.C", "parent_id": "service_c", "child_id": "service_a", "packet_size": 100},
        {"edge_id": "M.B.D", "parent_id": "service_d", "child_id": "service_b", "packet_size": 100},
        {"edge_id": "M.C.D", "parent_id": "service_d", "child_id": "service_c", "packet_size": 100},
        {"edge_id": "M.D.U", "parent_id": "user", "child_id": "service_d", "packet_size": 1000},
    ]

    g = mec_application.AppGraph(app_name, raw_node_set, raw_edge_set)
    yafs_app = g.convert_to_yafs_app()

    return yafs_app

def create_toy_app():
    a = Application(name="toy_app")
    a.set_modules([{"data_1": {"Type": Application.TYPE_SOURCE}},
                   {"data_2": {"Type": Application.TYPE_SOURCE}},
                   {"service_a": {"RAM": 100, "Type": Application.TYPE_MODULE}},
                   {"service_b": {"RAM": 100, "Type": Application.TYPE_MODULE}},
                   {"service_c": {"RAM": 100, "Type": Application.TYPE_MODULE}},
                   {"service_d": {"RAM": 100, "Type": Application.TYPE_MODULE}},
                   {"user": {"Type": Application.TYPE_SINK}}
                   ])

    m_1_a = Message("M.1.A", "data_1", "service_a", instructions=20, bytes=100)
    m_2_c = Message("M.2.C", "data_2", "service_c", instructions=30, bytes=50)
    m_a_b = Message("M.A.B", "service_a", "service_b", instructions=20, bytes=100)
    m_a_c = Message("M.A.C", "service_a", "service_c", instructions=30, bytes=100)
    m_b_d = Message("M.B.D", "service_b", "service_d", instructions=20, bytes=100)
    m_c_d = Message("M.C.D", "service_c", "service_d", instructions=20, bytes=100)
    m_d_u = Message("M.D.U", "service_d", "user", instructions=10, bytes=1000)

    """
    Defining which messages will be dynamically generated # the generation is controlled by Population algorithm
    """
    a.add_source_messages(m_1_a)
    a.add_source_messages(m_2_c)

    # MODULE SOURCES: only periodic messages
    dDistribution = deterministic_distribution(name="Deterministic", time=100)
    a.add_service_source("data_1", dDistribution, [m_1_a])
    a.add_service_source("data_2", dDistribution, [m_2_c])

    """
    MODULES/SERVICES: Definition of Generators and Consumers (AppEdges and TupleMappings in iFogSim)
    """
    # MODULE SERVICES
    a.add_service_module("service_a", [m_1_a], [m_a_b, m_a_c], fractional_selectivity, threshold=1.0)
    a.add_service_module("service_b", [m_a_b], [m_b_d], fractional_selectivity, threshold=1.0)
    a.add_service_module("service_c", [m_a_c, m_2_c], [m_c_d], fractional_selectivity, threshold=1.0)
    a.add_service_module("service_d", [m_b_d, m_c_d], [m_d_u], fractional_selectivity, threshold=1.0)

    return a


def create_toy_topology():
    server_1 = mec_environment.Server(server_id='S1', server_name="Server_1", mdc_id="M1", mem=50, frequency=100,
                                      device_factor=0.8)
    server_2 = mec_environment.Server('S2', "Server_2", "M1", 50, 100, 0.7)
    server_3 = mec_environment.Server('S3', "Server_3", "M1", 50, 100, 100, 0.5)

    server_4 = mec_environment.Server('S4', "Server_4", "M2", 60, 100, 100, 0.5)
    server_5 = mec_environment.Server('S5', "Server_5", "M2", 50, 100, 100, 0.6)
    user_device_1 = mec_environment.UserDevice('user', "user", "M2")

    data_source_1 = mec_environment.DataSource('data_1', "data_1", "M3")

    server_6 = mec_environment.Server('S6', "Server_6", "M3", 50, 100, 100, 0.6)

    data_source_2 = mec_environment.DataSource('data_2', "data_2", "M4")
    server_7 = mec_environment.Server('S7', "Server_7", "M4", 150, 100, 100, 1.1)
    server_8 = mec_environment.Server('S8', "Server_8", "M4", 50, 100, 100, 0.6)
    server_9 = mec_environment.Server('S9', "Server_9", "M4", 50, 100, 100, 0.6)

    mdc_1 = mec_environment.MDC(mdc_id='M1', mdc_name='MDC_1', mdc_links=[("M2", 10000), ("M4", 5000)],
                                servers=[server_1, server_2, server_3], energy_stored=0, battery_capacity=100,
                                charging_rate=10)
    mdc_2 = mec_environment.MDC('M2', 'MDC_2', [("M1", 10000), ("M3", 8000)], [server_4, server_5], 0, 100, 10, [],
                                [user_device_1])
    mdc_3 = mec_environment.MDC('M3', 'MDC_3', [("M2", 8000), ("M4", 10000)], [server_6], 0, 100, 10, [data_source_1],
                                [])
    mdc_4 = mec_environment.MDC('M4', 'MDC_4', [("M1", 5000), ("M3", 10000)], [server_7, server_8, server_9], 0, 100,
                                10,
                                [data_source_2], [])

    mec = mec_environment.MEC([mdc_1, mdc_2, mdc_3, mdc_4])

    topology_json, yafs_entity_id_name_map, server_info_map = mec.convert_to_yafs_topology()

    return mec, topology_json, yafs_entity_id_name_map, server_info_map


def main(simulated_time):
    folder_results = Path("results/")
    folder_results.mkdir(parents=True, exist_ok=True)
    folder_results = str(folder_results) + "/"

    # yafs_app = create_toy_app()
    yafs_app = create_toy_mec_app()

    t = Topology()

    mec, topology_json, yafs_entity_id_name_map, server_info_map = create_toy_topology()
    t.load(topology_json)
    random_result = {}
    for module in yafs_app.get_pure_modules():
        selected_server_id = random.choice(list(server_info_map.keys()))
        random_result[module] = server_info_map[selected_server_id]
    # create random results

    placement = placement_collection.CustomStaticPlacement(name="Placement")
    placement.preload_static_result(random_result)

    pop = Statical("Statical")

    pop.set_sink_control({"model": "user", "number": 1, "module": yafs_app.get_sink_modules()})

    dDistribution = deterministic_distribution(name="Deterministic", time=100)
    pop.set_src_control(
        {"model": "data_1", "number": 1, "message_out_list": [yafs_app.get_message("M.1.A")],
         "distribution": dDistribution})
    pop.set_src_control(
        {"model": "data_2", "number": 1, "message_out_list": [yafs_app.get_message("M.2.C")],
         "distribution": dDistribution})

    stop_time = simulated_time
    s = Sim(t, default_results_path=folder_results + "sim_trace")
    # print result traces
    selectorPath = MinimunPath()
    s.allocate_resources(yafs_app, random_result)
    s.deploy_app2(yafs_app, placement, pop, selectorPath)

    # s.run(stop_time, test_initial_deploy=True)
    s.run(stop_time, show_progress_monitor=False)
    s.print_debug_assignaments()


if __name__ == '__main__':
    import logging.config
    import os

    logging.config.fileConfig(os.getcwd() + '/logging.ini')
    start_time = time.time()
    main(simulated_time=1000)

    print("\n--- %s seconds ---" % (time.time() - start_time))
    print("Simulation Done!")
