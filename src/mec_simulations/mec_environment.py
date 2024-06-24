from copy import deepcopy

DEFAULT_MDC_BW = 2000
DEFAULT_CLOUD_BW = 10000
DEFAULT_EDGE_PR = 1
DEFAULT_CLOUD_PR = 8


class Server(object):

    def __init__(self, server_id, server_name, mdc_id, mem, frequency, device_factor,
                 num_of_slot=1,
                 availability=1):
        self.server_id = server_id
        self.server_name = server_name
        self.mdc_id = mdc_id
        # self.capacity = capacity # replaced by frequency
        self.mem = mem
        self.frequency = frequency
        self.device_factor = device_factor
        self.availability = availability
        self.num_of_slot = num_of_slot
        # self.power = power
        # self.power_min = power_min
        # self.power_max = power_max

    # def calculate_energy(self, active_slots, max_slots, time_period):
    #     # dynamic energy consumption
    #     operating_freq = self.frequency * active_slots / max_slots
    #     return self.device_factor * time_period * (operating_freq ** 3)
    #
    # def calculate_power(self, active_slots):
    #     # dynamic power consumption
    #     operating_freq = self.frequency * active_slots / self.num_of_slot
    #     return self.device_factor * (operating_freq ** 3)

    def get_id(self):
        return self.server_id

    def get_num_of_slot(self):
        return self.num_of_slot

    def update_num_of_slot(self, new_num_of_slot):
        self.num_of_slot = new_num_of_slot

    def update_availability(self, new_availability):
        self.availability = new_availability

    def update_mem(self, new_mem):
        self.mem -= new_mem
        # print('update memory of ', self.server_id, ' to ', self.mem)

    def __str__(self):
        return f'<Server id={self.server_id}, name={self.server_name}>'


class UserDevice(object):
    def __init__(self, user_id, user_name, location_mdc_id):
        self.user_id = user_id
        self.user_name = user_name
        self.location_mdc_id = location_mdc_id
        # the user id should be the same as the user module id


class DataSource(object):
    def __init__(self, data_source_id, data_source_name, location_mdc_id):
        self.data_source_id = data_source_id
        self.data_source_name = data_source_name
        self.location_mdc_id = location_mdc_id


class MDC(object):
    def __init__(self, mdc_id, mdc_name, mdc_links, servers, energy_stored, battery_capacity, charging_rate,
                 data_sources=None, users=None):
        self.mdc_id = mdc_id
        self.mdc_name = mdc_name
        self.mdc_links = mdc_links
        self.energy_stored = energy_stored
        self.battery_capacity = battery_capacity
        self.charging_rate = charging_rate  # charging power?
        # mdc_links: [(mdc_id, bandwidth, propagation),]
        self.servers = servers
        self.data_sources = data_sources
        if data_sources is None:
            self.data_sources = []
        self.users = users
        if users is None:
            self.users = []

    def __deepcopy__(self, memodict={}):
        new_mdc = MDC(self.mdc_id, self.mdc_name, self.mdc_links, self.servers, self.energy_stored,
                      self.battery_capacity, self.charging_rate)
        new_mdc.servers = deepcopy(self.servers)
        new_mdc.data_sources = deepcopy(self.data_sources)
        new_mdc.users = deepcopy(self.users)
        return new_mdc

    def add_user(self, user):
        self.users.append(user)

    def add_data_source(self, data_source):
        self.data_sources.append(data_source)

    def get_servers(self):
        return self.servers

    def get_links(self):
        return self.mdc_links

    def get_mdc_id(self):
        return self.mdc_id


class MEC(object):

    def __init__(self, mdc_list):
        self.mdc_list = mdc_list

    def __deepcopy__(self, memodict={}):
        new_mec = MEC(deepcopy(self.mdc_list))
        return new_mec

    def find_mdc_by_id(self, target_id):
        for mdc in self.mdc_list:
            if mdc.mdc_id == target_id:
                return mdc

    def add_user_device_to_mdc(self, user_device, mdc_id):
        for mdc in self.mdc_list:
            if mdc.mdc_id == mdc_id:
                mdc.add_user(user_device)

    def add_data_source_to_mdc(self, data_source, mdc_id):
        for mdc in self.mdc_list:
            if mdc.mdc_id == mdc_id:
                mdc.add_data_source(data_source)

    def find_mdc_servers_by_id(self, target_id):
        for mdc in self.mdc_list:
            if mdc.mdc_id == target_id:
                return mdc.get_servers()

    def find_mdc_links_by_id(self, target_id):
        for mdc in self.mdc_list:
            # print("finding mdc adj links of: " + str(target_id))
            if mdc.mdc_id == target_id:
                return mdc.get_links()
        raise ValueError

    def find_server_by_id(self, target_id):
        for mdc in self.mdc_list:
            for server in mdc.servers:
                if server.server_id == target_id:
                    return server

    def find_entity_by_id(self, target_id):
        for mdc in self.mdc_list:
            for server in mdc.servers:
                if server.server_id == target_id:
                    return server
            for user in mdc.users:
                if user.user_id == target_id:
                    return user
            for data_source in mdc.data_sources:
                if data_source.data_source_id == target_id:
                    return data_source

    def get_server_type_by_id(self, target_id):
        for mdc in self.mdc_list[:-1]:
            for server in mdc.servers:
                if server.server_id == target_id:
                    return "edge"
        return "cloud"

    def get_server_frequency_by_id(self, target_id):
        for mdc in self.mdc_list:
            for server in mdc.servers:
                if server.server_id == target_id:
                    return server.frequency

    def get_server_slot_by_id(self, target_id):
        for mdc in self.mdc_list:
            for server in mdc.servers:
                if server.server_id == target_id:
                    return server.num_of_slot

    def find_bw(self, mdc_a_id, mdc_b_id):
        cloud_dc = self.mdc_list[-1]
        if mdc_a_id == cloud_dc.mdc_id or mdc_b_id == cloud_dc.mdc_id:
            return DEFAULT_CLOUD_BW
        else:
            return DEFAULT_MDC_BW

    def find_pr(self, mdc_a_id, mdc_b_id):
        cloud_dc = self.mdc_list[-1]
        if mdc_a_id == cloud_dc.mdc_id or mdc_b_id == cloud_dc.mdc_id:
            return DEFAULT_CLOUD_PR
        else:
            return DEFAULT_EDGE_PR

    def num_of_hops(self, mdc_a_id, mdc_b_id):
        # get the number of hops between two MDCs, aka BFS shortest path len

        visited_ids = []
        distance = {}

        # start from mdc_a
        visited_ids.append(mdc_a_id)
        distance[mdc_a_id] = 0
        d_queue = [mdc_a_id]

        while len(d_queue) != 0:
            t = d_queue[0]
            d_queue.pop(0)
            for adj_mdc_id, link_bw in self.find_mdc_links_by_id(t):
                if adj_mdc_id not in visited_ids:
                    visited_ids.append(adj_mdc_id)
                    distance[adj_mdc_id] = distance[t] + 1
                    d_queue.append(adj_mdc_id)

                    if adj_mdc_id == mdc_b_id:
                        return distance[mdc_b_id]

    def locate_server(self, target_id):
        for mdc in self.mdc_list:
            for server in mdc.servers:
                if server.server_id == target_id:
                    return mdc.mdc_id

    def locate_data_source(self, target_id):
        # print("locating json_config source id", target_id)
        for mdc in self.mdc_list:
            for data_source in mdc.data_sources:
                # print(data_source.data_source_id)
                if data_source.data_source_id == target_id:
                    return mdc.mdc_id
        raise ValueError

    def locate_user_device(self, target_id):
        for mdc in self.mdc_list:
            if len(mdc.users) != 0:
                u_id_list = [u.user_id for u in mdc.users]
                if target_id in u_id_list:
                    return mdc.mdc_id
        raise ValueError

    def get_server_list(self):
        server_list = []
        for mdc in self.mdc_list:
            server_list += mdc.get_servers()
        return server_list

    def averages(self):
        t_nums = 0
        p_nums = 0
        transmission_sum = 0
        processing_sum = 0
        for mdc in self.mdc_list:
            for link in mdc.mdc_links:
                transmission_sum += link[1]
                t_nums += 1

            for server in mdc.servers:
                processing_sum += server.frequency
                p_nums += 1

        return transmission_sum / t_nums, processing_sum / p_nums

    def servers_in_range(self, mdc_id, hop):
        # find servers in the range of hops
        # 0: inside local MDC; 1: one hop, local mdc and adjacent mdc

        local_mdc = self.find_mdc_by_id(mdc_id)

        servers = []
        traced_mdc = []
        mdc_to_trace = [(0, local_mdc)]

        while len(mdc_to_trace) > 0:
            current_hop, mdc = mdc_to_trace.pop()
            if current_hop > hop:
                continue
            servers.extend(mdc.get_servers())
            traced_mdc.append(mdc)
            connected_mdcs = [self.find_mdc_by_id(k[0]) for k in mdc.mdc_links]

            for connected_mdc in connected_mdcs:
                if connected_mdc not in traced_mdc:
                    mdc_to_trace.append((current_hop + 1, connected_mdc))
        return servers

    def convert_to_yafs_topology(self):
        yafs_entity_id_name_map = {}
        server_id_entity_id_map = {}
        topology_json = {}
        topology_json["entity"] = []
        topology_json["link"] = []
        mdc_gw_id_map = {}
        entity_id_index = -1
        # central_nw_node = {"id": entity_id_index, "model": "central_nw", "mytag": "nw", "IPT": 0, "RAM": 0, "COST": 0,
        #                    "WATT": 0}
        # topology_json["entity"].append(central_nw_node)
        mdc_index = 0
        for mdc in self.mdc_list:
            if mdc_index < len(self.mdc_list) - 1:
                bw = DEFAULT_MDC_BW
                pr = DEFAULT_EDGE_PR
            else:
                bw = DEFAULT_CLOUD_BW
                pr = DEFAULT_CLOUD_PR
            mdc_index += 1

            entity_id_index += 1
            t_gw_name = mdc.mdc_id + "_gw"
            t_mdc_gateway = {"id": entity_id_index, "model": t_gw_name, "mytag": "gw", "IPT": 0, "RAM": 0, "COST": 0,
                             "WATT": 0}
            topology_json["entity"].append(t_mdc_gateway)
            mdc_gw_id_map[mdc.mdc_id] = entity_id_index
            yafs_entity_id_name_map[entity_id_index] = mdc.mdc_id
            # t_central_link = {"s": central_nw_node["id"], "d": entity_id_index, "BW": bandwidth, "PR": propagation}
            # topology_json["link"].append(t_central_link)

            entity_id_index += 1
            t_bs_name = mdc.mdc_id + "_bs"
            t_mdc_base_station = {"id": entity_id_index, "model": t_bs_name, "mytag": "bs", "IPT": 0, "RAM": 0,
                                  "COST": 0,
                                  "WATT": 0}
            topology_json["entity"].append(t_mdc_base_station)
            yafs_entity_id_name_map[entity_id_index] = t_bs_name
            t_bs_link = {"s": t_mdc_gateway["id"], "d": entity_id_index, "BW": bw, "PR": pr}
            topology_json["link"].append(t_bs_link)
            # add all inner-MDC entities and links
            for server in mdc.servers:
                entity_id_index += 1
                # t_server = {"id": entity_id_index, "model": server.server_id, "mytag": "server", "IPT": server.capacity,
                #             "RAM": server.mem, "COST": 0,
                #             "WATT": server.power}
                t_server = {"id": entity_id_index, "model": server.server_id, "mytag": "server",
                            "IPT": server.frequency,
                            "RAM": server.mem, "COST": 0,
                            "POWERmin": 0,
                            "POWERmax": 0,
                            "slot": server.num_of_slot}
                topology_json["entity"].append(t_server)
                yafs_entity_id_name_map[entity_id_index] = server.server_id
                server_id_entity_id_map[server.server_id] = entity_id_index
                t_server_link = {"s": t_mdc_gateway["id"], "d": entity_id_index, "BW": bw, "PR": 0}
                topology_json["link"].append(t_server_link)

            for data_source in mdc.data_sources:
                entity_id_index += 1
                t_data_source = {"id": entity_id_index, "model": data_source.data_source_id, "mytag": "ds", "IPT": 0,
                                 "RAM": 0, "COST": 0,
                                 "WATT": 0}
                topology_json["entity"].append(t_data_source)
                yafs_entity_id_name_map[entity_id_index] = data_source.data_source_id
                t_ds_link = {"s": t_mdc_gateway["id"], "d": entity_id_index, "BW": bw, "PR": 0}
                topology_json["link"].append(t_ds_link)

            for user in mdc.users:
                entity_id_index += 1
                t_user = {"id": entity_id_index, "model": user.user_id, "mytag": "user", "IPT": 0, "RAM": 0, "COST": 0,
                          "WATT": 0}
                topology_json["entity"].append(t_user)
                yafs_entity_id_name_map[entity_id_index] = user.user_id
                t_user_link = {"s": t_mdc_base_station["id"], "d": entity_id_index, "BW": bw, "PR": 0}
                topology_json["link"].append(t_user_link)
        # add all mdc-to-mdc links

        check_index = 0
        for mdc in self.mdc_list:
            if check_index < len(self.mdc_list) - 1:
                pr = DEFAULT_EDGE_PR
            else:
                pr = DEFAULT_CLOUD_PR
            mdc_index += 1

            t_src_gw_id = mdc_gw_id_map[mdc.mdc_id]
            for mdc_link in mdc.mdc_links:
                link_exist = False
                t_dst_gw_id = mdc_gw_id_map[mdc_link[0]]
                t_bw = mdc_link[1]
                for link in topology_json["link"]:
                    if link["s"] == t_dst_gw_id and link["d"] == t_src_gw_id:
                        link_exist = True
                if not link_exist:
                    t_link = {"s": t_src_gw_id, "d": t_dst_gw_id, "BW": t_bw, "PR": pr}
                    topology_json["link"].append(t_link)

        return topology_json, yafs_entity_id_name_map, server_id_entity_id_map
