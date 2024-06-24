from yafs.placement import Placement
import random


class CustomStaticPlacement(Placement):
    def __init__(self, name, activation_dist=None, logger=None):
        Placement.__init__(self, name, activation_dist, logger)
        self._static_result = None

    def preload_static_result(self, result: dict):
        self._static_result = result

    def initial_allocation(self, sim, app_name):
        if not self._static_result:
            print('Results from dsp_app must be preloaded first')
            exit(1)
        for key in self._static_result:
            module = key
            idtopo = self._static_result[key]
            app = sim.apps[app_name]
            services = app.services
            idDES = sim.deploy_module(app_name, module, [services[module]], [idtopo])

class JSONPlacement(Placement):
    def __init__(self, json, **kwargs):
        super(JSONPlacement, self).__init__(**kwargs)
        self.data = json

    def initial_allocation(self, sim, app_name):
        for item in self.data["initialAllocation"]:
            if app_name == item["app"]:
                # app_name = item["app"]
                module = item["module_name"]
                idtopo = item["id_resource"]
                app = sim.apps[app_name]
                services = app.services
                idDES = sim.deploy_module(app_name, module, services[module], [idtopo])

