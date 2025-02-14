import numpy as np
import random
import time
import json
from copy import deepcopy
from collections import deque
# import torch
import os, base64
import requests
from kafka import KafkaProducer
from datetime import datetime, timedelta
# from static.compressor_digital_twin import CompressorDigitalTwin
# from static.PumpDigitalTwin import PumpPower
from static.HeatExchangerDigitalTwin import HeatExchangerDigitalTwin
from static.BoilerDigitalTwin import Boiler
# from static.GeneratorDigitalTwin import GeneratorDigitalTwin
# from static.SteamTurbineDigitalTwin import SteamTurbine
# from static.GasTurbineDigitalTwin import GasTurbineDigitalTwin
# from static.spectrum_model import pumpEquipment, compressorEquipment, generatorEquipment, steamTurbineEquipment, gasTurbineEquipment
# from static.set_up_vs import PumpVectorstore, CompressorVectorstore, GeneratorVectorstore, SteamTurbineVectorstore, GasTurbineVectorstore
# from static.TransformerDigitalTwin import TransformerDigitalTwin
import multiprocessing
import logging
import traceback
multiprocessing_lock = multiprocessing.Lock()
import asyncio


class HeatExchangerDataGenerator():
    def __init__(self,asset_ecn):
        self.fault_type = None
        self.base_primary_fluid_inlet_temperature = 573
        self.base_primary_fluid_inlet_mass_flow = 18500  # kg/hr
        self.base_secondary_fluid_inlet_mass_flow = 10500  # kg/hr
        self.base_primary_fluid_inlet_pressure = 420_000
        self.base_heat_transfer_surface_area = 30
        self.heat_exchanger_formulation_computer = HeatExchangerDigitalTwin(self.base_primary_fluid_inlet_mass_flow, self.base_secondary_fluid_inlet_mass_flow, self.base_heat_transfer_surface_area)
        self.heat_exchanger_digital_twin = HeatExchangerDigitalTwin(self.base_primary_fluid_inlet_mass_flow, self.base_secondary_fluid_inlet_mass_flow, self.base_heat_transfer_surface_area)
        self.base_secondary_fluid_inlet_temperature = self.heat_exchanger_digital_twin.heat_exchanger.ambient_temperature
        # self.heat_exchanger_temperatures_model = torch.load("./saved_models/heat_exchanger_model.pth")
        self.ecn = asset_ecn
        self.url = "https://qa65.assetsense.com/c2/services/digitalTwinService/getDigitalTwins"
        self.previous_fault = None
        self.tag_name_map = {
            "heat_exchanger_primary_fluid_outlet_temperature": "HTE-PRCS-FLUD-OUTL-TEMP",
            "heat_exchanger_secondary_fluid_outlet_temperature": "HTE-SECDY-FLUD-OUTL-TEMP",
            "heat_exchanger_primary_fluid_inlet_volume_flow": "HTE-PRCS-FLUD-INL-FLOW",
            "heat_exchanger_primary_fluid_outlet_volume_flow": "HTE-PRCS-FLUD-OUTL-FLOW",
            "heat_exchanger_primary_fluid_inlet_temperature": "HTE-PRCS-FLUD-INL-TEMP",
            "heat_exchanger_secondary_fluid_inlet_temperature": "HTE-SECDY-FLUD-INL-TEMP",
            "heat_exchanger_secondary_fluid_mass_flow": "HTE-SECDY-FLUD-MASS-FLOW",
            "heat_exchanger_primary_fluid_inlet_pressure": "HTE-PRCS-FLUD-INL-PRES",
            "heat_exchanger_primary_fluid_mass_flow": "HTE-PRCS-FLUD-MASS-FLOW"
        }
        self.tags_names = list(self.tag_name_map.values())
        self.tenant = "historian"
        self.topic_name = self.tenant + "_condition_data"
        self.producer = KafkaProducer(
            bootstrap_servers = ['kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'],
            acks = 0,
            batch_size = 65536,
            linger_ms =5,
            key_serializer=lambda k: json.dumps(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.jsession = None

    def get_actual_heat_exchanger_outputs_from_formulation(self, primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure, fault_type):
        actual_data_map = self.heat_exchanger_formulation_computer.run_instance(primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure=primary_fluid_inlet_pressure, fault_type=self.fault_type)
        return actual_data_map

    async def generate_and_store_data(self):
        self.fault_type = None
        primary_fluid_inlet_temperature = self.base_primary_fluid_inlet_temperature+random.randint(-10,10)
        secondary_fluid_inlet_temperature = self.base_secondary_fluid_inlet_temperature+random.randint(-5, 25)
        primary_fluid_mass_flow = self.base_primary_fluid_inlet_mass_flow+(random.randint(-50, 50)*10)
        secondary_fluid_mass_flow = self.base_secondary_fluid_inlet_mass_flow+(random.randint(-30, 30)*10)
        primary_fluid_inlet_pressure = self.base_primary_fluid_inlet_pressure

        actual_outputs = self.get_actual_heat_exchanger_outputs_from_formulation(primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure, fault_type=self.fault_type)
        data = {"actual_outputs": actual_outputs}
        current_time = datetime.utcnow().isoformat()
        # for primary_obs in list(data.keys()):
        for obs in list(data["actual_outputs"].keys()):
            tagId = "AS-"+self.ecn+"-"
            topic = self.topic_name
            tagId = tagId + self.tag_name_map[obs]
            data_to_send = {
                "assetId": str(-1),
                "conditionDataId": str(0),
                "orgId": str(0),
                "createdBy": str(0),
                "formItemId": str(-1),
                "assetType": str(0),
                "v": str(data["actual_outputs"][obs]),
                "tag": str(tagId),
                "t": str(current_time)
            }
            self.producer.send(topic, value=data_to_send)
        print("pushed data for ", self.ecn, "at", time.time())

class BoilerDataGenerator():
    def __init__(self,asset_ecn):
        self.boiler = Boiler()
        self.tag_name_map = {
            "boiler_steam_flow": "BLR-STM-FLOW",
            "boiler_steam_temperature": "BLR-STM-TEMP",
            "boiler_steam_pressure": "BLR-STM-PRES",
            "boiler_efficiency": "BLR-EFF",
            "boiler_water_temperature": "BLR-INL-TEMP",
            "boiler_water_pressure": "BLR-INL-PRES",
            "boiler_feed_water_flow_rate" : "BLR-FEED-WTR-FLOW"
        }
        self.ecn = asset_ecn
        self.url = "https://qa65.assetsense.com/c2/services/digitalTwinService/getDigitalTwins"
        self.previous_fault = None
        self.tags_names = list(self.tag_name_map.values())
        self.tenant = "historian"
        self.topic_name = self.tenant + "_condition_data"
        self.producer = KafkaProducer(
            bootstrap_servers = ['kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'],
            acks = 0,
            batch_size = 65536,
            linger_ms =5,
            key_serializer=lambda k: json.dumps(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.jsession = None

    def get_current_fault_type(self):

        fault_code_map = {
            "BLR-00-000": None,
            "BLR-ME-004": 1,
            "BLR-ME-001": 2,
            "BLR-ME-002": 3,
            "BLR-AE-001": 4,
            "BLR-ME-003": 5
        }
        payload = json.dumps({
            "DigitalTwinFilter": {
                "ecn": self.ecn
            }
        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Basic c2Fpa2lyYW4uaGlzdG9yaWFuOkh5ZGVWaWwjNzE=',
            'Cookie': 'JSESSIONID=AADC36CAA056E96E7682A4B980F5C452'
        }
        if(self.jsession == None):
            pass
        else:
            headers["Cookie"] = self.jsession
        try:
            response = requests.request("POST", self.url, headers=headers, data=payload)
            if "JSESSIONID=" + response.cookies.get("JSESSIONID") != self.jsession:
                logging.info(f"jsessionid changed for {self.ecn}")
            if len(json.loads(response.text)["DigitalTwin"]) == 0:
                current_fault = self.previous_fault
            else:
                if json.loads(response.text)["DigitalTwin"][0]["faultCode"]["code"] in list(fault_code_map.keys()):
                    current_fault = fault_code_map[json.loads(response.text)["DigitalTwin"][0]["faultCode"]["code"]]
                else:
                    current_fault = self.previous_fault
            if(self.jsession == None or self.jsession != "JSESSIONID=" + response.cookies.get("JSESSIONID")):
                self.jsession = "JSESSIONID=" + response.cookies.get("JSESSIONID")
            else:
                pass
        except Exception as e:
            logging.error(f"Exception occured in: {self.ecn}, \nInfo: {e}", exc_info=True)
            current_fault = self.previous_fault
            self.jsession = None
        return current_fault

    async def generate_and_store_data(self):
        self.fault_type = None
        if(self.previous_fault != self.fault_type):
            logging.info(f"Induced Fault for {self.ecn} is {self.fault_type}")
        self.previous_fault = self.fault_type
        e_SF, e_ST, e_Sp, e_Be, e_WT, e_WP, a_SF, a_ST, a_Sp, a_Be, a_WT, a_WP = self.boiler.getData(self.fault_type)
        # logging.info(e_F,e_)
        expected_spressure = e_Sp
        actual_spressure = a_Sp
        expected_sflow = e_SF
        actual_sflow = a_SF
        expected_stemp = e_ST
        actual_stemp = a_ST
        actual_beff = a_Be
        expected_beff = e_Be
        expected_wtemp = e_WT
        expected_wpress = e_WP
        actual_wtemp = a_WT
        actual_wpress = a_WP
        # quantity, calorific, pH, ppm
        data = {
            "actual_outputs": {
                "boiler_steam_flow": actual_sflow,
                "boiler_steam_temperature": actual_stemp,
                "boiler_steam_pressure": actual_spressure,
                "boiler_efficiency": actual_beff,
                "boiler_water_temperature": actual_wtemp,
                "boiler_water_pressure": actual_wpress,
                "boiler_feed_water_flow_rate": 100
            },
        }
        current_time = datetime.utcnow().isoformat()
        for obs in list(data["actual_outputs"].keys()):
            tagId = "AS-"+self.ecn+"-"
            topic = self.topic_name
            tagId = tagId + self.tag_name_map[obs]
            data_to_send = {
                "assetId": str(-1), 
                "conditionDataId": str(0),
                "orgId": str(0),
                "createdBy": str(0),
                "formItemId": str(-1),
                "assetType": str(0),
                "v": str(data["actual_outputs"][obs]),
                "tag": str(tagId),
                "t": str(current_time)
            }
            self.producer.send(topic, value=data_to_send)
        print("pushed data for ", self.ecn, "at", time.time())

async def start_workers_blr(dt_objects):

    async def process_asset(dt_object):
        await dt_object.generate_and_store_data()

    while(True):
        start_time = time.time()
        tasks = []
        count = 0
        for dt_obj in dt_objects:
            task = asyncio.create_task(process_asset(dt_obj))
            # print(time.time())
            tasks.append(task)
        asyncio.gather(*tasks)
        end_time = time.time()
        time_elapsed = end_time - start_time
        # print("Done") 
        if time_elapsed < 1:
            await asyncio.sleep(1 - time_elapsed)
        count += 1
        # print("\n\n\n\n\n")

if __name__ == "__main__":
    assets = []
    assets.extend(["AS-HTE-DGT-"+str(i+1) for i in range(50)])
    assets.extend(["AS-BLR-DGT-"+str(i+1) for i in range(50)])
    # print(len(assets))
    # print("assets list: ", assets)
    dt_objects = []
    for asset_ecn in assets:
        if "HTE" in asset_ecn:
            dt_objects.append(HeatExchangerDataGenerator(asset_ecn))
        else:
            dt_objects.append(BoilerDataGenerator(asset_ecn))

    asyncio.run(start_workers_blr(dt_objects))