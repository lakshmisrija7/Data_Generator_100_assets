import numpy as np
import random
import time
import json
from copy import deepcopy
from collections import deque
import os, base64
import requests
from datetime import datetime, timedelta
from static.HeatExchangerDigitalTwin import HeatExchangerDigitalTwin
from static.BoilerDigitalTwin import Boiler
from static.TransformerDigitalTwin import TransformerDigitalTwin
import multiprocessing
import logging
import traceback
multiprocessing_lock = multiprocessing.Lock()
import asyncio
import concurrent
import queue 
import threading
from aiokafka import AIOKafkaProducer

logging.basicConfig(format='%(asctime)s -- %(levelname)s -- %(message)s -- %(exc_info)s', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.CRITICAL)

class HeatExchangerDataGenerator():
    def __init__(self):
        self.fault_type = None
        self.base_primary_fluid_inlet_temperature = 573
        self.base_primary_fluid_inlet_mass_flow = 18500  # kg/hr
        self.base_secondary_fluid_inlet_mass_flow = 10500  # kg/hr
        self.base_primary_fluid_inlet_pressure = 420_000
        self.base_heat_transfer_surface_area = 30
        self.heat_exchanger_formulation_computer = HeatExchangerDigitalTwin(self.base_primary_fluid_inlet_mass_flow, self.base_secondary_fluid_inlet_mass_flow, self.base_heat_transfer_surface_area)
        self.heat_exchanger_digital_twin = HeatExchangerDigitalTwin(self.base_primary_fluid_inlet_mass_flow, self.base_secondary_fluid_inlet_mass_flow, self.base_heat_transfer_surface_area)
        self.base_secondary_fluid_inlet_temperature = self.heat_exchanger_digital_twin.heat_exchanger.ambient_temperature
        # self.ecn = asset_ecn
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
        # self.tenant = tenant
        # self.topic_name = self.tenant + "_condition_data"
        # self.producer = kafka_producer
        self.jsession = None

    def get_actual_heat_exchanger_outputs_from_formulation(self, primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure, fault_type):
        actual_data_map = self.heat_exchanger_formulation_computer.run_instance(primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure=primary_fluid_inlet_pressure, fault_type=self.fault_type)
        return actual_data_map

    def generate_and_store_data(self):
        self.fault_type = None
        primary_fluid_inlet_temperature = self.base_primary_fluid_inlet_temperature+random.randint(-10,10)
        secondary_fluid_inlet_temperature = self.base_secondary_fluid_inlet_temperature+random.randint(-5, 25)
        primary_fluid_mass_flow = self.base_primary_fluid_inlet_mass_flow+(random.randint(-50, 50)*10)
        secondary_fluid_mass_flow = self.base_secondary_fluid_inlet_mass_flow+(random.randint(-30, 30)*10)
        primary_fluid_inlet_pressure = self.base_primary_fluid_inlet_pressure

        actual_outputs = self.get_actual_heat_exchanger_outputs_from_formulation(primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure, fault_type=self.fault_type)
        data = {"actual_outputs": actual_outputs}
        current_time = datetime.utcnow().isoformat()
        data_to_send_list = []
        for obs in list(data["actual_outputs"].keys()):
            tagId = "AS-"+"ECNHERE"+"-"
            # topic = self.topic_name
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
            data_to_send_list.append(data_to_send)
            # self.producer.send(topic, value=data_to_send)

        return [data_to_send_list, 0]


class BoilerDataGenerator():
    def __init__(self):
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
        # self.ecn = asset_ecn
        self.url = "https://qa65.assetsense.com/c2/services/digitalTwinService/getDigitalTwins"
        self.previous_fault = None
        self.tags_names = list(self.tag_name_map.values())
        # self.tenant = tenant
        # self.topic_name = self.tenant + "_condition_data"
        # self.producer = kafka_producer
        self.jsession = None


    def generate_and_store_data(self):
        self.fault_type = None
        self.previous_fault = self.fault_type
        e_SF, e_ST, e_Sp, e_Be, e_WT, e_WP, a_SF, a_ST, a_Sp, a_Be, a_WT, a_WP = self.boiler.getData(self.fault_type)
        # logging.info(e_F,e_)
        actual_spressure = a_Sp
        actual_sflow = a_SF
        actual_stemp = a_ST
        actual_beff = a_Be
        actual_wtemp = a_WT
        actual_wpress = a_WP
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
        data_to_send_list = []
        for obs in list(data["actual_outputs"].keys()):
            tagId = "AS-"+"ECNHERE"+"-"
            # topic = self.topic_name
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
            # self.producer.send(topic, value=data_to_send)
            data_to_send_list.append(data_to_send)
        return [data_to_send_list, 1]


class TransformerDataGenerator:
    def __init__(self):
        self.transformer_digital_twin = TransformerDigitalTwin()
        self.transformer_formulation_model = TransformerDigitalTwin()
        # self.ecn = ecn
        self.url = "https://qa65.assetsense.com/c2/services/digitalTwinService/getDigitalTwins"
        self.previous_fault = None
        self.tag_name_map = {
            "transformer_primary_voltage": "TRNS-PRIMY-VOLT",
            "transformer_secondary_voltage": "TRNS-SECDY-VOLT",
            "transformer_primary_current": "TRNS-PRIMY-CURR",
            "transformer_secondary_current": "TRNS-SECDY-CURR",
            "transformer_temperature": "TRNS-TEMP",
            "transformer_earthing_voltage": "TRNS-EARTH-VOLT",
            "transformer_efficiency": "TRNS-EFF",
            "transformer_ambient_temperature": "TRNS-AMB-TEMP"
        }
        self.tags_names = list(self.tag_name_map.values())
        # self.tenant = tenant
        # self.topic_name = self.tenant + "_condition_data"
        # self.producer = kafka_producer
        self.fault_counter = 10
        self.jsession = None


    def get_actual_outputs(self, fault_type, current_scale, reset_flag):
        formulation_model_data = self.transformer_formulation_model.run_instance(fault_type=fault_type, current_scale=current_scale, reset_flag=reset_flag)
        return formulation_model_data


    def generate_and_store_data(self):
        self.fault_type = None
        self.previous_fault = self.fault_type
        self.current_scale, reset_flag = 0, True
        actual_outputs = self.get_actual_outputs(fault_type=self.fault_type, current_scale=self.current_scale, reset_flag=reset_flag)
        data = {"actual_outputs": actual_outputs}
        current_time = datetime.utcnow().isoformat()
        data_to_send_list = []
        for primary_obs in list(data.keys()):
            for obs in list(data[primary_obs].keys()):
                tagId = "AS-"+"ECNHERE"+"-"
                # topic = self.topic_name
                tagId = tagId + self.tag_name_map[obs]
                data_to_send = {"assetId": str(-1), "conditionDataId": str(0),
                    "orgId": str(0),
                    "createdBy": str(0),
                    "formItemId": str(-1),
                    "assetType": str(0),
                    "v": str(data[primary_obs][obs]),
                    "tag": str(tagId),
                    "t": str(current_time)
                }
                data_to_send_list.append(data_to_send)
                # self.producer.send(topic, value=data_to_send)
        return [data_to_send_list, 2]


# def ingest_data(tenant, producer, data_to_send):
#     topic = tenant + "_condition_data"
#     for data in data_to_send:
#         producer.send(topic, value = data)
#     return "DONE"

def start_workers_blr(dt_objects, assets, tenants):

    def process_asset(dt_object):
        data = dt_object.generate_and_store_data()
        return data
    print("workers started")
    while True:
        # start_time = time.time()
        tasks = []
        data_to_send = []
        for dt_obj in dt_objects:
            # task = asyncio.create_task(process_asset(dt_obj))
            # tasks.append(task)
            data_to_send.append(process_asset(dt_obj))
        # main_data = []
        tag_count = 0
        for tenant in tenants:
            for data in data_to_send:
                for asset in assets[data[1]]:
                    with queue_condition:
                        for tag_data in data[0]:
                            tag_count+=1
                            tag_name = tag_data["tag"]
                            tag_name = tag_name.replace("ECNHERE", asset)
                            tag_data["tag"] = tag_name
                            # main_data.append(tag_data)c
                            enqueue_data = {
                                "tenant" : tenant,
                                "tag_data" : tag_data
                            }
                            data_queue.put(enqueue_data)
                        queue_condition.notify()


async def send_data_kafka(message):
    print("send data kafka")
    topic = message["tenant"]+ "_condition_data"
    await kafka_producer.send(topic,message["tag_data"])
    print("pushed data for a tag into kafka")

async def create_tasks_kafka():
    count = 0
    data_send_start_time = time.time()
    while True:
        tasks = []
        with queue_condition:
            if data_queue.empty():
                queue_condition.wait()
            while not data_queue.empty():
                message = data_queue.get()
                count+=1
                tasks.append(send_data_kafka(message))
            await asyncio.gather(*tasks)
            if(count%24000==0):
                data_send_end_time = time.time()
                print(f"time taken for 24000 tags {data_send_end_time-data_send_start_time}")
                data_send_start_time = data_send_end_time


    

def send_data_kafka_wrapper():
    asyncio.run(create_tasks_kafka())
        


async def initialize_kafka_producer():
    print("kafka producer initialisation started")
    producer = AIOKafkaProducer(
        bootstrap_servers = ['kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'],
        acks = 0,
        max_batch_size = 65536,
        linger_ms =5,
        key_serializer=lambda k: json.dumps(k).encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    return producer
    # return None


if __name__ == "__main__":
    logging.info(f"Data generator Started")
    assets = []
    assets.append(["AS-HTE-DGT-"+str(i+1) for i in range(500)])
    assets.append(["AS-BLR-DGT-"+str(i+1) for i in range(500)])
    assets.append(["AS-TRNS-DGT-"+str(i+1) for i in range(500)])
    tenants = ["historian","hydqatest"]
    kafka_producer = asyncio.run(initialize_kafka_producer())
    print("producers_created")
    # kafka_producers = {"historian": "ss", "hydqatest": "asas"}
    dt_objects = [BoilerDataGenerator(), HeatExchangerDataGenerator(), TransformerDataGenerator()]
    data_queue = queue.Queue()
    queue_condition = threading.Condition()
    print("condition_creadted")
    kafka_thread = threading.Thread(target= send_data_kafka_wrapper)
    print("thread_created")
    kafka_thread.start()
    print("thread_started")
    start_workers_blr(dt_objects, assets, tenants)
    print("workers_started")