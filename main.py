import random
import time
import json
from copy import deepcopy
import os, base64
import requests
from datetime import datetime, timedelta, timezone
import multiprocessing
import logging
import traceback
import asyncio
import threading
from aiokafka import AIOKafkaProducer
from static.HeatExchangerDigitalTwin import HeatExchangerDigitalTwin
from static.BoilerDigitalTwin import Boiler
from static.TransformerDigitalTwin import TransformerDigitalTwin
from static.GeneratorDigitalTwin import GeneratorDigitalTwin
multiprocessing_lock = multiprocessing.Lock()


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
        actual_data_map = self.heat_exchanger_formulation_computer.run_instance(primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure=primary_fluid_inlet_pressure, fault_type=fault_type)
        return actual_data_map

    def generate_and_store_data(self, recieved_fault_type=None):
        self.fault_type = None
        primary_fluid_inlet_temperature = self.base_primary_fluid_inlet_temperature+random.randint(-10,10)
        secondary_fluid_inlet_temperature = self.base_secondary_fluid_inlet_temperature+random.randint(-5, 25)
        primary_fluid_mass_flow = self.base_primary_fluid_inlet_mass_flow+(random.randint(-50, 50)*10)
        secondary_fluid_mass_flow = self.base_secondary_fluid_inlet_mass_flow+(random.randint(-30, 30)*10)
        primary_fluid_inlet_pressure = self.base_primary_fluid_inlet_pressure

        actual_outputs = self.get_actual_heat_exchanger_outputs_from_formulation(primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure, fault_type=recieved_fault_type)
        data = {"actual_outputs": actual_outputs}
        current_time = (datetime.now(timezone.utc)+timedelta(seconds=5)).isoformat()
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


    def generate_and_store_data(self, recieved_fault_type=None):
        self.fault_type = None
        self.previous_fault = self.fault_type
        e_SF, e_ST, e_Sp, e_Be, e_WT, e_WP, a_SF, a_ST, a_Sp, a_Be, a_WT, a_WP = self.boiler.getData(recieved_fault_type)
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
        current_time = (datetime.now(timezone.utc)+timedelta(seconds=5)).isoformat()
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
        formulation_model_data = self.transformer_digital_twin.run_instance(fault_type=fault_type, current_scale=current_scale, reset_flag=reset_flag)
        return formulation_model_data


    def generate_and_store_data(self, recieved_fault_type=None):
        self.fault_type = None
        self.previous_fault = self.fault_type
        self.current_scale, reset_flag = 0, True
        actual_outputs = self.get_actual_outputs(fault_type=recieved_fault_type, current_scale=self.current_scale, reset_flag=reset_flag)
        data = {"actual_outputs": actual_outputs}
        current_time = (datetime.now(timezone.utc)+timedelta(seconds=5)).isoformat()
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


class GeneratorDataGenerator():
    def __init__(self):
        self.fault_type = None
        self.generator_digital_twin = GeneratorDigitalTwin()
        self.url = "https://qa65.assetsense.com/c2/services/digitalTwinService/getDigitalTwins"
        self.previous_fault = None
        self.tag_name_map = {
            "generator_power_generated": "GNRT-POWR",
            "generator_load_current": "GNRT-LOAD-CURR",
            "generator_voltage": "GNRT-VOLT",
            "generator_earthing_voltage": "GNRT-EARTH-VOLT",
            "generator_rpm": "GNRT-RPM",
            "generator_shaft_torque": "GNRT-SHAFT-TORQ",
            "generator_max_windings_temperature": "GNRT-WIDG-TEMP",
            "generator_max_bearing_temperature": "GNRT-TEMP",
        }
        self.tags_names = list(self.tag_name_map.values())
        self.fault_counter = 10
        self.jsession = None

    def get_actual_outputs(self, fault_type=None):
        actual_data_map = self.generator_digital_twin.run_instance(fault_type=fault_type)
        return actual_data_map

    def generate_and_store_data(self, recieved_fault_type=None):
        self.fault_type = None
        self.previous_fault = self.fault_type
        self.current_scale, reset_flag = 0, True
        actual_outputs = self.get_actual_outputs(recieved_fault_type)
        data = {"actual_outputs": actual_outputs}
        current_time = (datetime.now(timezone.utc)+timedelta(seconds=5)).isoformat()
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
        return [data_to_send_list, 1]


async def start_workers(dt_objects, assets, tenants, queue_condition, data_queue):

    def process_asset(dt_object):
        data = dt_object.generate_and_store_data()
        return data
    logging.info("workers started")
    while True:
        start_time = time.time()
        tasks = []
        data_to_send = []
        for dt_obj in dt_objects:
            data_to_send.append(process_asset(dt_obj))
        # main_data = []
        tag_count = 0
        for tenant in tenants:
            for data in data_to_send:
                for asset in assets[data[1]]:
                    if tenant in list(fault_names_map.keys()):
                        if tenant not in list(faulty_dt_objects.keys()):
                            faulty_dt_objects[tenant] = {}
                        if asset in list(fault_names_map[tenant].keys()):
                            if asset not in list(faulty_dt_objects[tenant].keys()):
                                faulty_dt_objects[tenant][asset] = deepcopy(dt_objects[data[1]])
                            if fault_names_map[tenant][asset] is not None:
                                data[0] = faulty_dt_objects[tenant][asset].generate_and_store_data(recieved_fault_type=fault_names_map[tenant][asset])[0]
                    with queue_condition:
                        for temp_tag_data in data[0]:
                            tag_data = deepcopy(temp_tag_data)
                            tag_count += 1
                            tag_name = tag_data["tag"]
                            tag_name = tag_name.replace("ECNHERE", asset)
                            tag_data["tag"] = tag_name
                            enqueue_data = {
                                "tenant" : tenant,
                                "tag_data" : tag_data
                            }
                            await data_queue.put(enqueue_data)
                        queue_condition.notify()
        end_time = time.time()
        time_elapsed = end_time - start_time
        if(time_elapsed<1):
            time.sleep(1-time_elapsed)
            # print(f"Time elapsed: {time_elapsed}")


async def send_data_kafka(message, kafka_producer, data_queue):
    topic = message["tenant"]+ "_condition_data"
    try:
        await kafka_producer.send(topic, message["tag_data"])
        # print(message["tag_data"])
        return True
    except:
        await data_queue.put(message)
        return False


async def create_tasks_kafka(queue_condition, data_queue, kafka_producer):
    count = 0
    data_send_start_time = time.time()
    while True:
        tasks = set()
        with queue_condition:
            if data_queue.empty():
                queue_condition.wait()
            while not data_queue.empty():
                message = await data_queue.get()
                count+=1
                task = asyncio.create_task(send_data_kafka(message, kafka_producer, data_queue))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
                results = await asyncio.gather(*tasks)
                if(False in results):
                    logging.info("kafka restarted")
                    kafka_producer = await initialize_kafka_producer()
                data_send_end_time = time.time()
                if(data_send_end_time-data_send_start_time>100):
                    data_send_start_time = data_send_end_time
                    logging.info(f"Current Q size {data_queue.qsize()}")


def send_data_kafka_wrapper(queue_condition, data_queue, kafka_producer):
    asyncio.run(create_tasks_kafka(queue_condition, data_queue, kafka_producer))


async def initialize_kafka_producer():
    logging.info("kafka producer initialisation started")
    producer = AIOKafkaProducer(
        bootstrap_servers = ['kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'],
        # bootstrap_servers = '192.168.0.123:9092',
        acks = 0,
        max_batch_size = 6550,
        linger_ms =5,
        key_serializer=lambda k: json.dumps(k).encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=60000
    )
    await producer.start()
    return producer

def start_fault_retriever_thread(assets):
    while True:
        start_time = time.time()
        for tenant in tenants:
            for ecn_type in assets:
                for ecn in ecn_type:
                    fault_response = get_fault_from_c2(ecn, user_details[tenant])
                    manage_jsessionid(fault_response.cookies.get("JSESSIONID"))
                    if tenant not in list(fault_names_map.keys()):
                        fault_names_map[tenant] = {}
                    if ecn not in list(fault_names_map[tenant].keys()):
                        fault_names_map[tenant][ecn] = None
                    manage_asset_faults(tenant, ecn, fault_response)
        end_time = time.time()
        time_elapsed = end_time - start_time
        if time_elapsed < 60 * 10:
            time.sleep((60 * 10) - time_elapsed)

def get_fault_from_c2(ecn, user_details):
    url = "https://qa65.assetsense.com/c2/services/digitalTwinService/getDigitalTwins"
    username = user_details["username"]
    password = user_details["password"]
    payload = json.dumps({
        "DigitalTwinFilter": {
            "ecn": ecn
        }
    })
    authorization = base64.urlsafe_b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Basic {authorization}",
        'Cookie': f"JSESSIONID={global_jsessionid}"
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response

def manage_jsessionid(recieved_jsessionid):
    if recieved_jsessionid is not None:
        global_jsessionid = recieved_jsessionid

def manage_asset_faults(tenant, ecn, fault_response):
    parsed_fault_response = json.loads(fault_response.text)
    if len(parsed_fault_response["DigitalTwin"]) != 0:
        asset_type = str(parsed_fault_response["DigitalTwin"][0]["faultCode"]["assetType"]["id"])
        recieved_fault_code = str(parsed_fault_response["DigitalTwin"][0]["faultCode"]["code"])
        if recieved_fault_code in list(asset_type_fault_code_to_name_map[asset_type].keys()):
            if asset_type_fault_code_to_name_map[asset_type][recieved_fault_code] != fault_names_map[tenant][ecn]:
                if tenant in list(faulty_dt_objects.keys()):
                    if ecn in list(faulty_dt_objects[tenant].keys()):
                        faulty_dt_objects[tenant].pop(ecn)
            fault_names_map[tenant][ecn] = asset_type_fault_code_to_name_map[asset_type][recieved_fault_code]
            if asset_type_fault_code_to_name_map[asset_type][recieved_fault_code] is not None:
                print(f"Fault induced for {ecn} : {asset_type_fault_code_to_name_map[asset_type][recieved_fault_code]}")

async def main():
    try:
        kafka_producer = await initialize_kafka_producer()
        logging.info("producers_created")
        # dt_objects = [BoilerDataGenerator(), HeatExchangerDataGenerator(), TransformerDataGenerator(), GeneratorDataGenerator()]
        dt_objects = [HeatExchangerDataGenerator(), GeneratorDataGenerator()]
        data_queue = asyncio.Queue()
        queue_condition = threading.Condition()
        logging.info("condition_creadted")
        kafka_thread = threading.Thread(target=send_data_kafka_wrapper, args=(queue_condition, data_queue, kafka_producer))
        logging.info("kafka_thread_created")
        kafka_thread.start()
        logging.info("kafka_thread_started")
        fault_retrieval_thread = threading.Thread(target=start_fault_retriever_thread, args=(assets))
        fault_retrieval_thread.start()
        await start_workers(dt_objects, assets, tenants, queue_condition, data_queue)
        logging.info("workers_started")
    except Exception as e:
        logging.info(f"the following exception occured while Ingesting data: {e}")
        traceback.print_exception(type(e), e, e.__traceback__)


if __name__ == "__main__":
    logging.info(f"Data generator Started")
    tenants = ["historian","hydqatest"]
    user_details = {
        "historian": {
            "username": "saikiran.historian",
            "password": "HydeVil#71"
        },
        "hydqatest": {
            "username": "subhash.hydqatest",
            "password": "HydeVil#71"
        }
    }
    dt_object_position_to_asset_type_code_map = {
        0: "4511",
        1: "4500"
    }
    asset_type_fault_code_to_name_map = {
        "4511": {
            "HTE-00-000": None,
            "HTE-ME-001": "water_inlet_clog",
            "HTE-OT-001": "foul_accumulation",
            "HTE-HY-001": "hot_cooling_fluid",
            "HTE-ME-002": "shell_overpressure"
        },
        "4500": {
            "GNRT-00-000": None,
            "GNRT-EL-001": "short_circuit",
            "GNRT-EL-003": "insulation_issues",
            "GNRT-ME-002": "bearing_damage",
            "GNRT-OT-001": "control_system_failure",
            "GNRT-EL-002": "overloaded",
            "GNRT-ME-001": "cooling_failure"
        },
        "4507": {
            "TRNS-00-000": None,
            "TRNS-EL-001": "short_circuit",
            "TRNS-EL-002": "insulation_issues",
            "TRNS-EL-003": "overloaded",
            "TRNS-OT-002": "cooling_failure",
            "TRNS-OT-003": "interturner_fault",
            "TRNS-ME-001": "tap_changer_fault",
        },
        "4504": {
            "STRBN-00-000": None,
            "STRBN-ME-010": 1,
            "STRBN-ME-011": 2,
            "STRBN-ME-012": 3,
            "STRBN-ME-013": 4,
            "STRBN-OT-001": 5
        },
        "4503": {
            "GTRBN-00-000": None,
            "GTRBN-OT-003": "fouling",
            "GTRBN-OT-004": "errosion",
            "GTRBN-OT-002": "corrosion",
            "GTRBN-OT-005": "surge",
            "GTRBN-OT-001": "tip_clearance"
        },
        "6241": {
            "CFC-00-000": None,
            "CFC-ME-001": "inlet_clog",
            "CFC-ME-002": "inter_stage_air_leakage",
            "CFC-ME-003": "intercooler_inefficiency",
            "CFC-ME-006": "shaft_misalignment",
            "CFC-ME-005": "bearing_damage",
            "CFC-ME-011": "impeller_corrosion"
        },
        "6240": {
            "CFP-00-000": None,
            "CFP-OT-001": 1,
            "CFP-ME-001": 2,
            "CFP-ME-002": 3,
            "CFP-OT-002": 4,
            "CFP-AE-001": 5,
            "CFP-ME-003": 6
        },
        "4505": {
            "BLR-00-000": None,
            "BLR-ME-004": 1,
            "BLR-ME-001": 2,
            "BLR-ME-002": 3,
            "BLR-AE-001": 4,
            "BLR-ME-003": 5
        },
        "4501": {
            "HGNRT-00-000": None,
            "HGNRT-OT-001": "corrosion",
            "HGNRT-OT-002": "fatigue",
            "HGNRT-OT-003": "tube_failure",
            "HGNRT-OT-004": "creep",
        }
    }
    faulty_dt_objects = {}
    global_jsessionid = "qwertyuiopasdfghjklzxcvbnm"
    assets = []
    # assets.append(["AS-HTE-DGT-"+str(i+1) for i in range(500)])
    # assets.append(["AS-BLR-DGT-"+str(i+1) for i in range(500)])
    # assets.append(["AS-TRNS-DGT-"+str(i+1) for i in range(500)])
    assets.append(["HTE-FD-"+str(i) for i in range(10)])
    assets.append(["GEN-FD-"+str(i) for i in range(10)].append("GenTest101"))
    fault_names_map = {}
    asyncio.run(main())