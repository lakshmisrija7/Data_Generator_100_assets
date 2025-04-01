import numpy as np
import random
import time
import math
import traceback

class Generator:
    def __init__(self):
        self.shaft_rpm = 1800 # Synchronous Speed
        self.num_poles = 4
        self.shaft_torque = 5_500
        self.efficiency = 0.95
        self.required_voltage = 10_000
        self.num_phases = 3
        self.initial_power_factor = None
        self.power_factor = 0.95

        self.ambient_temperature = 288.15
        self.max_windings_temperature = 350
        self.max_bearing_temperature = 315
        self.earthing_voltage = 0

    def calculate_frequency(self):
        self.frequency = (self.shaft_rpm * self.num_poles) / 120

    def calculate_angular_velocity(self):
        self.angular_velocity = (2 * math.pi * self.shaft_rpm) / 60

    def calculate_mechanical_power(self):
        self.mechanical_power = self.angular_velocity * self.shaft_torque

    def calculate_preleminary_electrical_power(self):
        self.preliminary_electrical_power = self.efficiency * self.mechanical_power

    def calculate_load_current(self):
        self.load_current = self.preliminary_electrical_power / (math.sqrt(3) * self.required_voltage * self.power_factor)

    def set_shaft_torque(self, shaft_torque=5500):
        self.shaft_torque = shaft_torque

    def create_digital_twin(self, data_recieved, observation_codes):
        try:
            self.store_tags(data_recieved, observation_codes)

            self.initial_power_factor = data_recieved.custom_attributes["GNRT-POWR-FCTR"]
            self.num_poles = data_recieved.custom_attributes["GNRT-POLE-CNT"]

            def retrieve_values_using_observation_code(data_recieved, observation_code):
                raw_data = []
                for tag_id in list(data_recieved.tags.keys()):
                    if data_recieved.tags[tag_id]["obs_code"] == observation_code:
                        raw_data = data_recieved.tags[tag_id]["data"]
                data_to_send = []
                if len(raw_data) > 0:
                    for i in range(len(raw_data)):
                        data_to_send.append(raw_data[i]["value"])
                return np.mean(data_to_send)
            
            self.shaft_torque = retrieve_values_using_observation_code(data_recieved, "GNRT-SHAFT-TORQ")
            self.shaft_rpm = retrieve_values_using_observation_code(data_recieved, "GNRT-RPM")
            self.preliminary_electrical_power = retrieve_values_using_observation_code(data_recieved, "GNRT-POWR")
            self.required_voltage = retrieve_values_using_observation_code(data_recieved, "GNRT-VOLT")
            self.load_current = retrieve_values_using_observation_code(data_recieved, "GNRT-LOAD-CURR")

            self.angular_velocity = (2 * math.pi * self.shaft_rpm) / 60
            self.mechanical_power = self.angular_velocity * self.shaft_torque
            self.efficiency =  self.preliminary_electrical_power / self.mechanical_power
            self.power_factor = self.preliminary_electrical_power / (math.sqrt(3) * self.required_voltage * self.load_current)

            return {"flag": True, "message": "Operational digital twin created successfully"}
        
        except Exception as e:
            traceback.print_exception(type(e), e, e.__traceback__)
            return {"flag": False, "message": "Operational digital twin not created"}

    def store_tags(self, data_recieved, observation_codes):
        self.tags = {"actual": {}, "expected": {}}
        self.asset_type = data_recieved.asset_type
        self.observation_codes = observation_codes

        for tag_id in list(data_recieved.tags.keys()):
            if "EXP" in data_recieved.tags[tag_id]["obs_code"]:
                self.tags["expected"][data_recieved.tags[tag_id]["obs_code"]] = tag_id
            else:
                self.tags["actual"][data_recieved.tags[tag_id]["obs_code"]] = tag_id
