import numpy as np
import random
import time
import math
from static.Generator import Generator
from static.utils import Conversions


class GeneratorDigitalTwin:
    def __init__(self):
        self.generator = Generator()
        self.available_fault_types = ["short_circuit", "insulation_issues", "bearing_damage", "control_system_failure", "overloaded", "cooling_failure"]
        self.fault_intensity = 1

    def run_instance(self, shaft_torque=5500, fault_type=None):
        # self.update_fault_intensity(fault_type)
        self.generator.set_shaft_torque(shaft_torque)
        self.generator.calculate_frequency()
        self.generator.calculate_angular_velocity()
        self.generator.calculate_mechanical_power()
        self.generator.calculate_preleminary_electrical_power()
        self.generator.calculate_load_current()
        observations_map = {
            "generator_power_generated": round(self.generator.preliminary_electrical_power, 2),
            "generator_load_current": round(self.generator.load_current, 2),
            "generator_voltage": round(self.generator.required_voltage, 2),
            "generator_earthing_voltage": round(self.generator.earthing_voltage, 2),
            "generator_rpm": round(self.generator.shaft_rpm, 2),
            "generator_shaft_torque": round(self.generator.shaft_torque, 2),
            "generator_max_windings_temperature": round(self.generator.max_windings_temperature, 2),
            "generator_max_bearing_temperature": round(self.generator.max_bearing_temperature, 2),
        }
        # if fault_type is not None:
        #     observations_map = self.fault_inducer(observations_map, fault_type)
        
        observations_map["generator_max_windings_temperature"] = Conversions.kelvin_to_fahrenheit(observations_map["generator_max_windings_temperature"])
        observations_map["generator_max_bearing_temperature"] = Conversions.kelvin_to_fahrenheit(observations_map["generator_max_bearing_temperature"])

        return observations_map

    def update_fault_intensity(self, fault_type):
        if fault_type is not None and self.fault_intensity < 30:
            self.fault_intensity += 1
        if fault_type is None:
            self.fault_intensity = 1

    def generator_power_off_state(self, observations_map):
        observations_map["generator_power_generated"] = 0
        observations_map["generator_load_current"] = 0
        observations_map["generator_voltage"] = 0
        observations_map["generator_earthing_voltage"] = 0
        observations_map["generator_rpm"] = 0
        observations_map["generator_shaft_torque"] = 0
        return observations_map

    def fault_inducer(self, observations_map, fault_type=None):

        if fault_type == self.available_fault_types[0]:
            observations_map["generator_max_windings_temperature"] += (observations_map["generator_max_windings_temperature"] * 0.4)
            observations_map["generator_load_current"] = observations_map["generator_load_current"] * 3
            if self.fault_intensity > 2:
                observations_map = self.generator_power_off_state(observations_map)

        if fault_type == self.available_fault_types[1]:
            observations_map["generator_earthing_voltage"] += self.generator.required_voltage
            if self.fault_intensity > 2:
                observations_map = self.generator_power_off_state(observations_map)
        
        if fault_type == self.available_fault_types[2]:
            observations_map["generator_max_bearing_temperature"] += 20
            torque_loss = random.randint(10, 20) * 20
            observations_map["generator_shaft_torque"] -= torque_loss
            self.generator.set_shaft_torque(self.generator.shaft_torque - torque_loss)
            self.generator.calculate_frequency()
            self.generator.calculate_angular_velocity()
            self.generator.calculate_mechanical_power()
            self.generator.calculate_preleminary_electrical_power()
            self.generator.calculate_load_current()
            observations_map["generator_power_generated"] = round(self.generator.preliminary_electrical_power, 2)
            observations_map["generator_load_current"] = round(self.generator.load_current, 2)
            observations_map["generator_voltage"] = round(self.generator.required_voltage, 2)
            observations_map["generator_earthing_voltage"] = round(self.generator.earthing_voltage, 2)
            observations_map["generator_rpm"] = round(self.generator.shaft_rpm, 2)
            observations_map["generator_max_windings_temperature"] = round(self.generator.max_windings_temperature, 2)
        
        if fault_type == self.available_fault_types[3]:
            voltage_inaccuracy = random.randint(-20, 20) * 100
            observations_map["generator_voltage"] += voltage_inaccuracy
            torque_loss = voltage_inaccuracy / 10
            self.generator.set_shaft_torque(self.generator.shaft_torque - torque_loss)
            self.generator.calculate_frequency()
            self.generator.calculate_angular_velocity()
            self.generator.calculate_mechanical_power()
            self.generator.calculate_preleminary_electrical_power()
            self.generator.calculate_load_current()
            observations_map["generator_load_current"] = round(self.generator.load_current, 2)
            observations_map["generator_power_generated"] = round(self.generator.preliminary_electrical_power, 2)
        
        if fault_type == self.available_fault_types[4]:
            if self.fault_intensity < 20:
                demand_increase = self.generator.shaft_torque + (self.generator.shaft_torque * 0.03 * self.fault_intensity)
                self.generator.set_shaft_torque(demand_increase)
                self.generator.calculate_frequency()
                self.generator.calculate_angular_velocity()
                self.generator.calculate_mechanical_power()
                self.generator.calculate_preleminary_electrical_power()
                self.generator.calculate_load_current()
                observations_map["generator_power_generated"] = round(self.generator.preliminary_electrical_power, 2)
                observations_map["generator_load_current"] = round(self.generator.load_current, 2)
                observations_map["generator_voltage"] = round(self.generator.required_voltage, 2)
                observations_map["generator_earthing_voltage"] = round(self.generator.earthing_voltage, 2)
                observations_map["generator_rpm"] = round(self.generator.shaft_rpm, 2)
                observations_map["generator_max_windings_temperature"] += self.fault_intensity
            else:
                observations_map = self.generator_power_off_state(observations_map)

        if fault_type == self.available_fault_types[5]:
            observations_map["generator_max_windings_temperature"] += (self.fault_intensity/30) * 20
            observations_map["generator_max_bearing_temperature"] += (self.fault_intensity/30) * 20
            observations_map["generator_load_current"] -= (observations_map["generator_load_current"] * 0.1) * (self.fault_intensity/30)
            observations_map["generator_power_generated"] = observations_map["generator_load_current"] * math.sqrt(3) * observations_map["generator_voltage"] * self.generator.power_factor

        return observations_map