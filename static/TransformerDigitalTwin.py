import random
import time
import math
from static.Transformer import Transformer


class TransformerDigitalTwin:
    def __init__(self):
        self.transformer = Transformer()
        self.available_fault_types = ["short_circuit", "insulation_issues", "cooling_failure", "overloaded", "interturn_fault", "tap_changer_fault"]
        self.fault_intensity = 1
        self.deterioration_level = 0
        self.max_deterioration = (1 * 24 * 30 * 12 * 5)
        self.deterioration_ratios = {
            "transformer_primary_voltage": 0.0,
            "transformer_secondary_voltage": -1.0,
            "transformer_primary_current": 0.0,
            "transformer_secondary_current": -1.0,
            "transformer_temperature": 1.0,
            "transformer_earthing_voltage": 0.0,
            "transformer_efficiency": -1.0
        }

    def run_instance(self, fault_type=None, current_scale=1, reset_flag=0):
        self.update_fault_intensity(fault_type, current_scale)

        self.transformer.calculate_secondary_voltage()
        self.transformer.calculate_secondary_current()

        observations_map = {
            "transformer_primary_voltage" : round(self.transformer.primary_voltage, 2),
            "transformer_secondary_voltage" : round(self.transformer.secondary_voltage, 2),
            "transformer_primary_current" : round(self.transformer.primary_current, 2),
            "transformer_secondary_current" : round(self.transformer.secondary_current, 2),
            "transformer_temperature" : round(self.transformer.temperature, 2),
            "transformer_earthing_voltage" : round(self.transformer.earthing_voltage, 2),
            "transformer_efficiency" : round(self.transformer.efficiency * 100, 2)
        }
        if fault_type is not None:
            observations_map = self.fault_inducer(observations_map, fault_type)
        
        for observation_name in list(observations_map.keys()):
            observations_map[observation_name] = observations_map[observation_name] + (self.deterioration_level * self.deterioration_ratios[observation_name] / (self.max_deterioration * 100)) * observations_map[observation_name]

        return observations_map    

    def update_fault_intensity(self, fault_type, current_scale=1):
        if fault_type is not None and self.fault_intensity < 30:
            self.fault_intensity += current_scale
            if self.fault_intensity > 30:
                self.fault_intensity = 30
        if fault_type is None:
            self.fault_intensity = 1

    def update_deterioration(self, current_scale, reset_flag):
        if self.deterioration_level < self.max_deterioration:
            self.deterioration_level += current_scale
        if reset_flag != 0:
            self.deterioration_level = 0

    def transformer_power_off_state(self, observations_map):
        observations_map["transformer_primary_voltage"] = 0
        observations_map["transformer_secondary_voltage"] = 0
        observations_map["transformer_primary_current"] = 0
        observations_map["transformer_secondary_current"] = 0
        observations_map["transformer_temperature"] = 0
        observations_map["transformer_earthing_voltage"] = 0
        observations_map["transformer_efficiency"] = 0
        return observations_map

    def fault_inducer(self, observations_map, fault_type=None):

        if fault_type == self.available_fault_types[0]:
            observations_map = self.transformer_power_off_state(observations_map)

        if fault_type == self.available_fault_types[1]:
            observations_map["transformer_earthing_voltage"] += self.transformer.secondary_voltage
            if self.fault_intensity > 2:
                observations_map = self.transformer_power_off_state(observations_map)

        if fault_type == self.available_fault_types[2]:
            observations_map["transformer_temperature"] += observations_map["transformer_temperature"] * (self.fault_intensity / 100)
            observations_map["transformer_secondary_current"] -= observations_map["transformer_secondary_current"] * (self.fault_intensity / 100)
            observations_map["transformer_efficiency"] = self.transformer.calculate_efficiency(observations_map["transformer_primary_voltage"], observations_map["transformer_primary_current"], observations_map["transformer_secondary_voltage"], observations_map["transformer_secondary_current"])

        if fault_type == self.available_fault_types[3]:
            observations_map["transformer_temperature"] += observations_map["transformer_temperature"] * (self.fault_intensity / 50)
            observations_map["transformer_primary_current"] += observations_map["transformer_primary_current"] * (self.fault_intensity / 10)
            observations_map["transformer_secondary_current"] = ((self.transformer.primary_windings / self.transformer.secondary_windings) * (observations_map["transformer_primary_current"])) - observations_map["transformer_secondary_current"] * (self.fault_intensity / 100)
            observations_map["transformer_efficiency"] = self.transformer.calculate_efficiency(observations_map["transformer_primary_voltage"], observations_map["transformer_primary_current"], observations_map["transformer_secondary_voltage"], observations_map["transformer_secondary_current"])
            if self.fault_intensity > 20:
                observations_map = self.transformer_power_off_state(observations_map)

        if fault_type == self.available_fault_types[4]:
            incorrect_secondary_windings = self.transformer.secondary_windings - (self.transformer.secondary_windings/10)
            observations_map["transformer_secondary_current"] = (self.transformer.primary_current * self.transformer.efficiency) * (self.transformer.primary_windings / (incorrect_secondary_windings + (self.transformer.secondary_windings/20)))
            observations_map["transformer_secondary_voltage"] = self.transformer.primary_voltage * (incorrect_secondary_windings / self.transformer.primary_windings)
            observations_map["transformer_temperature"] += observations_map["transformer_temperature"] * (5 / 100)
            observations_map["transformer_efficiency"] = self.transformer.calculate_efficiency(observations_map["transformer_primary_voltage"], observations_map["transformer_primary_current"], observations_map["transformer_secondary_voltage"], observations_map["transformer_secondary_current"])

        if fault_type == self.available_fault_types[5]:
            incorrect_secondary_windings = self.transformer.secondary_windings / 2
            observations_map["transformer_secondary_current"] = (self.transformer.primary_current * self.transformer.efficiency) * (self.transformer.primary_windings / incorrect_secondary_windings)
            observations_map["transformer_secondary_voltage"] = self.transformer.primary_voltage * (incorrect_secondary_windings / self.transformer.primary_windings)
        
        return observations_map


    def run_static_data_instance(self):
        static_transformer = Transformer()

        static_observations_map = { 
            "primary_windings" : static_transformer.primary_windings,
            "secondary_windings" : static_transformer.secondary_windings,
            "ambient_temperature" : static_transformer.ambient_temperature
        }
        return static_observations_map
