import numpy as np
import random
import time
from static.HeatExchanger import HeatExchanger
from static.utils import Conversions

class HeatExchangerDigitalTwin:
    def __init__(self, primary_fluid_inlet_mass_flow, secondary_fluid_inlet_mass_flow, heat_transfer_surface_area):
        self.primary_fluid_inlet_mass_flow = primary_fluid_inlet_mass_flow
        self.secondary_fluid_inlet_mass_flow = secondary_fluid_inlet_mass_flow
        self.available_fault_types = ["water_inlet_clog", "foul_accumulation", "hot_cooling_fluid", "shell_overpressure"]
        self.heat_exchanger = HeatExchanger(heat_transfer_surface_area, primary_fluid_inlet_mass_flow, secondary_fluid_inlet_mass_flow)
        self.heat_exchanger.setup_heat_exchanger()
        self.water_inlet_clog_fault_coefficient = 55 * 3
        self.hot_cooling_fluid_fault_coefficient = 0.25 * 3
        self.foul_accumulation_fault_coefficient = 0.25 * 3
        self.shell_overpressure_fault_coeficient = 0.04 * 3
        self.fault_intensity = 1

    def update_fault_intensity(self, fault_type):
        if fault_type is not None and self.fault_intensity < 30:
            self.fault_intensity += 1
        if fault_type is None:
            self.fault_intensity = 1

    def run_instance(self, primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, primary_fluid_inlet_pressure, fault_type):
        self.update_fault_intensity(fault_type)

        if fault_type == "water_inlet_clog":
            secondary_fluid_mass_flow = secondary_fluid_mass_flow - (self.fault_intensity * self.water_inlet_clog_fault_coefficient)

        if fault_type == "hot_cooling_fluid":
            secondary_fluid_inlet_temperature = secondary_fluid_inlet_temperature + (self.hot_cooling_fluid_fault_coefficient * self.fault_intensity)

        if fault_type == "shell_overpressure":
            primary_fluid_inlet_pressure = primary_fluid_inlet_pressure + (self.fault_intensity * self.shell_overpressure_fault_coeficient * 100_000)

        primary_fluid_outlet_temperature, secondary_fluid_outlet_temperature = self.heat_exchanger.get_outlet_temperatures(primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, primary_fluid_mass_flow, secondary_fluid_mass_flow, self.heat_exchanger.heat_transfer_coefficient, self.heat_exchanger.heat_transfer_surface_area)
        if fault_type == "foul_accumulation":
            primary_fluid_outlet_temperature += abs(primary_fluid_inlet_temperature - primary_fluid_outlet_temperature) * ((self.fault_intensity * self.foul_accumulation_fault_coefficient) / 100)
            secondary_fluid_outlet_temperature -= abs(secondary_fluid_inlet_temperature - secondary_fluid_outlet_temperature) * ((self.fault_intensity * self.foul_accumulation_fault_coefficient) / 100)

        primary_fluid_inlet_volume_flow, primary_fluid_outlet_volume_flow = self.heat_exchanger.get_primary_fluid_volume_flows(primary_fluid_mass_flow, primary_fluid_inlet_temperature, primary_fluid_outlet_temperature, primary_fluid_inlet_pressure)

        if fault_type == "shell_overpressure" and self.fault_intensity > 25:
            primary_fluid_outlet_temperature, secondary_fluid_outlet_temperature, primary_fluid_inlet_volume_flow, primary_fluid_outlet_volume_flow, primary_fluid_inlet_temperature, secondary_fluid_inlet_temperature, secondary_fluid_mass_flow, primary_fluid_inlet_pressure = 0, 0, 0, 0, 0, 0, 0, 0

        observations_map = {    
            "heat_exchanger_primary_fluid_outlet_temperature": round(Conversions.kelvin_to_fahrenheit(primary_fluid_outlet_temperature), 2), 
            "heat_exchanger_secondary_fluid_outlet_temperature": round(Conversions.kelvin_to_fahrenheit(secondary_fluid_outlet_temperature), 2), 
            "heat_exchanger_primary_fluid_inlet_volume_flow": round(primary_fluid_inlet_volume_flow, 2), 
            "heat_exchanger_primary_fluid_outlet_volume_flow": round(primary_fluid_outlet_volume_flow, 2),
            "heat_exchanger_primary_fluid_inlet_temperature": round(Conversions.kelvin_to_fahrenheit(primary_fluid_inlet_temperature), 2),
            "heat_exchanger_secondary_fluid_inlet_temperature": round(Conversions.kelvin_to_fahrenheit(secondary_fluid_inlet_temperature), 2),
            "heat_exchanger_secondary_fluid_mass_flow": round(secondary_fluid_mass_flow, 2),
            "heat_exchanger_primary_fluid_inlet_pressure": round(primary_fluid_inlet_pressure, 2),
            "heat_exchanger_primary_fluid_mass_flow": round(primary_fluid_mass_flow, 2)
        }

        # return primary_fluid_outlet_temperature, secondary_fluid_outlet_temperature, primary_fluid_inlet_volume_flow, primary_fluid_outlet_volume_flow
        return observations_map
