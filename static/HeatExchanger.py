import numpy as np
import random
import time
import math
from scipy.optimize import fsolve

class HeatExchanger:
    def __init__(self, heat_transfer_surface_area, primary_fluid_inlet_mass_flow, secondary_fluid_inlet_mass_flow):
        self.heat_transfer_surface_area = heat_transfer_surface_area
        self.primary_fluid_inlet_mass_flow = primary_fluid_inlet_mass_flow
        self.secondary_fluid_inlet_mass_flow = secondary_fluid_inlet_mass_flow
        self.assumed_primary_fluid_inlet_temperature = 573 # K
        self.assumed_primary_fluid_outlet_temperature = 473 # K
        self.ambient_temperature = 288 # K
        self.ambient_pressure = 101325 # Pa
        self.primary_fluid_specific_heat = 1.005 # Cp of air: (kJ/kg·K)
        self.secondary_fluid_specific_heat = 4.186 # Cp of water: (kJ/kg·K)

    def set_secondary_fluid_inlet_temperature_as_ambient(self):
        self.secondary_fluid_inlet_temperature = self.ambient_temperature

    def calculate_heat_load(self):
        heat_loss_from_primary_fluid = self.primary_fluid_inlet_mass_flow * self.primary_fluid_specific_heat * abs(self.assumed_primary_fluid_inlet_temperature - self.assumed_primary_fluid_outlet_temperature)
        self.heat_load = heat_loss_from_primary_fluid

    def calculate_secondary_fluid_outlet_temperature(self):
        secondary_fluid_inlet_temperature = self.secondary_fluid_inlet_temperature + self.heat_load / (self.secondary_fluid_inlet_mass_flow * self.secondary_fluid_specific_heat)
        self.secondary_fluid_outlet_temperature = secondary_fluid_inlet_temperature

    def get_log_mean_temperature_difference(self):
        return ((abs(self.secondary_fluid_inlet_temperature-self.assumed_primary_fluid_outlet_temperature)-abs(self.secondary_fluid_outlet_temperature-self.assumed_primary_fluid_inlet_temperature)) / (math.log(abs(self.secondary_fluid_inlet_temperature-self.assumed_primary_fluid_outlet_temperature)/abs(self.secondary_fluid_outlet_temperature-self.assumed_primary_fluid_inlet_temperature))))

    def calculate_heat_transfer_coefficient(self):
        # In shell - air (primary fluid)
        # In tube - water (secondary fluid)
        log_mean_temperature_difference = self.get_log_mean_temperature_difference()
        heat_transfer_coefficient = (self.heat_load) / (self.heat_transfer_surface_area * log_mean_temperature_difference)
        self.heat_transfer_coefficient = heat_transfer_coefficient

    def setup_heat_exchanger(self):
        self.set_secondary_fluid_inlet_temperature_as_ambient()
        self.calculate_heat_load()
        self.calculate_secondary_fluid_outlet_temperature()
        self.calculate_heat_transfer_coefficient()

    def get_outlet_temperatures(self, T_air_in, T_water_in, current_primary_fluid_mass_flow, current_secondary_fluid_mass_flow, U, A):
        T_air_out_guess = T_air_in - (abs(T_air_in-T_water_in)*0.1)
        T_water_out_guess = T_water_in + (abs(T_air_in-T_water_in)*0.05)
        
        def heat_balance(T_air_out, T_water_out):
            Q_air = current_primary_fluid_mass_flow * self.primary_fluid_specific_heat * (T_air_in - T_air_out)
            Q_water = current_secondary_fluid_mass_flow * self.secondary_fluid_specific_heat * (T_water_out - T_water_in)
            self.current_heat_load = Q_air
            return Q_air - Q_water
        
        def equations(variables):
            T_air_out, T_water_out = variables
            Q = current_primary_fluid_mass_flow * self.primary_fluid_specific_heat * (T_air_in - T_air_out)
            delta_T1 = T_air_in - T_water_out
            delta_T2 = T_air_out - T_water_in
            delta_Tm = (delta_T1 - delta_T2) / np.log(delta_T1 / delta_T2)
            Q_calc = U * A * delta_Tm
            return [Q - Q_calc, heat_balance(T_air_out, T_water_out)]
        
        T_air_out, T_water_out = fsolve(equations, (T_air_out_guess, T_water_out_guess))
        
        return T_air_out, T_water_out

    def get_primary_fluid_volume_flows(self, primary_fluid_inlet_mass_flow, primary_fluid_inlet_temperature, primary_fluid_outlet_temperature, primary_fluid_inlet_pressure):

        def get_volume_flow_of_air(air_mass_flow, air_temperature, air_pressure, gas_constant=287):
            air_volume_flow = (air_mass_flow * gas_constant * air_temperature) / air_pressure
            return air_volume_flow

        primary_fluid_inlet_volume_flow = get_volume_flow_of_air(primary_fluid_inlet_mass_flow, primary_fluid_inlet_temperature, primary_fluid_inlet_pressure)
        primary_fluid_outlet_volume_flow = primary_fluid_inlet_volume_flow * (primary_fluid_outlet_temperature / primary_fluid_inlet_temperature)

        return primary_fluid_inlet_volume_flow, primary_fluid_outlet_volume_flow

