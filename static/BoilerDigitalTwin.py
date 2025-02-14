import numpy as np
import random
import math
from scipy import interpolate

"""
Amount of Fuel .
Fuel calorific value .
Water intake temperature .
Water intake pressure .
Outlet steam temperature .
Outlet Steam pressure .
Outlet Steam Flow rate .
Water pH .
Water hardess .
"""


class PumpPower:
    def __init__(self):
        pass

class Boiler:
    def __init__(self):
        self.steam_table = {
            'pressure': np.array([0.1, 0.5, 1.0, 5.0, 10.0, 20.0, 30.0, 40.0, 80.0, 120, 200]),  # in bar
            'temperature': np.array([45.8, 81.3, 99.6, 151.9, 179.9, 212.4, 232, 249, 293.6, 323, 364]),  # in °C
            'enthalpy': np.array([2584.78, 2645.99, 2675, 2747.54, 2776.16, 2797.21, 2802.27, 2801.1, 2761.2, 2690.2, 2437.5])  # in kJ/kg
        }
        self.temperature_interp = interpolate.interp1d(self.steam_table['pressure'], self.steam_table['temperature'], fill_value="extrapolate")
        self.enthalpy_interp = interpolate.interp1d(self.steam_table['pressure'], self.steam_table['enthalpy'], fill_value="extrapolate")
        self.temperature_data = np.array([
            0.01, 5, 10, 15, 20, 25, 30, 35, 40, 45,
            50, 60, 70, 80, 90,
            100, 110, 120, 130, 140,
            150, 170, 190,
            210, 250, 280,
            320, 374.14
        ])
        self.enthalpy_data = np.array([
            0.01, 20.98, 42.01, 62.99, 83.96, 104.89, 125.79, 146.68, 167.97, 188.45,
            209.33, 251.13, 292.98, 334.91, 376.92,
            419.04, 461.3, 503.71, 546.31, 589.13,
            632.20, 719.21, 807.62,
            897.76, 1085.4, 1236,
            1461.5, 2099.3
        ])
        self.Tfw_data = np.array([0.01, 2, 4, 10, 14, 18, 20, 25,
            30, 34, 40, 44, 50, 54, 60, 70,
            80, 90, 96, 100, 110, 120, 130, 140, 150,
            160, 180, 200, 220, 240, 260, 280,
            300, 320, 340, 360, 370
        ])
        self.Pfw_data = np.array([0.61165, 0.70599, 0.81355, 1.2282, 1.5990,
            2.0647, 2.3393, 3.1699, 4.2470, 5.3251, 7.3849, 9.1124, 12.352, 15.022, 19.946, 31.201,
            47.414, 70.182, 87.771, 101.42, 143.38, 198.67, 270.28, 361.54, 476.16, 618.23, 1002.8,
            1554.9, 2319.6, 3346.9, 4692.3, 6416.6, 8587.9, 11284, 14601, 18666, 21044
        ])
        self.enthalpy_interp_Pfw = interpolate.interp1d(self.Tfw_data, self.Pfw_data, fill_value="extrapolate")
        self.enthalpy_interp_w = interpolate.interp1d(self.temperature_data, self.enthalpy_data, fill_value="extrapolate")
        self.overhead = 0
        self.Q = 0.256793*(10**(3+3))  # Boiler heat input in kJ/s
        self.Fw = 100  # Feedwater flow rate in kg/s
        self.Tfw = 150  # Feedwater temperature in °C
        self.Pb = 160  # Boiler pressure in bar
        self.eff = random.randint(8400,8500)/10000
        self.quantity = 5.86 #kg/s
        self.calorific = 44000 #kJ/kg
        self.pH = 8.5
        self.ppm = 0.7
        self.contin = 0

    def getExpectedData(self):
        temp_Q,temp_Fw,temp_Tfw,temp_pb,temp_eff = self.normal_data()
        Ps = temp_pb
        Ts = self.temperature_interp(Ps)
        Ts1 = float(Ts)
        hf = self.enthalpy_interp_w(temp_Tfw)
        hg = self.enthalpy_interp(Ps)
        Pfw = float(self.enthalpy_interp_Pfw(temp_Tfw))
        delta_h = hg - hf
        Fs = (temp_Q / delta_h)*temp_eff  # in kg/s
        energy_output = Fs * delta_h
        efficiency = (energy_output / temp_Q) * 100  # in percentage
        return Fs, Ts1, Ps, efficiency, temp_Tfw, Pfw
    
    def getData(self, status):
        if(status == None):
            self.contin = 0
        else:
            self.contin +=1
        if(status == None):
            self.normal_condition()
            self.bumba = False
        else:
            if (status == 2 or status == 4):
                if self.contin < 10:
                    if(status == 2):
                        self.heater_tube_leakage()
                    elif(status == 4):
                        self.excess_air()
                else:
                    self.bumba = True
            if(self.contin<25):
                if(status == 1):
                    self.steam_trap_failure_fault()
                elif(status == 5):
                    self.burner_malfunction()
                elif(status == 3):
                    self.scaling_and_fouling()
                self.bumba = False
            else:
                self.bumba = True
        if(self.bumba == True):
            if(status == 1):
                temp_pb = self.Pb + random.randint(-5,1)
                temp_eff = self.eff + random.randint(-3,1)/1000
                temp_Tfw = self.Tfw
                temp_Q = self.Q
            elif(status == 2):
                temp_pb = self.Pb
                temp_Tfw = self.Tfw + random.randint(-5,1)
                temp_eff = self.eff + random.randint(-3,1)/1000
                temp_Q = self.Q
            elif(status == 3):
                temp_pb = self.Pb
                temp_eff = self.eff + random.randint(-3,1)/1000
                temp_Tfw = self.Tfw
                temp_Q = self.Q
            elif(status == 4):
                temp_pb = self.Pb
                temp_eff = self.eff + random.randint(-3,1)/1000
                temp_Tfw = self.Tfw
                temp_Q = self.Q
            elif(status == 5):
                temp_pb = self.Pb
                temp_eff = self.eff + random.randint(-3,1)/1000
                temp_Tfw = self.Tfw
                temp_Q = self.Q + (random.randint(-10,10))
        else:
            temp_pb = self.Pb
            temp_eff = self.eff + random.randint(-3,1)/1000
            temp_Tfw = self.Tfw
            temp_Q = self.Q
        Ps = temp_pb
        Ts = self.temperature_interp(Ps)
        Ts1 = float(Ts)
        # print(Ts1)
        hf = self.enthalpy_interp_w(temp_Tfw)
        hg = self.enthalpy_interp(Ps)
        Pfw = float(self.enthalpy_interp_Pfw(temp_Tfw))
        delta_h = hg - hf
        Fs = (temp_Q / delta_h)*temp_eff  # in kg/s
        energy_output = Fs * delta_h
        # print(energy_output,temp_Q)
        efficiency = (energy_output / temp_Q) * 100  # in percentage
        expected_Fs, expected_Ts, expected_Ps, expected_eff, expected_Tfw, expected_Pfw = self.getExpectedData()
        return expected_Fs, expected_Ts, expected_Ps, expected_eff, expected_Tfw, expected_Pfw, Fs, Ts1, Ps, efficiency, temp_Tfw, Pfw
    
    def steam_trap_failure_fault(self):
        self.Pb *= (random.randint(980,1000)/1000)  # 10% decrease
        self.eff *= 1

    def heater_tube_leakage(self):
        self.Tfw *= (random.randint(990,1000)/1000)  # 20°C drop
        self.eff *= (random.randint(980,1000)/1000)
        
    def scaling_and_fouling(self):
        self.Q *= 1.0  # Increase heat input to maintain output, implying reduced efficiency
        self.eff *= (random.randint(980,1000)/1000)
        
    def excess_air(self):
        self.Q *= 1.0  # More fuel needed due to less efficient combustion
        self.eff *= (random.randint(980,1000)/1000)
        
    def burner_malfunction(self):
        self.Q *= (random.randint(980,1000)/1000)  # Reduced heat input due to inefficient combustion
        self.eff *= 1
        
    def normal_condition(self):
        self.overhead = 0
        self.Q = 0.256793*(10**(3+3))  # Boiler heat input in kJ/s
        self.Fw = 100  # Feedwater flow rate in kg/s
        self.Tfw = 150  # Feedwater temperature in °C
        self.Pb = 160  # Boiler pressure in bar
        self.eff = random.randint(8400,8500)/10000

    def normal_data(self):
        Q = 0.256793*(10**(3+3))  # Boiler heat input in kJ/s
        Fw = 100  # Feedwater flow rate in kg/s
        Tfw = 150  # Feedwater temperature in °C
        Pb = 160  # Boiler pressure in bar
        eff = random.randint(8400,8500)/10000
        return Q,Fw,Tfw,Pb,eff
    
    def getStaticData(self):
        return self.quantity, self.calorific, self.pH, self.ppm