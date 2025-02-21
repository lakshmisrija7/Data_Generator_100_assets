
class Transformer:
    def __init__(self):
        self.primary_voltage = 10_000
        self.primary_current = 60
        self.secondary_voltage = None
        self.secondary_current = None
        self.primary_windings = 200
        self.secondary_windings = 6000
        self.num_phases = 3
        self.efficiency = 0.95

        self.ambient_temperature = 288.15
        self.temperature = 315
        self.earthing_voltage = 0
        self.rated_current = 60
        self.rated_voltage = 10_000

    def calculate_secondary_voltage(self):
        self.secondary_voltage = self.primary_voltage * (self.secondary_windings / self.primary_windings)

    def calculate_secondary_current(self):
        self.secondary_current = (self.primary_current * self.efficiency) * (self.primary_windings / self.secondary_windings)

    def calculate_efficiency(self, primary_voltage, primary_current, secondary_voltage, secondary_current):
        return ((secondary_voltage * secondary_current) / (primary_voltage * primary_current)) * 100
