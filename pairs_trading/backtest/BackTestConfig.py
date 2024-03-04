from dataclasses import dataclass
import os
import yaml


@dataclass
class BackTestConfig:
    def __init__(self, configFile):
        cur_path = os.getcwd()
        with open(cur_path + '/config/' + configFile, 'r') as yamlConfig:
            config = yaml.load(yamlConfig, Loader=yaml.FullLoader)
            self.z_upper_limit = config.get('z_upper_limit')
            self.z_lower_limit = config.get('z_lower_limit')
            self.short_window = config.get('short_window')
            self.long_window = config.get('long_window')
            self.initial_capital = config.get('initial_capital')
            self.analysis_start_year = config.get('analysis_start_year')
            self.analysis_end_year = config.get('analysis_end_year')
            self.trading_folder_suffix = config.get('trading_folder_suffix')
