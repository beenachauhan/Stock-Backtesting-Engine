from dataclasses import dataclass
import os
import yaml


@dataclass
class DBConfig:
    def __init__(self, configFile):
        cur_path = os.getcwd()
        with open(cur_path + '/config/' + configFile, 'r') as yamlConfig:
            config = yaml.load(yamlConfig, Loader=yaml.FullLoader)
            self.db_host = config.get('host')
            self.db_user = config.get('user')
            self.db_password = config.get('password')
            self.db_name = config.get('db_name')
            self.db_file = config.get('db_file')
