from dataclasses import dataclass

@dataclass
class CointegratedPairs:
    def __init__(self, sector, start_date, end_date, pairs):
        self.sector = sector
        self.start_date = start_date
        self.end_date = end_date
        self.pairs = pairs

    def __str__(self):
        return f"sector = {self.sector}, start date = {self.start_date}, end date = {self.end_date}, pairs = {self.pairs}"