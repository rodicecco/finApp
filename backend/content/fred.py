import pandas as pd
from . import source
from .admin import Database
import re
from datetime import date, datetime, timedelta
import time

class Releases(Database):

    def __init__(self):
        self.source = source.FREDData()
        self.endpoint = self.source.releases
        self.table_name = 'econ_releases'
        self.constraints = ['id']

        self.ct = 0
        self.limit = 500
        self.cols_ = ['id', 
                      'realtime_start', 
                      'realtime_end', 
                      'name', 
                      'press_release', 
                      'link', 
                      'notes']

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        raw_data = raw_data['releases']['releases']
        dic_lis = []
        for _ in raw_data:
            temp = {}
            for entry in self.cols_:
                if entry not in _.keys():
                    temp[entry] = None
                else:
                    temp[entry] = _[entry]

            dic_lis.append(temp)
        return dic_lis

    def data(self):
        raw_data = self.endpoint()
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(self.raw_data)

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()

        return frame

    def update_sequence(self):
        
        self.data()
        self.create_table()
        self.upsert_async()

        return True
    
class Series(Database):

    def __init__(self, release_ids:list):
        self.source = source.FREDData()
        self.endpoint = self.source.series
        self.table_name = 'econ_series'
        self.constraints = ['id']
        self.release_ids = release_ids

        self.ct = 0
        self.limit = 500
        self.cols_ = ['id',
                      'title', 
                      'realtime_start', 
                      'realtime_end', 
                      'observation_start', 
                      'observation_end', 
                      'frequency', 
                      'units', 
                      'seasonal_adjustment', 
                      'last_updated', 
                      'popularity', 
                      'group_popularity']

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        raw_data = raw_data
        dic_lis = []
        for _ in raw_data:
            temp_data = raw_data[_]['seriess']
            
            for entry in temp_data:
                temp = {'release_id': _ }
                for row in self.cols_:
                    if row not in entry.keys():
                        temp[row] = None
                    else:
                        temp[row] = entry[row]

                dic_lis.append(temp)

        return dic_lis

    def data(self):
        raw_data = self.endpoint(self.release_ids)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(self.raw_data)

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()

        return frame

    def update_sequence(self):
        
        self.data()
        self.create_table()
        self.upsert_async()

        return True 
    
class Observations(Database):

    def __init__(self, series_ids:list):
        self.source = source.FREDData()
        self.endpoint = self.source.observ
        self.table_name = 'econ_hist'
        self.constraints = ['id', 'date']
        self.series_ids = series_ids
        self.cols_ = [ 
                      'realtime_start', 
                      'realtime_end', 
                      'date', 
                      'value']


        self.ct = 0
        self.limit = 500


        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        raw_data = raw_data
        dic_lis = []
        for _ in raw_data:
            temp_data = raw_data[_]['observations']
            
            for entry in temp_data:
                temp = {'id': _ }
                for row in self.cols_:
                    if row not in entry.keys():
                        temp[row] = None
                    else:
                        temp[row] = entry[row]

                dic_lis.append(temp)

        return dic_lis

    def data(self):
        raw_data = self.endpoint(self.series_ids)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(self.raw_data)
        frame.value = pd.to_numeric(frame.value, errors='coerce')

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()


        return frame

    def update_sequence(self):
        
        self.data()
        self.create_table()
        self.upsert_async()

        return True 
    

class SeriesMeta(Database):

    def __init__(self, series_ids:list):
        self.source = source.FREDData()
        self.endpoint = self.source.series_meta
        self.table_name = 'econ_series_meta'
        self.constraints = ['id']
        self.series_ids = series_ids

        self.ct = 0
        self.limit = 500
        self.cols_ = ['id',
                      'title', 
                      'realtime_start', 
                      'realtime_end', 
                      'observation_start', 
                      'observation_end', 
                      'frequency', 
                      'units', 
                      'seasonal_adjustment', 
                      'last_updated', 
                      'popularity', 
                      'notes']

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        raw_data = raw_data
        dic_lis = []
        for _ in raw_data:
            temp_data = raw_data[_]['seriess']
            
            for entry in temp_data:
                temp = {}
                for row in self.cols_:
                    if row not in entry.keys():
                        temp[row] = None
                    else:
                        temp[row] = entry[row]

                dic_lis.append(temp)

        return dic_lis

    def data(self):
        raw_data = self.endpoint(self.series_ids)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(self.raw_data)

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()


        return frame

    def update_sequence(self):
        
        self.data()
        self.create_table()
        self.upsert_async()

        return True 
    

class SeriesRelease(Database):

    def __init__(self, release_ids:list):
        self.source = source.FREDData()
        self.endpoint = self.source.release_series
        self.table_name = 'econ_series_release'
        self.constraints = ['id']
        self.release_ids = release_ids

        self.ct = 0
        self.limit = 500
        self.cols_ = ['id',
                      'name', 
                      'press_release', 
                      'link']

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        raw_data = raw_data
        dic_lis = []
        for _ in raw_data:
            temp_data = raw_data[_]['releases'][0]
            temp_data['series_id'] = _
            dic_lis.append(temp_data)

        return dic_lis

    def data(self):
        raw_data = self.endpoint(self.release_ids)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(self.raw_data)

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()


        return frame

    def update_sequence(self):
        
        self.data()
        self.create_table()
        self.upsert_async()

        return True