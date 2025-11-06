# store merged frame back on the instance attribute
from content import fred
from content import eod
import pandas as pd
import numpy as np
from content.admin import Database as db


class Transformations:
    def __init__(self):
        pass

    def periodical_change(self, s, months=12):
        shifted = s.shift(freq=pd.DateOffset(months=months))
        pct_change = (s - shifted) / shifted
        return pct_change

    def Changes(self, series_df:pd.DataFrame, date_col:str, value_col:str, attr_name:str, freq='YoY'):

        freqs = {'YoY':12, 'QoQ':3, 'MoM':1}

        # operate on a copy and ensure dates are datetime
        df = series_df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(by=date_col).set_index(date_col)

        # ensure numeric
        s = pd.to_numeric(df[value_col], errors='coerce')

        # shift by one calendar year and compute YoY
        change =self.periodical_change(s, months=freqs[freq])

        # turn into DataFrame keyed by date_col for merging
        change_df = change.to_frame(name= freq +'_' + value_col).reset_index()

        # merge into the attribute DataFrame (align by date)
        attr_df = getattr(self, attr_name)
        # ensure attr date column is datetime for proper merge
        attr_df = attr_df.copy()
        attr_df[date_col] = pd.to_datetime(attr_df[date_col])

        merged = attr_df.merge(change_df, on=date_col, how='left')

        # store merged frame back on the instance attribute
        setattr(self, attr_name, merged)

        return True

    def YoY(self, series_df:pd.DataFrame, date_col:str, value_col:str, attr_name:str):
        return self.Changes(series_df, date_col, value_col, attr_name, freq='YoY')

    def QoQ(self, series_df:pd.DataFrame, date_col:str, value_col:str, attr_name:str):
        return self.Changes(series_df, date_col, value_col, attr_name, freq='QoQ')

    def MoM(self, series_df:pd.DataFrame, date_col:str, value_col:str, attr_name:str):
        return self.Changes(series_df, date_col, value_col, attr_name, freq='MoM')

class EconData(Transformations):
    def __init__(self, series_ids:list, trans=['YoY', 'QoQ', 'MoM']):

        self.series_ids = series_ids
        self.observations = fred.Observations(self.series_ids)
        self.series_meta = fred.SeriesMeta(self.series_ids)
        self.series_release = fred.SeriesRelease(self.series_ids)
        self.trans = trans
        self.applied_trans = []

        self.get_data()

        Transformations.__init__(self)

    def get_data(self):
        raw_data = self.observations.data()

        self.historical_data = raw_data.groupby(['id', 'date'])['value'].last().unstack('id')
        self.historical_data.index = pd.to_datetime(self.historical_data.index)
        self.historical_data.reset_index(inplace=True)

        for column in self.historical_data.columns:
            temp = self.historical_data[['date', column]].dropna()
            self.__setattr__( column, temp)

        self.meta_data = self.series_meta.data()

        self.release_data = self.series_release.data()

        return True

    def apply_transformations(self):
        for tran in self.trans:
            if tran not in self.applied_trans:
                self.applied_trans.append(tran)
                for column in self.historical_data.columns:
                    if column != 'date':
                        self.__getattribute__(tran)(self.historical_data, 'date', column, column)
                else:
                    pass
    
        return True

    def api_json(self):
        series = {}
        for _ in self.series_ids:
            # copy the DataFrame and coerce types to JSON-safe values
            df = getattr(self, _).copy()

            # convert any pandas Timestamp-like objects to ISO strings for portability
            if 'date' in df.columns:
                try:
                    df['date'] = df['date'].apply(lambda x: x.isoformat() if hasattr(x, 'isoformat') else (str(x) if pd.notnull(x) else None))
                except Exception:
                    df['date'] = df['date'].astype(str)

            # replace infinite values with None and null-out NaNs
            df = df.replace([np.inf, -np.inf], None)
            df = df.where(pd.notnull(df), None)

            # convert numpy scalar types to native python types and ensure everything is JSON-serializable
            raw = df.to_dict(orient='list')
            safe = {}
            for k, lst in raw.items():
                safe_list = []
                for v in lst:
                    if v is None:
                        safe_list.append(None)
                        continue
                    # pandas / numpy timestamp
                    if hasattr(v, 'isoformat'):
                        try:
                            safe_list.append(v.isoformat())
                            continue
                        except Exception:
                            pass
                    # numpy scalar -> python native
                    if isinstance(v, (np.generic,)):
                        try:
                            safe_list.append(v.item())
                            continue
                        except Exception:
                            pass
                    # builtin numeric/bool/str
                    if isinstance(v, (int, float, str, bool)):
                        # guard against non-finite floats
                        if isinstance(v, float) and (np.isinf(v) or np.isnan(v)):
                            safe_list.append(None)
                        else:
                            safe_list.append(v)
                        continue
                    # fallback: stringify
                    try:
                        safe_list.append(float(v))
                    except Exception:
                        safe_list.append(str(v))
                safe[k] = safe_list

            series[_] = {
                'meta': self.meta_data[self.meta_data.id == _].to_dict(orient='records'),
                'data': safe
            }
        return series




class MarketData(Transformations):
    def __init__(self, symbol_list:list, from_date='1900-01-01', trans=['YoY']):
        self.symbols = symbol_list
        self.from_date = from_date
        self.trans = trans
        self.applied_trans = []


        self.get_historical()
        self.get_meta()

        Transformations.__init__(self)

    def get_historical(self):
        obj = eod.Historical()

        self.historical_data = obj.data(self.symbols, from_date=self.from_date)

        self.historical_data.date = pd.to_datetime(self.historical_data.date)

        for symbol in self.historical_data.symbol.unique():
            temp = self.historical_data[self.historical_data.symbol == symbol].drop(columns=['symbol']).set_index('date')
            self.__setattr__(symbol, temp.reset_index())

        return True

    def get_meta(self):
        db_obj = db('', [])

        with db_obj.engine().connect() as conn:
            query = f'''SELECT * FROM exchange WHERE code IN ({', '.join([f"'{sym}'" for sym in self.symbols])})'''
            self.meta_data = pd.read_sql_query(query, conn)

        return True


    def apply_transformations(self):
        for tran in self.trans:
            if tran not in self.applied_trans:
                self.applied_trans.append(tran)
                for symbol in self.symbols:
                    self.__getattribute__(tran)(getattr(self, symbol), 
                                                'date',
                                                'adjusted_close', 
                                                symbol)
                else:
                    pass
        return True

    def api_json(self):
        series = {}
        for symbol in self.symbols:
            series[symbol] = {
                'meta': self.meta_data[self.meta_data.code == symbol].to_dict(orient='records'),
                'data': getattr(self, symbol).to_dict(orient='list')
            }
        return series

class OverlayData(Transformations):
    def __init__(self, econ_ids:list, market_symbols:list, market_driver='adjusted_close',from_date='1900-01-01', trans=['YoY']):
        self.econ_ids = econ_ids
        self.market_symbols = market_symbols
        self.econ_data = EconData(econ_ids)
        self.market_data = MarketData(market_symbols, from_date=from_date)
        self.market_driver = market_driver
        self.trans = trans
        self.applied_trans = []


        self.get_historical()

        Transformations.__init__(self)

    def get_historical(self):
        self.econ_data.get_data()
        self.market_data.get_historical()

        econ_data = self.econ_data.historical_data.copy()
        econ_data.date = pd.to_datetime(econ_data.date)
        econ_data.set_index('date', inplace=True)

        market_data = self.market_data.historical_data.groupby(['symbol', 'date'])[self.market_driver].last().unstack('symbol')
        market_data.index = pd.to_datetime(market_data.index)

        min_date = min(econ_data.index.min(), market_data.index.min())
        max_date = max(econ_data.index.max(), market_data.index.max())
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')


        historical_data = pd.DataFrame(index=date_range)
        historical_data = historical_data.merge(econ_data, left_index=True, right_index=True, how='left')
        historical_data = historical_data.merge(market_data, left_index=True, right_index=True, how='left')
        historical_data.index.name = 'date'
        historical_data.reset_index(inplace=True)


        self.historical_data = historical_data

        return True


    def apply_transformations(self):
        for tran in self.trans:
            if tran not in self.applied_trans:
                self.applied_trans.append(tran)
                for _ in (self.econ_ids + self.market_symbols):
                    self.__getattribute__(tran)(getattr(self, 'historical_data'), 
                                                'date',
                                                _, 
                                                'historical_data')
                else:
                    pass
        return True 




