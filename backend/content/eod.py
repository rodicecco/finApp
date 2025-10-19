import pandas as pd
import source
from admin import Database
import re
from datetime import date, datetime, timedelta
import time

exchanges = ['NYSE', 'NYSE ARCA', 'NASDAQ', 'NYSE MKT']
priority_set = ['GSPC', 'SML', 'MID', 'DJC', 'DJI', 'DJT', 'DJU', 'SPSIRE']

#Update set that includes all the tickers in the exchanges listed above
def master_update_set(exch=exchanges):
    db = Database('_', [])

    exchanges_str = ','.join([f"'{x}'" for x in exch])
    query = f'''SELECT Code FROM exchange
                WHERE exchange IN ({exchanges_str})
                and type='Common Stock';'''
    
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute(query)
        resp = cur.fetchall()
    
    resp = [_[0] for _ in resp]
    return resp

#Update set that only takes tickers in the indexes listed above
def priority_update_set(inds:list = priority_set):
    db = Database('_', [])

    index_str = ','.join([f"'{x}'" for x in inds])
    query = f'''SELECT distinct code 
                FROM index_comps
                WHERE indcode IN ({index_str})
                and date = (SELECT max(date) FROM index_comps WHERE indcode IN ({index_str}));'''
    
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute(query)
        resp = cur.fetchall()
    
    resp = [_[0] for _ in resp]
    return resp

#Update set for indexes listed above
def master_index_update_set():
    db = Database('_', [])
    query = f'''SELECT code FROM indexes
                WHERE country='USA';'''
    
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute(query)
        resp = cur.fetchall()
    
    resp = [_[0]+'.INDX' for _ in resp]
    return resp   

#Update set for indexes listed above
def master_etf_update_set():
    resp = ['SPY', 'XLK', 'XLC', 'XLY', 'XLP', 'XLI', 'XLB', 'XLF', 'XLI', 'XLY', 'XLC', 'XLB', 'XLI', 'XLF', 'TLT', 'IWM', 'IJR', 'SPLG']
    return resp  


rem_ints = lambda x: re.sub(r'\d+', '', x)

#Function to move any integers to the end of the string when creating
#a sql table (Avoids conflict)
def move_integers_to_end(input_string):
    # Use regular expressions to find the leading integers
    match = re.match(r'(\d+)(.*)', input_string)
    if match:
        numbers = match.group(1)
        rest_of_string = match.group(2)
        # Concatenate the rest of the string with the leading integers at the end
        result = rest_of_string + numbers
    else:
        result = input_string
    return result

#Class to organize and post to database intraday price data
#for instruments in the EODData api
#NOTE: DESIGN SO IT CAN BE USED FOR INDEXES AS WELL AS STOCKS/ETFS
class Intraday(Database):

    def __init__(self, bulk = 'stock'):
        self.source = source.EODData()
        self.endpoint = self.source.intraday
        self.tables= {'index':'hist_index',
                        'stock':'historical',
                        'etf':'hist_etf'}
        self.table_name = self.tables[bulk]
        self.constraints = ['date', 'symbol']
        self.bulk = bulk
        self.ct = 0
        self.limit = 15
        self.sleep_time = 10
        self.sleep_ct = 100
        self.errors = []

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        diclis = []
        strip_ = lambda x: ''.join(x.split('.')[:-1])
        for _ in raw_data:
            try:
                temp = {'date': datetime.fromtimestamp(_['timestamp']).strftime('%Y-%m-%d'), 
                        'open' : _['open'],
                        'high' : _['high'],
                        'low' : _['low'],
                        'close' : _['close'],
                        'adjusted_close' : _['close'],
                        'volume' : _['volume'],
                        'symbol' : strip_(_['code'])}
                diclis.append(temp)
            except:
                print('Error in intraday data')
                self.errors.append(_['code'])
                print(_['code'])
                continue
        return diclis
    
    def data(self, symbols:list, **kwargs):
        raw_data = self.source.intraday(symbols, **kwargs)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(self.raw_data)

        self.data_ = frame
        
        cols = list(frame.columns)

        self.columns = frame[cols].columns
        self.dtypes = frame.dtypes.items()

        return frame

    #Function to run updates on the set specified
    def update_sequence(self):
        update_sets = {'index': master_index_update_set,
                        'stock': priority_update_set, 
                        'etf': master_etf_update_set}
        symbols = update_sets[self.bulk]()
        
        steps = self.limit

        ct=self.ct
        sleep_ct = 0
        while ct <= len(symbols):
            
            symbols_set = symbols[ct:min(ct+steps, len(symbols))]
            print(symbols_set)
            print(ct, ct+steps)
            self.data(symbols_set)
            self.create_table()
            self.upsert_async()

            ct+=steps
            sleep_ct+=1
            if sleep_ct == self.sleep_ct:
                time.sleep(self.sleep_time)
                sleep_ct = 0

        return True

#Class to organize and post to database historical price data
#for instruments in the EODData api
class Historical(Database):

    def __init__(self, update_set:object = priority_update_set):
        self.source = source.EODData()
        self.endpoint = self.source.historical
        self.table_name = 'historical'
        self.constraints = ['date', 'symbol']
        self.update_set = update_set

        self.ct = 0
        self.limit = 80
        self.sleep_time = 10
        self.sleep_ct = 3

        Database.__init__(self, self.table_name, self.constraints)
    
    #Function that verifies the last date of update in the
    #specified set of symbols
    def update_date(self, symbols:list):
        
        query = f'''WITH dates AS (SELECT MAX(date) AS max_date
                                    FROM {self.table_name}
                                    GROUP BY symbol)
                                    SELECT MIN(max_date)
                                    FROM dates;'''
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute(query)
            resp = cur.fetchone()

        return resp[0]

    def prep_raw(self, raw_data):
        diclis = []
        for _ in raw_data:
            for entry in raw_data[_]:
                temp = entry
                temp['symbol'] = _
                diclis.append(temp)
        return diclis
            
    #Function that creates dataframe and cleans data for final
    #posting in the database
    def data(self, symbols:list, filter:str=False, **kwargs):
        raw_data = self.source.historical(symbols,adj=True, **kwargs)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame()

        for _ in raw_data:
            temp = pd.DataFrame(raw_data[_])
            temp['symbol'] = _
            frame = pd.concat([frame, temp])

        if filter == False:
            frame = frame
        else:
            frame = frame.groupby(self.constraints).last()[filter]
            frame = frame.unstack('symbols').reset_index()
        
        self.data_ = frame
        
        cols = list(frame.columns)
        cols.remove('symbol')
        cols.append('symbol')

        self.columns = frame[cols].columns
        self.dtypes = frame.dtypes.items()

        return frame
    
    #Function to run updates on the set specified
    def update_sequence(self):
        symbols = self.update_set()
        from_date = self.update_date(symbols)
        from_date = (datetime.strptime(from_date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')
        
        steps = self.limit

        ct=self.ct
        sleep_ct = 0
        while ct <= len(symbols):
            symbols_set = symbols[ct:min(ct+steps, len(symbols))]
            print(ct, ct+steps)
            self.data(symbols_set, from_date=from_date)
            self.create_table()
            self.upsert_async()

            ct+=steps
            sleep_ct+=1
            if sleep_ct == self.sleep_ct:
                time.sleep(self.sleep_time)
                sleep_ct = 0

        return True

#Class to organize and post historical price data for indexes
class HistoricalIndex(Historical, Database):

    def __init__(self, update_set:object=master_index_update_set):

        self.update_set = update_set

        Historical.__init__(self, update_set = self.update_set)

        self.table_name = 'hist_index'
        self.constraints = ['symbol', 'date']

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        diclis = []
        for _ in raw_data:
            for entry in raw_data[_]:
                temp = entry
                temp['symbol'] = _.rstrip('.INDX')
                diclis.append(temp)
        return diclis

#Class to organize and post historical price data for indexes
class HistoricalETF(Historical, Database):

    def __init__(self, update_set:object=master_etf_update_set):

        self.update_set = update_set

        Historical.__init__(self, update_set = self.update_set)

        self.table_name = 'hist_etf'
        self.constraints = ['symbol', 'date']

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        diclis = []
        for _ in raw_data:
            for entry in raw_data[_]:
                temp = entry
                temp['symbol'] = _
                diclis.append(temp)
        return diclis

#Class to organize and post general data on the indexes specified
#in the priority set
class Indexes(Database):

    def __init__(self):
        self.source = source.EODData()
        self.endpoint = self.source.tickers
        self.table_name = 'indexes'
        self.constraints = ['Code']

        self.ct = 0
        self.limit = 500

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        diclis = []

        for _ in raw_data:
            for entry in raw_data[_]:
                diclis.append(entry)
        return diclis

    def data(self, exchange:str='INDX'):
        raw_data = self.endpoint(exchange=exchange)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(raw_data[exchange])

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()

        return frame

    def update_sequence(self):
        
        self.data()
        self.create_table()
        self.upsert_async()

        return True

#Class to organize and post the index components in the
#indexes specified in the priority set
class IndexComps(Database):

    def __init__(self, update_set:list = priority_set):
        self.source = source.EODData()
        self.endpoint = self.source.index_comps
        self.table_name = 'index_comps'

        self.constraints = ['indcode','code', 'date']

        self.update_set = update_set
        self.master_indx = [x+'.INDX' for x in self.update_set]

        self.ct = 0
        self.limit = 500

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        diclis = []

        for _ in raw_data:
            for entry in raw_data[_]:
                temp = raw_data[_][entry]
                temp['indcode'] = _.rstrip('.INDX')
                temp['date'] = date.today().strftime('%Y-%m-%d')
                diclis.append(temp)

        return diclis

    def data(self, inds:list):
        raw_data = self.endpoint(inds)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(self.raw_data)

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()

        return frame
    
    def update_sequence(self):
        self.data(self.master_indx)
        self.create_table()
        self.upsert_async()

        return True

#Class to organize and post all the tickers in the exchanges 
#specified in the master set of exchanges
class Tickers(Database):

    def __init__(self):
        self.source = source.EODData()
        self.endpoint = self.source.tickers
        self.table_name = 'exchange'
        self.constraints = ['Code']

        self.ct = 0
        self.limit = 500

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        diclis = []

        for _ in raw_data:
            for entry in raw_data[_]:
                diclis.append(entry)
        return diclis

    def data(self, exchange:str='US'):
        raw_data = self.endpoint(exchange=exchange)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(raw_data[exchange])

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()

        return frame

    def update_sequence(self):
        
        self.data()
        self.create_table()
        self.upsert_async()

        return True

#Class to organize and post data related to the GIC Sectors of the
#tickers included in the priority set
class Sectors(Database):

    def __init__(self, update_set:object = priority_update_set):
        self.source = source.EODData()
        self.endpoint = self.source.general_equity
        self.table_name = 'gic_sectors'
        self.constraints = ['Code']
        self.update_set = update_set

        self.ct = 0
        self.limit = 100
        self.cols_ = ['Code', 'GicSector', 'GicGroup', 'GicIndustry', 'GicSubIndustry']

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        dic_lis = []
        for _ in raw_data:
            temp = {}
            for entry in self.cols_:
                if entry not in raw_data[_].keys():
                    temp[entry] = None
                else:
                    temp[entry] = raw_data[_][entry]

            dic_lis.append(temp)
        
        return dic_lis
    
    def data(self, symbols:list, **kwargs):
        raw_data = self.endpoint(symbols, **kwargs)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame(self.raw_data)
        
        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()

        return frame
    
    def update_sequence(self):
        symbols = self.update_set()

        steps = self.limit

        ct=self.ct

        while ct <= len(symbols):
            symbols_set = symbols[ct:min(ct+steps, len(symbols))]
            print(ct, ct+steps)
            self.data(symbols_set)
            self.create_table()
            self.upsert_async()

            ct+=steps

        return True   

#Class to organize and post earnings historical data on the
#tickers in the priority set
class EarningsHist(Database):

    def __init__(self, update_set:object = priority_update_set):
        self.source = source.EODData()
        self.endpoint = self.source.earnings
        self.table_name = 'epshist'
        self.constraints = ['Code', 'date', 'reportDate']
        self.update_set = update_set

        self.ct = 0
        self.limit = 70

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        dic_lis = []
        errors = []
        for _ in raw_data:
            if raw_data[_] == 'NA':
                errors.append(_)
                #Skip symbols that do not have earnings data
                continue
            else:
                temp = {}
                for entry in raw_data[_]:
                    temp = raw_data[_][entry]
                    temp['code'] = _
                    dic_lis.append(temp)
        self.errors = errors
        return dic_lis

    def data(self, symbols:list, **kwargs):
        raw_data = self.endpoint(symbols, filters=['History'], **kwargs)
        self.raw_data = self.prep_raw(raw_data)
        frame = pd.DataFrame(self.raw_data)

        self.data_ = frame
        self.columns = frame.columns
        self.dtypes = frame.dtypes.items()

        return frame

    def update_sequence(self):
        symbols = self.update_set()
        steps = self.limit
        ct = self.ct

        while ct <= len(symbols):
            symbols_set = symbols[ct:min(ct+steps, len(symbols))]
            print(ct, ct+steps)
            self.data(symbols_set)
            self.create_table()
            self.upsert_async()

            ct+=steps

        return True  


#Classes to build and update tables based on bulk fundamentals
#Includes tables for highlights, valuation metrics, general information, technicals and dividends
class BulkGeneral(Database):

    def __init__(self, data):
        self.table_name = 'bulk_general'
        self.constraints = ['Code']
        self.data_ = data
        self.columns = self.data_.columns
        self.dtypes = self.data_.dtypes.items()

        Database.__init__(self, self.table_name, self.constraints)

class Highlights(Database):

    def __init__(self, data):
        self.table_name = 'highlights'
        self.constraints = ['Code']
        self.data_ = data
        self.columns = self.data_.columns
        self.dtypes = self.data_.dtypes.items()

        Database.__init__(self, self.table_name, self.constraints)

class Valuation(Database):

    def __init__(self, data):
        self.table_name = 'valuation'
        self.constraints = ['Code']
        self.data_ = data
        self.columns = self.data_.columns
        self.dtypes = self.data_.dtypes.items()

        Database.__init__(self, self.table_name, self.constraints)

class Technicals(Database):

    def __init__(self, data):
        self.table_name = 'technicals'
        self.constraints = ['Code']
        self.data_ = data
        self.columns = self.data_.columns
        self.dtypes = self.data_.dtypes.items()

        Database.__init__(self, self.table_name, self.constraints)

class Dividends(Database):

    def __init__(self, data):
        self.table_name = 'dividends'
        self.constraints = ['Code']
        self.data_ = data
        self.columns = self.data_.columns
        self.dtypes = self.data_.dtypes.items()

        Database.__init__(self, self.table_name, self.constraints)

class BulkFund(Database):
    
    def __init__(self, update_set:object = priority_update_set):
        self.source = source.EODData()
        self.endpoint = self.source.bulk_fundamental

        self.sets = {'General': BulkGeneral,
                     'Valuation': Valuation, 
                     'Highlights':Highlights,
                     'Technicals' : Technicals,
                     'SplitsDividends': Dividends}
        
        self.update_set = update_set
        self.limit = 450
        self.ct = 0
        self.table_name = 'None'
        self.constraints = []

        Database.__init__(self, self.table_name, self.constraints)

    def data(self, symbols:list,  **kwargs):
        raw_data = self.endpoint(symbols=symbols, **kwargs)['bulk']
        self.raw_data = raw_data

        dic = {}
        for _ in self.sets.keys():
            dic[_] = []

        for _ in raw_data:
            code  = raw_data[_]['General']['Code']
            for subset in raw_data[_]:
                if subset in self.sets.keys():
                    temp = raw_data[_][subset]
                    temp['Code'] = code
                    dic[subset].append(temp)
                else:
                    continue
        
        for _ in dic:
            frame = pd.DataFrame(dic[_])
            frame.columns = [move_integers_to_end(x) for x in frame.columns]
            setattr(self, _ , self.sets[_](frame))
            att = getattr(self, _ )
            att.raw_data = dic[_]

        for _ in self.sets.keys():
            att = getattr(self, _)
            raw_data = att.raw_data

            diclis = []
            for entry in raw_data:

                temp = {}
                keys = [move_integers_to_end(x) for x in entry.keys()]
                alt_entry = dict(zip(keys, entry.values()))

                for col in att.columns:
                    if col not in alt_entry.keys():
                        temp[col] = None
                    else:                   
                        temp[col] = alt_entry[col]
                    
                diclis.append(temp)
            att.raw_data = diclis

        return dic

    def update_sequence(self):
        symbols = self.update_set()
        steps = self.limit

        ct=self.ct
        while ct <= len(symbols):
            symbols_set = symbols[ct:min(ct+steps, len(symbols))]
            print(ct, ct+steps)
            self.data(symbols_set)
            
            for obj in self.sets:
    
                att = getattr(self, obj)
                att.create_table()         
                att.upsert_async()

            ct+=steps

        return True



#Classes to organize and post financial statement data
#on the symbols in the priority set
#Includes balance sheet, income statement and cash flow statemenet
class BalanceSheet(Database):

    def __init__(self, data):
        self.table_name = 'balance_sheet'
        self.constraints = ['Code', 'date']
        self.data_ = data
        self.columns = self.data_.columns
        self.dtypes = self.data_.dtypes.items()

        Database.__init__(self, self.table_name, self.constraints)


class Income(Database):

    def __init__(self, data):
        self.table_name = 'income_st'
        self.constraints = ['Code', 'date']
        self.data_ = data
        self.columns = self.data_.columns
        self.dtypes = self.data_.dtypes.items()

        Database.__init__(self, self.table_name, self.constraints)

class CashFlow(Database):

    def __init__(self, data):
        self.table_name = 'cash_flow_st'
        self.constraints = ['Code', 'date']
        self.data_ = data
        self.columns = self.data_.columns
        self.dtypes = self.data_.dtypes.items()

        Database.__init__(self, self.table_name, self.constraints)

class Financials(Database):
    
    def __init__(self, update_set:object = priority_update_set):
        self.source = source.EODData()
        self.endpoint = self.source.financial

        self.sets = {'Balance_Sheet': BalanceSheet,
                     'Cash_Flow': CashFlow, 
                     'Income_Statement':Income}
        
        self.update_set = update_set
        self.limit = 100
        self.ct = 0
        self.table_name = 'None'
        self.constraints = []

        Database.__init__(self, self.table_name, self.constraints)

    def data(self, symbols:list,  **kwargs):
        raw_data = self.endpoint(symbols=symbols, **kwargs)
        self.raw_data = raw_data

        dic = {}
        for _ in self.sets.keys():
            dic[_] = []

        for _ in raw_data:
            for subset in raw_data[_]:
                if subset in self.sets.keys():
                    for entry in raw_data[_][subset]['quarterly']:
                        temp = raw_data[_][subset]['quarterly'][entry]
                        temp['Code'] = _
                        dic[subset].append(temp)
                else:
                    continue
        
        for _ in dic:
            frame = pd.DataFrame(dic[_])
            frame.columns = [move_integers_to_end(x) for x in frame.columns]
            setattr(self, _ , self.sets[_](frame))
            att = getattr(self, _ )
            att.raw_data = dic[_]

        for _ in self.sets.keys():
            att = getattr(self, _)
            raw_data = att.raw_data

            diclis = []
            for entry in raw_data:

                temp = {}
                keys = [move_integers_to_end(x) for x in entry.keys()]
                alt_entry = dict(zip(keys, entry.values()))

                for col in att.columns:
                    if col not in alt_entry.keys():
                        temp[col] = None
                    else:                   
                        temp[col] = alt_entry[col]
                    
                diclis.append(temp)
            att.raw_data = diclis

        return dic

    def update_sequence(self):
        symbols = self.update_set()
        steps = self.limit

        ct=self.ct
        while ct <= len(symbols):
            symbols_set = symbols[ct:min(ct+steps, len(symbols))]
            print(ct, ct+steps)
            self.data(symbols_set)
            
            for obj in self.sets:
    
                att = getattr(self, obj)
                att.create_table()         
                att.upsert_async()

            ct+=steps

        return True