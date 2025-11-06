
import requests
from . import secret
import aiohttp
import asyncio
import nest_asyncio

nest_asyncio.apply()

#Class to hold functions applicable to all APIs
class BaseRequests:
    def __init__(self):
        pass

    #Function to build dictionary with all attributes of the payload
    def build_params(self, main_payload, adj=False,**kwargs):
        payload = main_payload.copy()
        if adj == False:
            for key, value in kwargs.items():
                payload[key] = value
            return payload
        else:     
            for key, value in kwargs.items():
                if key == 'from_date':
                    payload['from'] = value
                else:
                    payload[key] = value
            return payload
        
    #Function to make bulk api requests synchronously
    def sync_request(self, params:dict):
        with requests.Session() as session:
            dic = {}
            for symbol in params:
                url, payload = params[symbol]
                dic[symbol] = session.get(url = url, params=payload).json()
            return dic 
    
    #Function to assemble api requests asynchronously
    async def async_fetch_data(self, session, url, payload):
        async with session.get(url, params=payload) as response:
            return await response.json()
        
    #Function to make bulk api requests asynchronously
    async def async_setup(self, params:dict):
        async with aiohttp.ClientSession() as session:
            tasks=[]
            for key in params:
                url, payload = params[key]
                tasks.append(self.async_fetch_data(session, url, payload))
            
            responses = await asyncio.gather(*tasks)
            return dict(zip(params.keys(), responses))
        

    def select_request(self, params, asyn=True):
        if asyn == True:
            responses = asyncio.run(self.async_setup(params))
        else:
            responses = self.sync_request(params)
        return responses                 
    
#End of day Data API wrapper
class EODData(BaseRequests):

    def __init__(self):
        self.api_key = secret.key_chain['EOD']
        self.main_url = 'https://eodhd.com/api'
        self.main_params = {
                    'api_token': self.api_key, 
                    'fmt': 'json'
                            }
    
        BaseRequests.__init__(self)

    #Pair of functions to request data from the EOD endpoint
    #Function that builds dictionary of parameters for the api requests to the EOD endpoint
    def historical_params(self, symbols:list, **kwargs):

        main_url = self.main_url
        endpoint = '/eod'
        
        dic = {}
        for symbol in symbols:
            payload = self.build_params(self.main_params, **kwargs)
            url = main_url + endpoint + '/' + symbol
            dic[symbol] = (url, payload)

        return dic
    
    #Function to make calls to the EOD endpoint
    def historical(self, symbols:list, asyn=True ,**kwargs):
        params = self.historical_params(symbols, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses

    #Functions to retrieve quotes for stocks, indices and ETFs
    def intraday_params(self, symbols:list, **kwargs):
        
        main_url = self.main_url
        endpoint = '/real-time/' + symbols[0]
        symbols = ','.join(symbols[1:])
        dic = {}
        payload = self.build_params(self.main_params, s=symbols, **kwargs)
        url = main_url + endpoint
        dic = {'intraday': (url, payload)}
        return dic
    
    def intraday(self, symbols:list, asyn=True, **kwargs):
        params = self.intraday_params(symbols, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses['intraday']

    #Function to retrieve tickers from a specified exchange
    def tickers_params(self, exchange:str='US', **kwargs):

        main_url = self.main_url
        endpoint = '/exchange-symbol-list'

        payload = self.build_params(self.main_params, **kwargs)
        url = main_url+endpoint+'/'+exchange
        
        dic = {
            exchange: (url, payload)
        }
        return dic

    def tickers(self, exchange:str='US', asyn=True, **kwargs):
        params = self.tickers_params(exchange, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses


    #Functions to retrieve general and fundamental information from equities
    #Function to build the filters applied to the API request
    def fundamental_browser(self, route:str ,filters:list=[]):
        if len(filters)>=1:
            filters = '::'+f',{route}::'.join(filters)
        else:
            filters = ''

        route = route + filters
        return route      

    def index_comps_params(self, inds:list, filters:list=[], **kwargs):
        main_url = self.main_url
        endpoint = '/fundamentals'
        route = 'Components'
        filters = self.fundamental_browser(route, filters)

        dic = {}

        for ind in inds:
            payload = self.build_params(self.main_params, filter=filters, **kwargs)
            url = main_url + endpoint + '/' + ind
            dic[ind] = (url, payload)

        return dic
    
    def index_comps(self, inds:list, filters:list=[], asyn=True, **kwargs):
        params = self.index_comps_params(inds, filters=filters, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses


    def general_equity_params(self, symbols:list, filters:list=[], **kwargs):

        main_url = self.main_url
        endpoint = '/fundamentals'
        route = 'General'
        filters = self.fundamental_browser(route, filters)

        dic = {}

        for symbol in symbols:
            payload = self.build_params(self.main_params, filter=filters, **kwargs)
            url = main_url + endpoint + '/' + symbol
            dic[symbol] = (url, payload)
        
        return dic
    
    def general_equity(self, symbols:list, filters:list=[], asyn=True, **kwargs):
        params = self.general_equity_params(symbols, filters, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses
    
    def earnings_params(self, symbols:list, filters:list=[], **kwargs):

        main_url = self.main_url
        endpoint = '/fundamentals'
        route = 'Earnings'
        filters = self.fundamental_browser(route, filters=filters)

        dic={}
        for symbol in symbols:
            payload = self.build_params(self.main_params, filter=filters, **kwargs)
            url = main_url + endpoint + '/' + symbol
            dic[symbol] = (url, payload)

        return dic
    
    def earnings(self, symbols:list, **kwargs):
        params = self.earnings_params(symbols, **kwargs)
        responses = self.select_request(params, asyn=True)
        return responses

    def financial_params(self, symbols:list,filters:list=[], **kwargs):
        main_url = self.main_url
        endpoint = '/fundamentals'
        route = 'Financials'
        filters = self.fundamental_browser(route, filters=filters)

        dic = {}
        for symbol in symbols:
            payload = self.build_params(self.main_params, filter=filters)
            url = main_url + endpoint +'/' +symbol
            dic[symbol] = (url, payload)

        return dic
    
    def financial(self, symbols:list, **kwargs):
        params = self.financial_params(symbols, **kwargs)
        responses = self.select_request(params, asyn=True)
        return responses

    def bulk_fundamental_params(self, symbols:list=[], exchange='US', **kwargs):
        main_url = self.main_url
        endpoint = '/bulk-fundamentals'

        if len(symbols) >0:
            symbols_str = ','.join(symbols)
            params = self.build_params(self.main_params, symbols=symbols_str,**kwargs)
        else:
            params = self.build_params(self.main_params, **kwargs)

        url =  main_url + endpoint + '/' + exchange
        dic = {'bulk': (url, params)}
        return dic
    
    def bulk_fundamental(self, symbols:list=[], **kwargs):
        params = self.bulk_fundamental_params(symbols, **kwargs)
        responses = self.select_request(params, asyn=True)
        return responses


        
    
#Fred Data API wrapper
class FREDData(BaseRequests):

    def __init__(self):
        self.api_key = secret.key_chain['FRED']
        self.main_url = 'https://api.stlouisfed.org/fred'
        self.main_params = {
                    'api_key': self.api_key, 
                    'file_type': 'json'
                            }
    
        BaseRequests.__init__(self)

    #Pair of functions to request data from the FRED endpoint
    #Function that builds dictionary of parameters for the api requests to the EOD endpoint

    #Get all releases of economic data.
    def releases_params(self, **kwargs):

        main_url = self.main_url
        endpoint = '/releases'

        dic = {}        
        payload = self.build_params(self.main_params, **kwargs)
        url = main_url + endpoint 
        dic['releases'] = (url, payload)

        return dic
    
    #Function to make calls to the EOD endpoint
    def releases(self, asyn=True ,**kwargs):
        params = self.releases_params(**kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses
    
    #Get all releases of economic data.
    def series_params(self, release_ids:list, **kwargs):

        main_url = self.main_url
        endpoint = '/release/series'

        dic = {}
        for rid in release_ids:        
            payload = self.build_params(self.main_params, release_id=rid, **kwargs)
            url = main_url + endpoint 
            dic[rid] = (url, payload)

        return dic
    
    #Function to make calls to the EOD endpoint
    def series(self, release_ids:list, asyn=True ,**kwargs):
        params = self.series_params(release_ids, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses
    
    #Get all releases of economic data.
    def observ_params(self, series_ids:list, **kwargs):

        main_url = self.main_url
        endpoint = '/series/observations'

        dic = {}
        for rid in series_ids:        
            payload = self.build_params(self.main_params, series_id=rid, **kwargs)
            url = main_url + endpoint 
            dic[rid] = (url, payload)

        return dic
    
    #Function to make calls to the EOD endpoint
    def observ(self, series_ids:list, asyn=True ,**kwargs):
        params = self.observ_params(series_ids, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses
    
    def series_meta_params(self, series_ids:list, **kwargs):

        main_url = self.main_url
        endpoint = '/series'

        dic = {}
        for sid in series_ids:        
            payload = self.build_params(self.main_params, series_id=sid, **kwargs)
            url = main_url + endpoint 
            dic[sid] = (url, payload)

        return dic
    
    def series_meta(self, series_ids:list, asyn=True ,**kwargs):
        params = self.series_meta_params(series_ids, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses
    
    def release_series_params(self, series_ids:list, **kwargs):

        main_url = self.main_url
        endpoint = '/series/release'

        dic = {}
        for sid in series_ids:        
            payload = self.build_params(self.main_params, series_id=sid, **kwargs)
            url = main_url + endpoint 
            dic[sid] = (url, payload)

        return dic

    def release_series(self, series_ids:list, asyn=True ,**kwargs):
        params = self.release_series_params(series_ids, **kwargs)
        responses = self.select_request(params, asyn=asyn)
        return responses