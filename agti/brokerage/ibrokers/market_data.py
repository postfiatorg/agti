import asyncio
from ib_insync import *
import nest_asyncio
import pandas as pd
import datetime
# Apply the nest_asyncio patch
nest_asyncio.apply()

class AsyncStockDataManager:
    def __init__(self, ib):
        self.ib = ib

    async def fetch_ticker_data(self, contract):
        # Request frozen market data (snapshot)
        ticker = self.ib.reqMktData(contract=contract, genericTickList='', snapshot=True, regulatorySnapshot=False, mktDataOptions=[])
        # Wait until data is available
        while ticker.time is None:
            await asyncio.sleep(0.1)
        return ticker

    async def qualify_contracts(self, contracts):
        # Qualify contracts to resolve ambiguities
        qualified_contracts = []
        for contract in contracts:
            qualified_contract = self.ib.qualifyContracts(contract)
            if qualified_contract:
                qualified_contracts.append(qualified_contract[0])
            else:
                print(f"Could not qualify contract: {contract}")
        return qualified_contracts

    async def get_list_of_tickers__ticker_based_query(self, tickers=['AAPL', 'GOOG', 'MSFT', 'AMZN', 'META']):
        # Define the list of contracts
        contracts = [Stock(symbol=ticker, exchange='SMART', currency='USD') for ticker in tickers]

        # Qualify the contracts
        qualified_contracts = await self.qualify_contracts(contracts)

        # Fetch ticker data concurrently
        tasks = [self.fetch_ticker_data(contract) for contract in qualified_contracts]
        results = await asyncio.gather(*tasks)

        # Store the retrieved data
        ticker_data = []
        for result in results:
            ticker_data.append({
                'Contract ID': result.contract.conId,
                'Symbol': result.contract.symbol,
                'Time': result.time,
                'Bid': result.bid, 'Bid Size': result.bidSize, 'Bid Exchange': result.bidExchange,
                'Ask': result.ask, 'Ask Size': result.askSize, 'Ask Exchange': result.askExchange,
                'Last': result.last, 'Last Size': result.lastSize, 'Last Exchange': result.lastExchange,
                'Volume': result.volume, 'High': result.high, 'Low': result.low, 'Close': result.close,
                'Open': result.open  # Include the open price of the day
            })

        # Convert to DataFrame for easier viewing
        df = pd.DataFrame(ticker_data)
        return df

    async def get_contract_ids_for_us_equities(self, tickers=['AAPL', 'GOOG', 'MSFT', 'AMZN', 'META']):
        # Define the list of contracts
        contracts = [Stock(symbol=ticker, exchange='SMART', currency='USD') for ticker in tickers]

        # Qualify the contracts
        qualified_contracts = await self.qualify_contracts(contracts)

        # Extract contract IDs and prepare the data
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = {
            'ticker': [contract.symbol for contract in qualified_contracts],
            'contract_id': [contract.conId for contract in qualified_contracts],
            'date_run': [now] * len(qualified_contracts),
            'exchange': 'SMART',
            'currency':'USD'
        }

        # Convert to DataFrame
        df = pd.DataFrame(data)
        return df
        
    async def get_price_info__contract_id_based_query(self, contract_ids):
        """" 
        EXAMPLE:
        contract_ids = asyncio.run(async_stock_data_manager.get_contract_ids_for_us_equities())
        asyncio.run(async_stock_data_manager.get_price_info__contract_id_based_query(list(contract_ids['contract_id'])))"""
         # Define the list of contracts based on contract IDs
        contracts = [Contract(conId=contract_id, exchange='SMART', currency='USD') for contract_id in contract_ids]
        
        # Qualify the contracts
        qualified_contracts = await self.qualify_contracts(contracts)
        
        # Fetch ticker data concurrently
        tasks = [self.fetch_ticker_data(contract) for contract in qualified_contracts]
        results = await asyncio.gather(*tasks)
        
        # Store the retrieved data
        ticker_data = []
        for result in results:
            ticker_data.append({
                'Contract ID': result.contract.conId,
                'Symbol': result.contract.symbol,
                'Time': result.time,
                'Bid': result.bid, 'Bid Size': result.bidSize, 'Bid Exchange': result.bidExchange,
                'Ask': result.ask, 'Ask Size': result.askSize, 'Ask Exchange': result.askExchange,
                'Last': result.last, 'Last Size': result.lastSize, 'Last Exchange': result.lastExchange,
                'Volume': result.volume, 'High': result.high, 'Low': result.low, 'Close': result.close,
                'Open': result.open  # Include the open price of the day
            })
        
        # Convert to DataFrame for easier viewing
        df = pd.DataFrame(ticker_data)
        return df

