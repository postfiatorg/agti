# Filename: agti/live_trading/ibkr_equity_realignment.py

import asyncio
import pandas as pd
from ib_insync import *
from agti.utilities.google_sheet_manager import GoogleSheetManager
from agti.brokerage.ibrokers.connection_spawn import IBConnectionSpawn
import datetime
import nest_asyncio
nest_asyncio.apply()  # Apply nest_asyncio to allow nested event loops

class AsyncStockDataManager:
    """
    Asynchronous manager for qualifying contracts and fetching market data.
    """
    def __init__(self, ib):
        self.ib = ib

    async def qualify_contracts(self, contracts):
        """
        Asynchronously qualify a list of contracts.
        """
        tasks = [self.ib.qualifyContractsAsync(contract) for contract in contracts]
        qualified_contracts = await asyncio.gather(*tasks)
        # Flatten the list
        qualified_contracts = [qc[0] for qc in qualified_contracts if qc]
        return qualified_contracts

class IBEquityRealignment:
    """
    This class realigns equity positions in an IBKR account to match the target positions
    specified in the 'ibkr_target' Google Sheet. It allows generating a preview of orders
    before execution and uses bulk asynchronous contract qualification.
    """

    def __init__(self, pw_map, client_id=5, session='dt'):
        """
        Initializes the IBEquityRealignment instance.

        :param pw_map: Dictionary containing passwords and configurations.
        :param client_id: Client ID for the IBKR connection.
        :param session: Session identifier ('dt' or 'nt').
        """
        self.pw_map = pw_map
        self.client_id = client_id
        self.session = session
        self.google_sheet_manager = GoogleSheetManager(prod_trading=True)
        self.ib_connection = None
        self.async_manager = None

    def connect_ibkr(self):
        """
        Connects to the Interactive Brokers API using the IBConnectionSpawn class.
        """
        ib_conn_spawn = IBConnectionSpawn(clientId=self.client_id)
        ib_conn_spawn.connect()
        self.ib_connection = ib_conn_spawn.ib_connection
        self.async_manager = AsyncStockDataManager(self.ib_connection)

    def disconnect_ibkr(self):
        """
        Disconnects from the Interactive Brokers API.
        """
        if self.ib_connection is not None:
            self.ib_connection.disconnect()
            self.ib_connection = None
            self.async_manager = None

    def get_target_portfolio(self):
        """
        Retrieves the target equity positions from the 'ibkr_target' Google Sheet.

        :return: DataFrame containing the target equity positions for the specified session.
        """
        target_df = self.google_sheet_manager.load_google_sheet_as_df(
            workbook=self.pw_map['production_trading_gsheet_workbook_name'],
            worksheet='ibkr_target'
        )

        # Filter only equity positions for the specified session
        equity_df = target_df[
            (target_df['contract_type'] == 'STK') &
            (target_df['session'] == self.session)
        ].copy()

        # Ensure necessary columns are of correct type
        equity_df['position'] = equity_df['position'].astype(int)
        equity_df['localSymbol'] = equity_df['localSymbol'].astype(str)
        equity_df['strategy'] = equity_df.get('strategy', 'default').astype(str)
        equity_df['metatag'] = equity_df.get('metatag', 'realignment').astype(str)
        return equity_df

    def get_current_positions(self):
        """
        Retrieves the current equity positions from the IBKR account.

        :return: DataFrame containing the current equity positions.
        """
        portfolio_items = self.ib_connection.portfolio()
        positions = []
        for item in portfolio_items:
            if item.contract.secType == 'STK':
                positions.append({
                    'conId': item.contract.conId,
                    'localSymbol': item.contract.localSymbol,
                    'position': item.position
                })
        current_positions_df = pd.DataFrame(positions)
        current_positions_df['position'] = current_positions_df['position'].astype(int)
        current_positions_df['localSymbol'] = current_positions_df['localSymbol'].astype(str)
        return current_positions_df

    def calculate_position_deltas(self, target_df, current_df):
        """
        Calculates the difference between target and current positions.

        :param target_df: DataFrame of target positions.
        :param current_df: DataFrame of current positions.
        :return: DataFrame containing position deltas.
        """
        # Merge on localSymbol
        merged_df = pd.merge(
            target_df[['localSymbol', 'position']],
            current_df[['localSymbol', 'position']],
            on='localSymbol',
            how='outer',
            suffixes=('_target', '_current')
        ).fillna(0)

        # Calculate delta
        merged_df['delta'] = merged_df['position_target'] - merged_df['position_current']
        merged_df['delta'] = merged_df['delta'].astype(int)
        return merged_df

    def cancel_conflicting_orders(self):
        """
        Cancels any open orders that might conflict with the new target positions.
        Only cancels orders for the specified session.
        """
        open_orders = self.ib_connection.reqAllOpenOrders()
        for trade in open_orders:
            order = trade.order
            contract = trade.contract
            # Check if the order is for the specified session and is an equity
            if contract.secType == 'STK':
                # Parse the session from the orderRef
                order_ref_session = order.orderRef.split('.')[0] if order.orderRef else ''
                if self.session == order_ref_session:
                    print(f"Cancelling order for {contract.localSymbol}, Order ID: {order.orderId}")
                    self.ib_connection.cancelOrder(order)

    async def qualify_contracts_async(self, local_symbols):
        """
        Asynchronously qualify contracts for the list of local symbols.

        :param local_symbols: List of ticker symbols.
        :return: DataFrame with qualified contracts.
        """
        contracts = [Stock(symbol=symbol, exchange='SMART', currency='USD') for symbol in local_symbols]
        qualified_contracts = await self.async_manager.qualify_contracts(contracts)
        qualified_df = pd.DataFrame([{
            'localSymbol': qc.symbol,
            'qualified_contract': qc,
            'conId': qc.conId
        } for qc in qualified_contracts])
        return qualified_df

    def generate_order_preview(self, deltas_df, target_df, order_type):
        """
        Generates a preview DataFrame of orders to be placed.

        :param deltas_df: DataFrame containing position deltas.
        :param target_df: DataFrame of target positions.
        :param order_type: Order type to use for all orders.
        :return: DataFrame containing order preview.
        """
        # Filter deltas with non-zero delta
        orders_df = deltas_df[deltas_df['delta'] != 0].copy()
        if orders_df.empty:
            print("No orders to place. Portfolio is already aligned.")
            return pd.DataFrame()

        # Get additional info from target_df
        orders_df = orders_df.merge(
            target_df[['localSymbol', 'strategy', 'metatag']],
            on='localSymbol',
            how='left'
        )

        # Determine action and quantity
        orders_df['action'] = orders_df['delta'].apply(lambda x: 'BUY' if x > 0 else 'SELL')
        orders_df['quantity'] = orders_df['delta'].abs()

        # Generate order reference
        orders_df['order_ref'] = orders_df.apply(
            lambda row: f"{self.session}.{row['strategy']}.{row['metatag']}.{row['localSymbol']}.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
            axis=1
        )

        # Qualify contracts asynchronously
        loop = asyncio.get_event_loop()
        qualified_df = loop.run_until_complete(self.qualify_contracts_async(orders_df['localSymbol'].tolist()))

        # Merge qualified contracts
        orders_df = orders_df.merge(qualified_df, on='localSymbol', how='left')

        # Add order type
        orders_df['order_type'] = order_type

        # Rearrange columns
        orders_df = orders_df[[
            'localSymbol', 'conId', 'qualified_contract', 'action', 'quantity',
            'order_type', 'order_ref', 'strategy', 'metatag'
        ]]

        return orders_df

    def place_orders_from_preview(self, orders_df):
        """
        Places orders based on the provided orders DataFrame.

        :param orders_df: DataFrame containing orders to be placed.
        """
        for _, row in orders_df.iterrows():
            # Create the order
            order = self.construct_order(
                action=row['action'],
                quantity=row['quantity'],
                order_type=row['order_type'],
                order_ref=row['order_ref']
            )

            # Place the order
            self.ib_connection.placeOrder(row['qualified_contract'], order)
            print(f"Placed {row['order_type'].upper()} {row['action']} order for {row['quantity']} shares of {row['localSymbol']}")

    def construct_order(self, action, quantity, order_type, order_ref):
        """
        Constructs an IBKR Order object based on the specified order type.

        :param action: 'BUY' or 'SELL'.
        :param quantity: Number of shares.
        :param order_type: 'market', 'market_on_open', 'market_on_close'.
        :param order_ref: Reference string for the order.
        :return: IBKR Order object.
        """
        order_type = order_type.lower()
        if order_type == 'market':
            order = Order(
                action=action,
                orderType='MKT',
                totalQuantity=quantity,
                tif='DAY',
                orderRef=order_ref
            )
        elif order_type == 'market_on_open':
            order = Order(
                action=action,
                orderType='MKT',
                totalQuantity=quantity,
                tif='OPG',  # Market on Open
                orderRef=order_ref
            )
        elif order_type == 'market_on_close':
            order = Order(
                action=action,
                orderType='MOC',
                totalQuantity=quantity,
                tif='DAY',
                orderRef=order_ref
            )
        else:
            raise ValueError(f"Unsupported order type: {order_type}")
        return order

    def execute_realignment(self, order_type='market', preview_only=False):
        """
        Executes the portfolio realignment using the specified order type.
        Optionally generates a preview of orders without executing.

        :param order_type: Order type to use for all orders ('market', 'market_on_open', 'market_on_close').
        :param preview_only: If True, only generates a preview without executing orders.
        :return: DataFrame containing the order preview.
        """
        # Step 1: Connect to IBKR
        self.connect_ibkr()

        try:
            # Step 2: Get target and current positions
            target_df = self.get_target_portfolio()
            current_df = self.get_current_positions()

            # Step 3: Calculate deltas
            deltas_df = self.calculate_position_deltas(target_df, current_df)

            # Step 4: Generate order preview
            orders_df = self.generate_order_preview(deltas_df, target_df, order_type)

            if orders_df.empty:
                print("No orders to execute.")
                return orders_df

            if preview_only:
                print("Order preview generated. No orders executed.")
                return orders_df

            # Step 5: Cancel conflicting orders
            self.cancel_conflicting_orders()

            # Step 6: Place orders
            self.place_orders_from_preview(orders_df)

            return orders_df

        finally:
            # Step 7: Disconnect from IBKR
            self.disconnect_ibkr()

    def get_order_preview(self, order_type='market'):
        """
        Generates and returns an order preview without executing.

        :param order_type: Order type to use for all orders ('market', 'market_on_open', 'market_on_close').
        :return: DataFrame containing the order preview.
        """
        return self.execute_realignment(order_type=order_type, preview_only=True)

# Usage examp
