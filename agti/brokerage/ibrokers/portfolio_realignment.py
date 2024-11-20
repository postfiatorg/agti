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

    DEFAULT_STRATEGY = "nst"  # Default 3-letter strategy code
    DEFAULT_METATAG = "closing"  # Default metatag
    ORDER_TIMEOUT = 10  # seconds to wait for order acknowledgment

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
        
        # Verify connection
        if not self.ib_connection.isConnected():
            raise ConnectionError("Failed to establish IBKR connection")

    def disconnect_ibkr(self):
        """
        Disconnects from the Interactive Brokers API.
        """
        if self.ib_connection is not None:
            self.ib_connection.disconnect()
            self.ib_connection = None
            self.async_manager = None

    def verify_account_status(self):
        """
        Verifies that the account is ready for trading.
        
        :return: bool indicating if account is ready for trading
        """
        try:
            account_values = self.ib_connection.accountValues()
            account_summary = self.ib_connection.accountSummary()
            
            # Check if account data is available
            if not account_values or not account_summary:
                return False
                
            # Add any specific checks needed for your trading environment
            return True
        except Exception as e:
            print(f"Error verifying account status: {str(e)}")
            return False

    def get_target_portfolio(self):
        """
        Retrieves the target equity positions from the 'ibkr_target' Google Sheet.

        :return: DataFrame containing the target equity positions for the specified session.
        """
        target_df = self.google_sheet_manager.load_google_sheet_as_df(
            workbook=self.pw_map['production_trading_gsheet_workbook_name'],
            worksheet='ibkr_target'
        )

        if target_df.empty:
            raise ValueError("Target portfolio sheet is empty")

        # Filter only equity positions for the specified session
        equity_df = target_df[
            (target_df['contract_type'] == 'STK') &
            (target_df['session'] == self.session)
        ].copy()

        if equity_df.empty:
            raise ValueError(f"No equity positions found for session {self.session}")

        # Ensure necessary columns are of correct type
        equity_df['position'] = equity_df['position'].astype(int)
        equity_df['localSymbol'] = equity_df['localSymbol'].astype(str)
        
        # Handle missing or blank strategy values
        equity_df['strategy'] = equity_df['strategy'].fillna(self.DEFAULT_STRATEGY)
        equity_df['strategy'] = equity_df['strategy'].replace('', self.DEFAULT_STRATEGY)
        equity_df['strategy'] = equity_df['strategy'].astype(str)
        
        # Ensure strategy is exactly 3 letters
        def format_strategy(strat):
            if len(strat) > 3:
                return strat[:3].lower()
            elif len(strat) < 3:
                return (strat + self.DEFAULT_STRATEGY)[:3].lower()
            return strat.lower()
            
        equity_df['strategy'] = equity_df['strategy'].apply(format_strategy)
        
        # Handle metatag
        equity_df['metatag'] = equity_df['metatag'].fillna(self.DEFAULT_METATAG)
        equity_df['metatag'] = equity_df['metatag'].replace('', self.DEFAULT_METATAG)
        equity_df['metatag'] = equity_df['metatag'].astype(str)

        return equity_df

    def get_current_positions(self):
        """
        Retrieves the current equity positions from the IBKR account.

        :return: DataFrame containing the current equity positions.
        """
        portfolio_items = self.ib_connection.portfolio()
        
        if not portfolio_items:
            print("Warning: No portfolio items returned from IBKR")
            return pd.DataFrame(columns=['conId', 'localSymbol', 'position'])
            
        positions = []
        for item in portfolio_items:
            if item.contract.secType == 'STK':
                positions.append({
                    'conId': item.contract.conId,
                    'localSymbol': item.contract.localSymbol,
                    'position': item.position
                })
        current_positions_df = pd.DataFrame(positions)
        if not current_positions_df.empty:
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
        if target_df.empty:
            raise ValueError("Target positions DataFrame is empty")
            
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
        
        # Verify calculations
        if not (merged_df['delta'] == (merged_df['position_target'] - merged_df['position_current'])).all():
            raise ValueError("Delta calculation verification failed")
            
        return merged_df

    def cancel_conflicting_orders(self):
        """
        Cancels any open orders that might conflict with the new target positions.
        Only cancels orders for the specified session.
        """
        open_orders = self.ib_connection.reqAllOpenOrders()
        cancelled_orders = []
        failed_cancels = []
        
        for trade in open_orders:
            order = trade.order
            contract = trade.contract
            # Check if the order is for the specified session and is an equity
            if contract.secType == 'STK':
                # Parse the session from the orderRef
                order_ref_session = order.orderRef.split('.')[0] if order.orderRef else ''
                if self.session == order_ref_session:
                    try:
                        self.ib_connection.cancelOrder(order)
                        cancelled_orders.append(contract.localSymbol)
                        print(f"Cancelled order for {contract.localSymbol}, Order ID: {order.orderId}")
                    except Exception as e:
                        failed_cancels.append((contract.localSymbol, str(e)))
                        print(f"Failed to cancel order for {contract.localSymbol}: {str(e)}")
        
        if failed_cancels:
            raise RuntimeError(f"Failed to cancel orders for: {failed_cancels}")
            
        return cancelled_orders

    async def qualify_contracts_async(self, local_symbols):
        """
        Asynchronously qualify contracts for the list of local symbols.

        :param local_symbols: List of ticker symbols.
        :return: DataFrame with qualified contracts.
        """
        if not local_symbols:
            raise ValueError("No symbols provided for contract qualification")
            
        contracts = [Stock(symbol=symbol, exchange='SMART', currency='USD') for symbol in local_symbols]
        qualified_contracts = await self.async_manager.qualify_contracts(contracts)
        
        if not qualified_contracts:
            raise ValueError("No contracts could be qualified")
            
        qualified_df = pd.DataFrame([{
            'localSymbol': qc.symbol,
            'qualified_contract': qc,
            'conId': qc.conId
        } for qc in qualified_contracts])
        
        # Verify all symbols were qualified
        unqualified_symbols = set(local_symbols) - set(qualified_df['localSymbol'])
        if unqualified_symbols:
            raise ValueError(f"Failed to qualify contracts for symbols: {unqualified_symbols}")
            
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

        # Merge with target_df to get strategy and metatag
        orders_df = orders_df.merge(
            target_df[['localSymbol', 'strategy', 'metatag']],
            on='localSymbol',
            how='left'
        )

        # Only use defaults if values are actually missing
        orders_df['strategy'] = orders_df['strategy'].apply(
            lambda x: self.DEFAULT_STRATEGY if pd.isna(x) or str(x).strip() == '' else str(x).strip()
        )
        orders_df['metatag'] = orders_df['metatag'].apply(
            lambda x: self.DEFAULT_METATAG if pd.isna(x) or str(x).strip() == '' else str(x).strip()
        )

        # Format strategy to exactly 3 letters only if it's the default strategy
        def format_strategy(strat):
            if strat == self.DEFAULT_STRATEGY:
                return strat
            if len(strat) > 3:
                return strat[:3].lower()
            elif len(strat) < 3:
                return (strat + self.DEFAULT_STRATEGY)[:3].lower()
            return strat.lower()
        
        orders_df['strategy'] = orders_df['strategy'].apply(format_strategy)

        # Determine action and quantity
        orders_df['action'] = orders_df['delta'].apply(lambda x: 'BUY' if x > 0 else 'SELL')
        orders_df['quantity'] = orders_df['delta'].abs()

        # Generate order reference using actual values from sheet
        orders_df['order_ref'] = orders_df.apply(
            lambda row: f"{self.session}.{row['strategy']}.{row['metatag']}."
                       f"{row['localSymbol']}.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
            axis=1
        )

        # Qualify contracts asynchronously
        loop = asyncio.get_event_loop()
        qualified_df = loop.run_until_complete(self.qualify_contracts_async(orders_df['localSymbol'].tolist()))

        # Verify all contracts were qualified
        if len(qualified_df) != len(orders_df):
            unqualified_symbols = set(orders_df['localSymbol']) - set(qualified_df['localSymbol'])
            raise ValueError(f"Failed to qualify contracts for symbols: {unqualified_symbols}")

        # Merge qualified contracts
        orders_df = orders_df.merge(qualified_df, on='localSymbol', how='inner')

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
            Places orders based on the provided orders DataFrame with enhanced error handling.
            """
            placed_orders = []
            try:
                for _, row in orders_df.iterrows():
                    # Create the order
                    order = self.construct_order(
                        action=row['action'],
                        quantity=row['quantity'],
                        order_type=row['order_type'],
                        order_ref=row['order_ref']
                    )
    
                    # Place the order
                    trade = self.ib_connection.placeOrder(row['qualified_contract'], order)
                    placed_orders.append((trade, row['localSymbol']))
                    
                    # Wait for order to be acknowledged with extended timeout
                    start_time = datetime.datetime.now()
                    max_wait = 30  # Extended timeout to 30 seconds
    
                    while True:
                        self.ib_connection.sleep(0.1)  # Small sleep to prevent CPU spin
                        
                        current_status = trade.orderStatus.status
                        if current_status in ['Submitted', 'Filled', 'Cancelled']:
                            print(f"Order for {row['localSymbol']} reached status: {current_status}")
                            break
                            
                        if current_status == 'Inactive':
                            raise ValueError(f"Order for {row['localSymbol']} became inactive")
                            
                        elapsed = (datetime.datetime.now() - start_time).seconds
                        if elapsed > max_wait:
                            raise TimeoutError(f"Order timeout for {row['localSymbol']} after {max_wait}s in status: {current_status}")
    
                    print(f"Placed {row['order_type'].upper()} {row['action']} order for {row['quantity']} shares of {row['localSymbol']}")
                    
                    # Add delay between orders
                    self.ib_connection.sleep(1.0)  # Increased delay between orders
    
            except Exception as e:
                print(f"Error during order placement: {str(e)}")
                print("Attempting to cancel all placed orders...")
                
                cancellation_errors = []
                for trade, symbol in placed_orders:
                    try:
                        if trade.orderStatus.status not in ['Cancelled', 'Filled']:
                            self.ib_connection.cancelOrder(trade.order)
                            
                            # Wait for cancellation to be acknowledged
                            cancel_start = datetime.datetime.now()
                            while True:
                                self.ib_connection.sleep(0.1)
                                if trade.orderStatus.status in ['Cancelled']:
                                    print(f"Successfully cancelled order for {symbol}")
                                    break
                                if (datetime.datetime.now() - cancel_start).seconds > 10:
                                    raise TimeoutError(f"Cancellation timeout for {symbol}")
                                    
                    except Exception as cancel_error:
                        error_msg = f"Error cancelling order for {symbol}: {str(cancel_error)}"
                        print(error_msg)
                        cancellation_errors.append(error_msg)
                
                # Raise final error with details
                error_msg = f"Order placement failed: {str(e)}. "
                if not cancellation_errors:
                    error_msg += "All placed orders have been cancelled."
                else:
                    error_msg += f"Some cancellations failed: {'; '.join(cancellation_errors)}"
                
                raise RuntimeError(error_msg) from e

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
            # Step 2: Verify connection
            if not self.ib_connection.isConnected():
                raise ConnectionError("Failed to establish IBKR connection")

            # Step 3: Get target and current positions
            target_df = self.get_target_portfolio()
            if target_df.empty:
                raise ValueError("No target positions found in Google Sheet")

            current_df = self.get_current_positions()
            if current_df.empty:
                print("Warning: No current positions found in account")

            # Step 4: Calculate deltas
            deltas_df = self.calculate_position_deltas(target_df, current_df)

            # Step 5: Generate order preview
            orders_df = self.generate_order_preview(deltas_df, target_df, order_type)

            if orders_df.empty:
                print("No orders to execute.")
                return orders_df

            if preview_only:
                print("Order preview generated. No orders executed.")
                return orders_df

            # Step 6: Cancel conflicting orders
            self.cancel_conflicting_orders()

            # Step 7: Verify account is ready for trading
            account_ready = self.verify_account_status()
            if not account_ready:
                raise RuntimeError("Account not ready for trading")

            # Step 8: Place orders
            self.place_orders_from_preview(orders_df)

            return orders_df

        except Exception as e:
            print(f"Error during realignment: {str(e)}")
            raise

        finally:
            # Step 9: Always disconnect from IBKR
            self.disconnect_ibkr()

    def get_order_preview(self, order_type='market'):
        """
        Generates and returns an order preview without executing.

        :param order_type: Order type to use for all orders ('market', 'market_on_open', 'market_on_close').
        :return: DataFrame containing the order preview.
        """
        return self.execute_realignment(order_type=order_type, preview_only=True)