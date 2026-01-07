"""
Signal Execution Module
Executes trading signals via webhooks with 3-step workflow:
1. Fetch signals from database
2. Prepare webhook JSON with client quantities
3. Execute and update strategy tables
"""
import sys
import os
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Add Databases path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'Databases'))

from Databases.app_data_db_connection import get_session
from Databases.strategy_models import ETFSignal, StockSignal, ETFSavedStrategy, StockSavedStrategy


class SignalExecutor:
    """
    Executes trading signals via webhooks
    """
    
    def __init__(self):
        """Initialize the signal executor"""
        self.setup_logging()
        self.init_database()
        
        # NSE holidays for 2025 (update annually)
        self.nse_holidays = [
            '2025-01-26',  # Republic Day
            '2025-03-14',  # Holi
            '2025-03-31',  # Id-Ul-Fitr
            '2025-04-10',  # Mahavir Jayanti
            '2025-04-14',  # Dr. Ambedkar Jayanti
            '2025-04-18',  # Good Friday
            '2025-05-01',  # Maharashtra Day
            '2025-06-07',  # Bakri Id
            '2025-08-15',  # Independence Day
            '2025-08-27',  # Ganesh Chaturthi
            '2025-10-02',  # Gandhi Jayanti
            '2025-10-21',  # Dussehra
            '2025-11-01',  # Diwali
            '2025-11-05',  # Diwali (Balipratipada)
            '2025-11-24',  # Gurunanak Jayanti
            '2025-12-25',  # Christmas
        ]
    
    def setup_logging(self):
        """Setup logging"""
        log_dir = os.path.join(current_dir, 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(log_dir, 'signal_execution.log'), encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def init_database(self):
        """Initialize database connections"""
        try:
            from Databases.app_data_db_connection import create_connection
            from market_data_db_connection import create_connection as create_market_connection
            
            if not create_connection():
                raise RuntimeError("Failed to connect to application database")
            
            if not create_market_connection():
                raise RuntimeError("Failed to connect to market data database")
            
            self.logger.info("Database connections initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    # ========================================================================
    # HELPER FUNCTIONS
    # ========================================================================
    
    def get_next_monday(self, from_date: datetime = None) -> str:
        """
        Get next Monday date, skipping NSE holidays
        
        Args:
            from_date: Starting date (default: today)
        
        Returns:
            Next Monday date as string (YYYY-MM-DD)
        """
        if from_date is None:
            from_date = datetime.now()
        
        # Find next Monday
        days_until_monday = (7 - from_date.weekday()) % 7
        if days_until_monday == 0:  # If today is Monday
            days_until_monday = 7
        
        next_monday = from_date + timedelta(days=days_until_monday)
        
        # Skip holidays
        while next_monday.strftime('%Y-%m-%d') in self.nse_holidays:
            next_monday += timedelta(days=1)
            # If it's not a weekday, move to next Monday
            if next_monday.weekday() >= 5:  # Saturday or Sunday
                days_until_monday = (7 - next_monday.weekday()) % 7
                next_monday += timedelta(days=days_until_monday)
        
        return next_monday.strftime('%Y-%m-%d')
    
    def get_current_ltp(self, symbol: str, exchange: str = 'NSE') -> Optional[float]:
        """
        Get current LTP (Last Traded Price) for a symbol
        Fetches the latest available price from database
        
        Args:
            symbol: Symbol name
            exchange: Exchange (NSE/BSE)
        
        Returns:
            Current LTP or None if not available
        """
        try:
            from market_data_db_connection import get_session as get_market_session
            from sqlalchemy import text
            
            session = get_market_session()
            try:
                # Get today's date
                today = datetime.now().strftime('%Y-%m-%d')
                
                # Try to get today's price first (if market has opened)
                # Then fall back to latest available price
                
                # Try ETF data first
                query = text("""
                    SELECT close, date FROM etf_data 
                    WHERE symbol = :symbol 
                    ORDER BY date DESC 
                    LIMIT 1
                """)
                result = session.execute(query, {"symbol": symbol})
                row = result.fetchone()
                
                if row:
                    ltp = float(row[0])
                    price_date = row[1]
                    self.logger.info(f"LTP for {symbol}: ₹{ltp:.2f} (date: {price_date})")
                    return ltp
                
                # Try stock data
                query = text("""
                    SELECT close, date FROM stock_data 
                    WHERE symbol = :symbol 
                    ORDER BY date DESC 
                    LIMIT 1
                """)
                result = session.execute(query, {"symbol": symbol})
                row = result.fetchone()
                
                if row:
                    ltp = float(row[0])
                    price_date = row[1]
                    self.logger.info(f"LTP for {symbol}: ₹{ltp:.2f} (date: {price_date})")
                    return ltp
                
                self.logger.warning(f"No LTP found for {symbol}")
                return None
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error fetching LTP for {symbol}: {e}")
            return None
    
    # ========================================================================
    # STEP 1: FETCH SIGNALS
    # ========================================================================
    
    def fetch_etf_signals(self, user_id: str = None, strategy_name: str = None) -> List[Dict[str, Any]]:
        """
        Fetch PENDING signals from etf_signal table
        
        Args:
            user_id: Filter by user_id (optional)
            strategy_name: Filter by strategy_name (optional)
        
        Returns:
            List of signal dictionaries
        """
        try:
            session = get_session()
            try:
                query = session.query(ETFSignal).filter(ETFSignal.execution_status == 'pending')
                
                if user_id:
                    query = query.filter(ETFSignal.user_id == user_id)
                if strategy_name:
                    query = query.filter(ETFSignal.strategy_name == strategy_name)
                
                # Fetch pending signals
                signals = []
                for signal in query.all():
                    signals.append({
                        'id': signal.id,
                        'user_id': signal.user_id,
                        'user_code': signal.user_code,  # NEW: Fetch user_code
                        'strategy_name': signal.strategy_name,
                        'order_side': signal.order_side,
                        'symbol_name': signal.symbol_name,
                        'client_json': signal.client_json,
                        'webhook_url': signal.webhook_url,
                        'price': signal.price
                    })
                
                self.logger.info(f"Fetched {len(signals)} pending ETF signals")
                return signals
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error fetching ETF signals: {e}")
            return []
    
    def fetch_stock_signals(self, user_id: str = None, strategy_name: str = None) -> List[Dict[str, Any]]:
        """
        Fetch PENDING signals from stock_signal table
        
        Args:
            user_id: Filter by user_id (optional)
            strategy_name: Filter by strategy_name (optional)
        
        Returns:
            List of signal dictionaries
        """
        try:
            session = get_session()
            try:
                query = session.query(StockSignal).filter(StockSignal.execution_status == 'pending')
                
                if user_id:
                    query = query.filter(StockSignal.user_id == user_id)
                if strategy_name:
                    query = query.filter(StockSignal.strategy_name == strategy_name)
                
                # Fetch pending signals
                signals = []
                for signal in query.all():
                    signals.append({
                        'id': signal.id,
                        'user_id': signal.user_id,
                        'user_code': signal.user_code,  # NEW: Fetch user_code
                        'strategy_name': signal.strategy_name,
                        'order_side': signal.order_side,
                        'symbol_name': signal.symbol_name,
                        'client_json': signal.client_json,
                        'webhook_url': signal.webhook_url,
                        'price': signal.price
                    })
                
                self.logger.info(f"Fetched {len(signals)} pending Stock signals")
                return signals
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error fetching Stock signals: {e}")
            return []
    
    def mark_etf_signal_executed(self, signal_id: int, success: bool = True):
        """
        Mark an ETF signal as executed or failed
        
        Args:
            signal_id: Signal ID
            success: Whether execution was successful
        """
        try:
            session = get_session()
            try:
                signal = session.query(ETFSignal).filter(ETFSignal.id == signal_id).first()
                
                if signal:
                    signal.executed_at = datetime.now()
                    signal.execution_status = 'executed' if success else 'failed'
                    session.commit()
                    
                    self.logger.info(f"Marked ETF signal {signal_id} as {signal.execution_status}")
                else:
                    self.logger.warning(f"ETF signal {signal_id} not found")
                    
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error marking ETF signal as executed: {e}")
    
    def mark_stock_signal_executed(self, signal_id: int, success: bool = True):
        """
        Mark a Stock signal as executed or failed
        
        Args:
            signal_id: Signal ID
            success: Whether execution was successful
        """
        try:
            session = get_session()
            try:
                signal = session.query(StockSignal).filter(StockSignal.id == signal_id).first()
                
                if signal:
                    signal.executed_at = datetime.now()
                    signal.execution_status = 'executed' if success else 'failed'
                    session.commit()
                    
                    self.logger.info(f"Marked Stock signal {signal_id} as {signal.execution_status}")
                else:
                    self.logger.warning(f"Stock signal {signal_id} not found")
                    
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error marking Stock signal as executed: {e}")
    
    # ========================================================================
    # STEP 2: PREPARE WEBHOOK JSON
    # ========================================================================
    
    def prepare_webhook_json(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare webhook JSON for execution
        
        Args:
            signal: Signal dictionary
        
        Returns:
            Webhook JSON payload
        """
        try:
            # Get current LTP
            ltp = self.get_current_ltp(signal['symbol_name'])
            if not ltp:
                self.logger.error(f"Cannot prepare webhook: No LTP for {signal['symbol_name']}")
                return None
            
            # Parse client_json
            try:
                client_data = json.loads(signal['client_json']) if signal['client_json'] else {}
            except:
                self.logger.error(f"Invalid client_json: {signal['client_json']}")
                return None
            
            # Calculate quantities for each client
            clients = {}
            for client_id, client_cap in client_data.items():
                try:
                    # Handle formatted currency values (e.g., "₹5,000.00" or "5000")
                    if isinstance(client_cap, str):
                        # Remove currency symbols, commas, and spaces
                        cap_str = client_cap.replace('₹', '').replace(',', '').replace(' ', '').strip()
                        cap = float(cap_str)
                    else:
                        cap = float(client_cap)
                    
                    quantity = int(cap / ltp)
                    if quantity > 0:
                        clients[client_id] = str(quantity)  # Convert to string
                        self.logger.info(f"Client {client_id}: cap={cap:.2f}, LTP={ltp:.2f}, quantity={quantity}")
                except Exception as e:
                    self.logger.error(f"Error calculating quantity for client {client_id}: {e}")
                    continue
            
            if not clients:
                self.logger.warning(f"No valid clients for signal: {signal['symbol_name']}")
                return None
            
            # Build webhook JSON (NEW FORMAT)
            webhook_json = {
                "exchange": "NSE",
                "symbol": signal['symbol_name'],
                "user_id": signal['user_code'],  # NEW: Use user_code as user_id
                "order_side": signal['order_side'],
                "product_type": "delivery",
                "clients": clients
            }
            
            self.logger.info(f"Prepared webhook JSON for {signal['symbol_name']}: {len(clients)} clients")
            return webhook_json
            
        except Exception as e:
            self.logger.error(f"Error preparing webhook JSON: {e}")
            return None
    
    # ========================================================================
    # STEP 3: EXECUTE AND UPDATE
    # ========================================================================
    
    def execute_webhook(self, webhook_url: str, webhook_json: Dict[str, Any]) -> bool:
        """
        Execute webhook via POST request
        
        Args:
            webhook_url: Webhook URL
            webhook_json: Webhook payload
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Executing webhook: {webhook_url}")
            self.logger.info(f"Payload: {json.dumps(webhook_json, indent=2)}")
            
            response = requests.post(
                webhook_url,
                json=webhook_json,
                timeout=30,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                self.logger.info(f"Webhook execution successful: {response.text}")
                return True
            else:
                self.logger.error(f"Webhook execution failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error executing webhook: {e}")
            return False
    
    def update_etf_strategy(self, user_id: str, strategy_name: str):
        """
        Update ETF strategy after successful execution
        
        Updates:
        - accumulation_weeks = accumulation_weeks - 1
        """
        try:
            session = get_session()
            try:
                strategy = session.query(ETFSavedStrategy).filter(
                    ETFSavedStrategy.user_id == user_id,
                    ETFSavedStrategy.strategy_name == strategy_name
                ).first()
                
                if not strategy:
                    self.logger.error(f"Strategy not found: {user_id} - {strategy_name}")
                    return False
                
                # Update accumulation weeks
                strategy.accumulation_weeks = (strategy.accumulation_weeks or 0) - 1
                
                session.commit()
                
                self.logger.info(f"Updated ETF strategy: {strategy_name}")
                self.logger.info(f"  Accumulation weeks: {strategy.accumulation_weeks}")
                
                return True
                
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error updating ETF strategy: {e}")
            return False
    
    def update_stock_strategy(self, user_id: str, strategy_name: str):
        """
        Update Stock strategy after successful execution
        
        Updates:
        - accumulation_weeks = accumulation_weeks - 1
        """
        try:
            session = get_session()
            try:
                strategy = session.query(StockSavedStrategy).filter(
                    StockSavedStrategy.user_id == user_id,
                    StockSavedStrategy.strategy_name == strategy_name
                ).first()
                
                if not strategy:
                    self.logger.error(f"Strategy not found: {user_id} - {strategy_name}")
                    return False
                
                # Update accumulation weeks
                strategy.accumulation_weeks = (strategy.accumulation_weeks or 0) - 1
                
                session.commit()
                
                self.logger.info(f"Updated Stock strategy: {strategy_name}")
                self.logger.info(f"  Accumulation weeks: {strategy.accumulation_weeks}")
                
                return True
                
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error updating Stock strategy: {e}")
            return False
    
    # ========================================================================
    # MAIN EXECUTION WORKFLOW
    # ========================================================================
    
    def execute_etf_signals(self):
        """Execute all ETF signals"""
        print("\n" + "="*80)
        print("ETF SIGNAL EXECUTION")
        print("="*80 + "\n")
        
        # Step 1: Fetch signals
        print("STEP 1: Fetching ETF signals from database...")
        print("-" * 80)
        signals = self.fetch_etf_signals()
        
        if not signals:
            print("⚠️  No ETF signals to execute\n")
            return
        
        print(f"✅ Found {len(signals)} ETF signals\n")
        
        # Group signals by user and strategy
        signal_groups = {}
        for signal in signals:
            key = (signal['user_id'], signal['strategy_name'])
            if key not in signal_groups:
                signal_groups[key] = []
            signal_groups[key].append(signal)
        
        # Execute each group
        for (user_id, strategy_name), group_signals in signal_groups.items():
            print(f"\nProcessing: {strategy_name} ({user_id})")
            print(f"Signals: {len(group_signals)}")
            
            success_count = 0
            
            for signal in group_signals:
                print(f"\n  {signal['order_side']} {signal['symbol_name']}")
                
                # Step 2: Prepare webhook JSON
                print("  STEP 2: Preparing webhook JSON...")
                webhook_json = self.prepare_webhook_json(signal)
                
                if not webhook_json:
                    print("  ❌ Failed to prepare webhook JSON")
                    continue
                
                print(f"  ✅ Webhook JSON prepared ({len(webhook_json['clients'])} clients)")
                
                # Step 3: Execute webhook
                print("  STEP 3: Executing webhook...")
                success = self.execute_webhook(signal['webhook_url'], webhook_json)
                
                # Mark signal as executed or failed
                self.mark_etf_signal_executed(signal['id'], success)
                
                if success:
                    print("  ✅ Webhook executed successfully")
                    success_count += 1
                else:
                    print("  ❌ Webhook execution failed")
            
            # Update strategy if any signal was successful
            if success_count > 0:
                print(f"\n  Updating strategy...")
                self.update_etf_strategy(user_id, strategy_name)
                print(f"  ✅ Strategy updated")
            
            print("-" * 80)
        
        print("\n" + "="*80)
        print("ETF SIGNAL EXECUTION COMPLETE")
        print("="*80 + "\n")
    
    def execute_stock_signals(self):
        """Execute all Stock signals"""
        print("\n" + "="*80)
        print("STOCK SIGNAL EXECUTION")
        print("="*80 + "\n")
        
        # Step 1: Fetch signals
        print("STEP 1: Fetching Stock signals from database...")
        print("-" * 80)
        signals = self.fetch_stock_signals()
        
        if not signals:
            print("⚠️  No Stock signals to execute\n")
            return
        
        print(f"✅ Found {len(signals)} Stock signals\n")
        
        # Group signals by user and strategy
        signal_groups = {}
        for signal in signals:
            key = (signal['user_id'], signal['strategy_name'])
            if key not in signal_groups:
                signal_groups[key] = []
            signal_groups[key].append(signal)
        
        # Execute each group
        for (user_id, strategy_name), group_signals in signal_groups.items():
            print(f"\nProcessing: {strategy_name} ({user_id})")
            print(f"Signals: {len(group_signals)}")
            
            success_count = 0
            
            for signal in group_signals:
                print(f"\n  {signal['order_side']} {signal['symbol_name']}")
                
                # Step 2: Prepare webhook JSON
                print("  STEP 2: Preparing webhook JSON...")
                webhook_json = self.prepare_webhook_json(signal)
                
                if not webhook_json:
                    print("  ❌ Failed to prepare webhook JSON")
                    continue
                
                print(f"  ✅ Webhook JSON prepared ({len(webhook_json['clients'])} clients)")
                
                # Step 3: Execute webhook
                print("  STEP 3: Executing webhook...")
                success = self.execute_webhook(signal['webhook_url'], webhook_json)
                
                # Mark signal as executed or failed
                self.mark_stock_signal_executed(signal['id'], success)
                
                if success:
                    print("  ✅ Webhook executed successfully")
                    success_count += 1
                else:
                    print("  ❌ Webhook execution failed")
            
            # Update strategy if any signal was successful
            if success_count > 0:
                print(f"\n  Updating strategy...")
                self.update_stock_strategy(user_id, strategy_name)
                print(f"  ✅ Strategy updated")
            
            print("-" * 80)
        
        print("\n" + "="*80)
        print("STOCK SIGNAL EXECUTION COMPLETE")
        print("="*80 + "\n")
    
    def execute_all_signals(self):
        """Execute both ETF and Stock signals"""
        self.execute_etf_signals()
        self.execute_stock_signals()


if __name__ == "__main__":
    executor = SignalExecutor()
    executor.execute_all_signals()
