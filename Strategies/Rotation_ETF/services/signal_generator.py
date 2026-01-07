"""
Refactored ETF Signal Generator
Implements new 3-step workflow:
1. Fetch strategy details from etf_saved_strategy
2. Generate signals using 52-week high/low logic
3. Save to etf_signal table
"""
import sys
import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json

# Add Databases path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
databases_path = os.path.join(project_root, 'Databases')
if databases_path not in sys.path:
    sys.path.insert(0, databases_path)

class LiveETFSignalGenerator:
    """
    Live ETF Signal Generator - Refactored
    New 3-step workflow for signal generation
    """
    
    def __init__(self):
        """Initialize the ETF signal generator"""
        self.setup_logging()
        self.create_tables()
    
    def setup_logging(self):
        """Setup logging"""
        log_dir = os.path.join(project_root, 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(log_dir, 'etf_signals.log'), encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def create_tables(self):
        """Initialize database tables"""
        try:
            from Databases.app_data_db_connection import create_connection, init_database
            from market_data_db_connection import (
                create_connection as create_market_connection,
                init_database as init_market_database
            )
            
            if not create_connection():
                raise RuntimeError("Failed to connect to PostgreSQL database")
            
            if not init_database():
                raise RuntimeError("Failed to initialize database tables")
            
            if not create_market_connection():
                raise RuntimeError("Failed to connect to market data database")
            
            if not init_market_database():
                raise RuntimeError("Failed to initialize market data tables")
            
            self.logger.info("Database tables initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing tables: {e}")
            raise
    
    # ========================================================================
    # STEP 1: FETCH STRATEGY DETAILS
    # ========================================================================
    
    def fetch_running_strategies(self, user_id: str = None) -> List[Dict[str, Any]]:
        """
        Step 1: Fetch running strategies from etf_saved_strategy table
        
        Args:
            user_id: Optional user_id to filter strategies
            
        Returns list of strategies with:
        - user_id
        - user_code
        - strategy_name
        - tickers
        - client_information_json
        - webhook_url
        - accumulation_weeks
        """
        try:
            from Databases.app_data_db_connection import get_session
            from Databases.strategy_models import ETFSavedStrategy
            
            session = get_session()
            try:
                # Query etf_saved_strategy WHERE status='running'
                query = session.query(ETFSavedStrategy).filter(
                    ETFSavedStrategy.status == 'running'
                )
                
                if user_id:
                    query = query.filter(ETFSavedStrategy.user_id == user_id)
                    
                strategies_query = query.all()
                
                strategies = []
                for strategy in strategies_query:
                    # Parse tickers from JSON
                    try:
                        tickers = json.loads(strategy.tickers) if strategy.tickers else []
                    except:
                        tickers = []
                    
                    if not tickers:
                        self.logger.warning(f"Strategy {strategy.strategy_name} has no tickers, skipping")
                        continue
                    
                    strategies.append({
                        'user_id': strategy.user_id,
                        'user_code': strategy.user_code,  # NEW: Fetch user_code
                        'strategy_name': strategy.strategy_name,
                        'tickers': tickers,
                        'client_information_json': strategy.client_information_json,
                        'webhook_url': strategy.webhook_url,
                        'accumulation_weeks': strategy.accumulation_weeks,  # NEW: Fetch accumulation_weeks
                        'run_id': strategy.run_id  # For logging
                    })
                
                self.logger.info(f"Found {len(strategies)} running ETF strategies")
                return strategies
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error fetching running strategies: {e}")
            return []
    
    # ========================================================================
    # STEP 2: GENERATE SIGNALS USING 52-WEEK HIGH/LOW LOGIC
    # ========================================================================
    
    def load_etf_data(self, symbol: str, days_back: int = 365) -> pd.DataFrame:
        """Load ETF data from database"""
        from market_data_db_connection import get_session as get_market_data_session
        from sqlalchemy import text
        
        session = None
        try:
            session = get_market_data_session()
            
            # Calculate date range
            today = datetime.now()
            days_since_friday = (today.weekday() - 4) % 7
            last_friday = today - timedelta(days=days_since_friday)
            
            end_date = last_friday.strftime('%Y-%m-%d')
            start_date = (last_friday - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            query = text("""
                SELECT date, open, high, low, close, volume, adjusted_close
                FROM etf_data
                WHERE symbol = :symbol AND date >= CAST(:start_date AS DATE) AND date <= CAST(:end_date AS DATE)
                ORDER BY date
            """)
            
            result = session.execute(query, {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date
            })
            rows = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
            
            if df.empty:
                return pd.DataFrame()
            
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df['symbol'] = symbol
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading data for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            if session:
                session.close()
    
    def calculate_52week_metrics(self, tickers: List[str]) -> pd.DataFrame:
        """Calculate 52-week high/low metrics for ETFs"""
        try:
            all_metrics = []
            
            for symbol in tickers:
                try:
                    df = self.load_etf_data(symbol)
                    
                    if df.empty or len(df) < 100:
                        self.logger.warning(f"Insufficient data for {symbol}: {len(df)} days")
                        continue
                    
                    close_prices = df['close']
                    
                    # Get latest date and price
                    latest_date = close_prices.index[-1]
                    current_price = close_prices.iloc[-1]
                    
                    # Calculate 52-week high and low
                    window_size = min(len(close_prices), 252)
                    high_52w = close_prices.rolling(window=window_size).max().iloc[-1]
                    low_52w = close_prices.rolling(window=window_size).min().iloc[-1]
                    
                    metrics = {
                        'symbol': symbol,
                        'date': latest_date,
                        'price': current_price,
                        'high_52w': high_52w,
                        'low_52w': low_52w
                    }
                    
                    all_metrics.append(metrics)
                    
                except Exception as e:
                    self.logger.error(f"Error calculating metrics for {symbol}: {e}")
                    continue
            
            if not all_metrics:
                return pd.DataFrame()
            
            metrics_df = pd.DataFrame(all_metrics)
            self.logger.info(f"Calculated metrics for {len(metrics_df)} ETFs")
            
            return metrics_df
            
        except Exception as e:
            self.logger.error(f"Error calculating 52-week metrics: {e}")
            raise
    
    def generate_signals(self, metrics_df: pd.DataFrame, accumulation_weeks: int = 0) -> List[Dict[str, Any]]:
        """
        Generate BUY/SELL signals based on accumulation_weeks:
        - If accumulation_weeks >= 0: Generate ONLY BUY signals (1 signal)
        - If accumulation_weeks <= 0: Generate BOTH BUY and SELL signals (1 BUY + 3 SELL)
        
        BUY: Top 1 ETF closest to 52-week LOW
        SELL: Top 3 ETFs closest to 52-week HIGH
        """
        try:
            signals = []
            
            # Calculate distance percentages
            metrics_df['distance_from_low_pct'] = ((metrics_df['price'] - metrics_df['low_52w']) / metrics_df['low_52w']) * 100
            metrics_df['distance_from_high_pct'] = ((metrics_df['high_52w'] - metrics_df['price']) / metrics_df['high_52w']) * 100
            
            # BUY: Top 1 closest to 52-week low (ALWAYS generated)
            buy_candidates = metrics_df.sort_values('distance_from_low_pct', ascending=True)
            for i, (_, row) in enumerate(buy_candidates.head(1).iterrows()):
                signal = {
                    'symbol': row['symbol'],
                    'side': 'BUY',
                    'score': round(100 - row['distance_from_low_pct'], 2),
                    'reason': f"Rank {i+1} closest to 52-week min close: {row['distance_from_low_pct']:.2f}% from min close",
                    'price': row['price'],
                    'high_52w': row['high_52w'],
                    'low_52w': row['low_52w']
                }
                signals.append(signal)
            
            # SELL: Top 3 closest to 52-week high (ONLY if accumulation_weeks <= 0)
            if accumulation_weeks <= 0:
                sell_candidates = metrics_df.sort_values('distance_from_high_pct', ascending=True)
                for i, (_, row) in enumerate(sell_candidates.head(3).iterrows()):
                    signal = {
                        'symbol': row['symbol'],
                        'side': 'SELL',
                        'score': round(100 - row['distance_from_high_pct'], 2),
                        'reason': f"Rank {i+1} closest to 52-week max close: {row['distance_from_high_pct']:.2f}% from max close",
                        'price': row['price'],
                        'high_52w': row['high_52w'],
                        'low_52w': row['low_52w']
                    }
                    signals.append(signal)
                self.logger.info(f"Accumulation weeks: {accumulation_weeks} (<=0) - Generating BUY and SELL signals")
            else:
                self.logger.info(f"Accumulation weeks: {accumulation_weeks} (>=0) - Generating ONLY BUY signals")
            
            self.logger.info(f"Generated {len(signals)} signals: {len([s for s in signals if s['side'] == 'BUY'])} BUY, {len([s for s in signals if s['side'] == 'SELL'])} SELL")
            
            return signals
            
        except Exception as e:
            self.logger.error(f"Error generating signals: {e}")
            raise
    
    # ========================================================================
    # STEP 3: SAVE TO etf_signal TABLE
    # ========================================================================
    
    def save_signals(self, signals: List[Dict[str, Any]], strategy_details: Dict[str, Any]):
        """
        Step 3: Save signals to etf_signal table
        
        Saves each signal with:
        - user_id (from strategy_details)
        - user_code (from strategy_details)
        - strategy_name (from strategy_details)
        - order_side (from signal)
        - symbol_name (from signal)
        - client_json (from strategy_details)
        - webhook_url (from strategy_details)
        - signal_date
        - score, reason, price, high_52w, low_52w (for reference)
        """
        try:
            from Databases.app_data_db_connection import get_session
            from Databases.strategy_models import ETFSignal
            
            # Calculate signal date (last Friday)
            today = datetime.now()
            days_since_friday = (today.weekday() - 4) % 7
            last_friday = today - timedelta(days=days_since_friday)
            signal_date = last_friday.strftime('%Y-%m-%d')
            
            session = get_session()
            try:
                for signal in signals:
                    # Ensure user_code is an integer
                    user_code = strategy_details.get('user_code')
                    if user_code is not None:
                        try:
                            user_code = int(user_code)
                        except (ValueError, TypeError):
                            self.logger.warning(f"Invalid user_code value: {user_code}, setting to None")
                            user_code = None
                    
                    etf_signal = ETFSignal(
                        user_id=strategy_details['user_id'],
                        user_code=user_code,  # NEW: Save user_code as integer
                        strategy_name=strategy_details['strategy_name'],
                        order_side=signal['side'],
                        symbol_name=signal['symbol'],
                        client_json=strategy_details['client_information_json'],
                        webhook_url=strategy_details['webhook_url'],
                        signal_date=signal_date,
                        score=signal['score'],
                        reason=signal['reason'],
                        price=signal['price'],
                        high_52w=signal['high_52w'],
                        low_52w=signal['low_52w']
                    )
                    session.add(etf_signal)
                
                session.commit()
                self.logger.info(f"Saved {len(signals)} signals to etf_signal table for strategy: {strategy_details['strategy_name']}")
                
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error saving signals: {e}")
            raise
    
    # ========================================================================
    # MAIN WORKFLOW
    # ========================================================================
    
    def run_weekly_signal_generation(self, user_id: str = None) -> Dict[str, Any]:
        """
        Run complete 3-step workflow for all running strategies
        
        Args:
            user_id: Optional user_id to run generation only for that user
        """
        start_time = datetime.now()
        total_signals = 0
        
        try:
            print("\n" + "="*80)
            print("ETF SIGNAL GENERATION - 3-STEP WORKFLOW")
            if user_id:
                print(f"Target User: {user_id}")
            print("="*80 + "\n")
            
            # STEP 1: Fetch running strategies
            print("STEP 1: Fetching running strategies from etf_saved_strategy...")
            print("-" * 80)
            strategies = self.fetch_running_strategies(user_id)
            
            if not strategies:
                print("⚠️  No running strategies found\n")
                return {
                    'success': True,
                    'message': 'No running strategies found',
                    'signals_generated': 0
                }
            
            print(f"✅ Found {len(strategies)} running strategy(ies)\n")
            
            # Process each strategy
            for strategy in strategies:
                print(f"\nProcessing: {strategy['strategy_name']}")
                print(f"User: {strategy['user_id']}")
                print(f"ETFs: {', '.join(strategy['tickers'][:5])}{'...' if len(strategy['tickers']) > 5 else ''}")
                print()
                
                # STEP 2: Generate signals
                print("STEP 2: Generating signals using 52-week high/low logic...")
                print("-" * 80)
                
                metrics_df = self.calculate_52week_metrics(strategy['tickers'])
                
                if metrics_df.empty:
                    print("⚠️  No valid metrics calculated, skipping\n")
                    continue
                
                # Pass accumulation_weeks to generate_signals
                accumulation_weeks = strategy.get('accumulation_weeks', 0)
                signals = self.generate_signals(metrics_df, accumulation_weeks)
                
                print(f"✅ Generated {len(signals)} signals:")
                for sig in signals:
                    print(f"   {sig['side']:4s} {sig['symbol']:15s} Score: {sig['score']:.2f}")
                print()
                
                # STEP 3: Save signals
                print("STEP 3: Saving signals to etf_signal table...")
                print("-" * 80)
                
                self.save_signals(signals, strategy)
                
                print(f"✅ Saved {len(signals)} signals to database\n")
                print("=" * 80 + "\n")
                
                total_signals += len(signals)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            print("\n" + "="*80)
            print("SIGNAL GENERATION COMPLETE")
            print("="*80)
            print(f"Strategies processed: {len(strategies)}")
            print(f"Total signals generated: {total_signals}")
            print(f"Duration: {duration:.2f}s")
            print("="*80 + "\n")
            
            return {
                'success': True,
                'strategies_processed': len(strategies),
                'signals_generated': total_signals,
                'duration_seconds': duration
            }
            
        except Exception as e:
            self.logger.error(f"Signal generation failed: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                'success': False,
                'error': str(e)
            }
    
    def cleanup(self):
        """Cleanup resources"""
        pass


if __name__ == "__main__":
    generator = LiveETFSignalGenerator()
    try:
        result = generator.run_weekly_signal_generation()
        exit(0 if result['success'] else 1)
    finally:
        generator.cleanup()
