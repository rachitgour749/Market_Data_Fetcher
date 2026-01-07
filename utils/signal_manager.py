"""
Signal Manager
Centralized signal generation and execution management
"""
import sys
import os
import importlib
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import pytz

# Add Databases path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, os.path.join(project_root, 'Databases'))

from Databases.app_data_db_connection import get_session
from Databases.strategy_models import ETFSignal, StockSignal
from utils.config_loader import get_config_loader


class SignalManager:
    """
    Manages signal generation and execution for all strategies
    """
    
    def __init__(self):
        """Initialize signal manager"""
        self.config_loader = get_config_loader()
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.timezone(self.config_loader.get_timezone())
    
    def generate_signals(self, strategy_name: str) -> Dict[str, Any]:
        """
        Generate signals for a specific strategy
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            Result dictionary with success status and details
        """
        try:
            # Get strategy configuration
            strategy_config = self.config_loader.get_strategy_config(strategy_name)
            
            if not strategy_config:
                return {
                    'success': False,
                    'error': f'Strategy not found: {strategy_name}'
                }
            
            if not strategy_config.get('enabled', False):
                return {
                    'success': False,
                    'error': f'Strategy not enabled: {strategy_name}'
                }
            
            # Get generator module and class
            generator_module = strategy_config.get('generator_module')
            generator_class = strategy_config.get('generator_class')
            
            if not generator_module or not generator_class:
                return {
                    'success': False,
                    'error': f'Generator module or class not configured for: {strategy_name}'
                }
            
            # Dynamically import and instantiate generator
            self.logger.info(f"Loading generator: {generator_module}.{generator_class}")
            
            module = importlib.import_module(generator_module)
            GeneratorClass = getattr(module, generator_class)
            
            # Create generator instance
            generator = GeneratorClass()
            
            # Run signal generation
            self.logger.info(f"Running signal generation for: {strategy_name}")
            result = generator.run_weekly_signal_generation()
            
            # Cleanup
            if hasattr(generator, 'cleanup'):
                generator.cleanup()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error generating signals for {strategy_name}: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                'success': False,
                'error': str(e)
            }
    
    def fetch_pending_signals(self, strategy_name: str) -> List[Dict[str, Any]]:
        """
        Fetch pending signals for a specific strategy
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            List of pending signal dictionaries
        """
        try:
            strategy_config = self.config_loader.get_strategy_config(strategy_name)
            
            if not strategy_config:
                self.logger.error(f"Strategy not found: {strategy_name}")
                return []
            
            signal_table = strategy_config.get('signal_table')
            
            if not signal_table:
                self.logger.error(f"Signal table not configured for: {strategy_name}")
                return []
            
            session = get_session()
            try:
                # Determine which model to use
                if signal_table == 'etf_signal':
                    SignalModel = ETFSignal
                elif signal_table == 'stock_signal':
                    SignalModel = StockSignal
                else:
                    self.logger.error(f"Unknown signal table: {signal_table}")
                    return []
                
                # Fetch pending signals
                query = session.query(SignalModel).filter(
                    SignalModel.execution_status == 'pending'
                )
                
                signals = []
                for signal in query.all():
                    signals.append({
                        'id': signal.id,
                        'user_id': signal.user_id,
                        'strategy_name': signal.strategy_name,
                        'order_side': signal.order_side,
                        'symbol_name': signal.symbol_name,
                        'client_json': signal.client_json,
                        'webhook_url': signal.webhook_url,
                        'price': signal.price,
                        'signal_date': signal.signal_date,
                        'execution_status': signal.execution_status
                    })
                
                self.logger.info(f"Fetched {len(signals)} pending signals for {strategy_name}")
                return signals
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error fetching pending signals for {strategy_name}: {e}")
            return []
    
    def mark_signal_executed(self, signal_id: int, signal_table: str, success: bool = True):
        """
        Mark a signal as executed or failed
        
        Args:
            signal_id: Signal ID
            signal_table: Signal table name ('etf_signal' or 'stock_signal')
            success: Whether execution was successful
        """
        try:
            session = get_session()
            try:
                # Determine which model to use
                if signal_table == 'etf_signal':
                    SignalModel = ETFSignal
                elif signal_table == 'stock_signal':
                    SignalModel = StockSignal
                else:
                    self.logger.error(f"Unknown signal table: {signal_table}")
                    return
                
                # Update signal
                signal = session.query(SignalModel).filter(SignalModel.id == signal_id).first()
                
                if signal:
                    signal.executed_at = datetime.now(self.timezone)
                    signal.execution_status = 'executed' if success else 'failed'
                    session.commit()
                    
                    self.logger.info(f"Marked signal {signal_id} as {signal.execution_status}")
                else:
                    self.logger.warning(f"Signal {signal_id} not found in {signal_table}")
                    
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error marking signal as executed: {e}")
    
    def mark_signals_executed_batch(self, signal_ids: List[int], signal_table: str, success: bool = True):
        """
        Mark multiple signals as executed or failed
        
        Args:
            signal_ids: List of signal IDs
            signal_table: Signal table name
            success: Whether execution was successful
        """
        for signal_id in signal_ids:
            self.mark_signal_executed(signal_id, signal_table, success)
    
    def get_signal_statistics(self, strategy_name: str) -> Dict[str, Any]:
        """
        Get signal statistics for a strategy
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            Statistics dictionary
        """
        try:
            strategy_config = self.config_loader.get_strategy_config(strategy_name)
            
            if not strategy_config:
                return {}
            
            signal_table = strategy_config.get('signal_table')
            
            if not signal_table:
                return {}
            
            session = get_session()
            try:
                # Determine which model to use
                if signal_table == 'etf_signal':
                    SignalModel = ETFSignal
                elif signal_table == 'stock_signal':
                    SignalModel = StockSignal
                else:
                    return {}
                
                # Get counts by status
                total = session.query(SignalModel).count()
                pending = session.query(SignalModel).filter(SignalModel.execution_status == 'pending').count()
                executed = session.query(SignalModel).filter(SignalModel.execution_status == 'executed').count()
                failed = session.query(SignalModel).filter(SignalModel.execution_status == 'failed').count()
                
                return {
                    'total': total,
                    'pending': pending,
                    'executed': executed,
                    'failed': failed
                }
                
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error getting signal statistics: {e}")
            return {}


# Singleton instance
_signal_manager_instance = None


def get_signal_manager() -> SignalManager:
    """
    Get singleton signal manager instance
    
    Returns:
        SignalManager instance
    """
    global _signal_manager_instance
    
    if _signal_manager_instance is None:
        _signal_manager_instance = SignalManager()
    
    return _signal_manager_instance


if __name__ == "__main__":
    # Test signal manager
    import logging
    logging.basicConfig(level=logging.INFO)
    
    manager = get_signal_manager()
    
    print("Signal Manager Test")
    print("=" * 60)
    
    # Test fetching pending signals
    for strategy_name in ['rotation_etf', 'rotation_stocks']:
        print(f"\n{strategy_name}:")
        
        signals = manager.fetch_pending_signals(strategy_name)
        print(f"  Pending signals: {len(signals)}")
        
        stats = manager.get_signal_statistics(strategy_name)
        print(f"  Statistics: {stats}")
