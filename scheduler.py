"""
Automated Signal Generation and Execution Scheduler
Uses APScheduler for flexible, configuration-driven scheduling
Supports multiple strategies with different timing requirements
"""
import sys
import os
import logging
import importlib
from datetime import datetime, time
from typing import Dict, Any
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# Add paths
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'Databases'))
sys.path.insert(0, current_dir)

from utils.config_loader import get_config_loader
from utils.trading_calendar import get_trading_calendar
from utils.signal_manager import get_signal_manager
from signal_executor import SignalExecutor


class AutomatedScheduler:
    """
    Automated scheduler for signal generation and execution
    Supports multiple strategies with configurable timing
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize automated scheduler
        
        Args:
            config_path: Path to scheduler_config.json
        """
        self.config_loader = get_config_loader(config_path)
        self.trading_calendar = get_trading_calendar(config_path)
        self.signal_manager = get_signal_manager()
        self.executor = SignalExecutor()
        
        # Setup logging
        self.setup_logging()
        
        # Create scheduler
        timezone = pytz.timezone(self.config_loader.get_timezone())
        self.scheduler = BlockingScheduler(timezone=timezone)
        
        # Add event listeners
        self.scheduler.add_listener(self.job_executed_listener, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self.job_error_listener, EVENT_JOB_ERROR)
        
        self.logger.info("Automated Scheduler initialized")
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config_loader.get_logging_config()
        log_dir = log_config.get('log_dir', 'logs')
        
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(log_dir, 'scheduler_main.log'), encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def job_executed_listener(self, event):
        """Listener for successful job execution"""
        self.logger.info(f"Job {event.job_id} executed successfully")
    
    def job_error_listener(self, event):
        """Listener for job execution errors"""
        self.logger.error(f"Job {event.job_id} failed: {event.exception}")
    
    # ========================================================================
    # SIGNAL GENERATION JOBS
    # ========================================================================
    
    def generate_signals_job(self, strategy_name: str):
        """
        Job to generate signals for a strategy
        
        Args:
            strategy_name: Name of the strategy
        """
        try:
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"Starting Signal Generation: {strategy_name}")
            self.logger.info(f"Time: {datetime.now()}")
            self.logger.info(f"{'='*80}\n")
            
            # Check if today is a trading day (if skip_holidays is enabled)
            strategy_config = self.config_loader.get_strategy_config(strategy_name)
            gen_config = strategy_config.get('signal_generation', {})
            
            if gen_config.get('skip_holidays', False):
                today = datetime.now(self.trading_calendar.timezone)
                if not self.trading_calendar.is_trading_day(today):
                    self.logger.info(f"Skipping signal generation - today is not a trading day")
                    return
            
            # Generate signals
            result = self.signal_manager.generate_signals(strategy_name)
            
            if result.get('success'):
                self.logger.info(f"\n✅ Signal Generation Successful for {strategy_name}")
                if 'signals_generated' in result:
                    self.logger.info(f"   Signals Generated: {result['signals_generated']}")
            else:
                self.logger.error(f"\n❌ Signal Generation Failed for {strategy_name}")
                self.logger.error(f"   Error: {result.get('error', 'Unknown error')}")
            
            self.logger.info(f"\n{'='*80}\n")
            
        except Exception as e:
            self.logger.error(f"Error in signal generation job for {strategy_name}: {e}")
            import traceback
            traceback.print_exc()
    
    # ========================================================================
    # SIGNAL EXECUTION JOBS
    # ========================================================================
    
    def execute_signals_job(self, strategy_name: str):
        """
        Job to execute signals for a strategy
        
        Args:
            strategy_name: Name of the strategy
        """
        try:
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"Starting Signal Execution: {strategy_name}")
            self.logger.info(f"Time: {datetime.now()}")
            self.logger.info(f"{'='*80}\n")
            
            # Check if today is a trading day (if skip_holidays is enabled)
            strategy_config = self.config_loader.get_strategy_config(strategy_name)
            exec_config = strategy_config.get('signal_execution', {})
            
            if exec_config.get('skip_holidays', False):
                today = datetime.now(self.trading_calendar.timezone)
                if not self.trading_calendar.is_trading_day(today):
                    self.logger.info(f"Skipping signal execution - today is not a trading day")
                    return
            
            # Execute based on strategy type
            signal_table = strategy_config.get('signal_table')
            
            if signal_table == 'etf_signal':
                self.executor.execute_etf_signals()
            elif signal_table == 'stock_signal':
                self.executor.execute_stock_signals()
            else:
                self.logger.error(f"Unknown signal table: {signal_table}")
            
            self.logger.info(f"\n{'='*80}\n")
            
        except Exception as e:
            self.logger.error(f"Error in signal execution job for {strategy_name}: {e}")
            import traceback
            traceback.print_exc()
    
    # ========================================================================
    # DATA FETCHING JOBS
    # ========================================================================
    
    def data_fetch_job(self, strategy_name: str):
        """
        Job to fetch market data (ETF, Stock, Index)
        
        Args:
            strategy_name: Name of the data fetch strategy
        """
        try:
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"Starting Data Fetch: {strategy_name}")
            self.logger.info(f"Time: {datetime.now()}")
            self.logger.info(f"{'='*80}\n")
            
            # Get strategy configuration
            strategy_config = self.config_loader.get_strategy_config(strategy_name)
            fetch_config = strategy_config.get('data_fetch', {})
            
            # Check if today is a trading day (if skip_holidays is enabled)
            if fetch_config.get('skip_holidays', False):
                today = datetime.now(self.trading_calendar.timezone)
                if not self.trading_calendar.is_trading_day(today):
                    self.logger.info(f"Skipping data fetch - today is not a trading day")
                    return
            
            # Get fetch module and function
            fetch_module = strategy_config.get('fetch_module')
            fetch_function = strategy_config.get('fetch_function', 'main')
            
            if not fetch_module:
                self.logger.error(f"Fetch module not configured for: {strategy_name}")
                return
            
            # Dynamically import and run the fetch function
            self.logger.info(f"Loading module: {fetch_module}")
            
            module = importlib.import_module(fetch_module)
            fetch_func = getattr(module, fetch_function)
            
            # Run the data fetch
            self.logger.info(f"Running data fetch function: {fetch_function}")
            fetch_func()
            
            self.logger.info(f"\n✅ Data Fetch Successful for {strategy_name}")
            self.logger.info(f"\n{'='*80}\n")
            
        except Exception as e:
            self.logger.error(f"Error in data fetch job for {strategy_name}: {e}")
            import traceback
            traceback.print_exc()
    
    # ========================================================================
    # JOB SCHEDULING
    # ========================================================================
    
    def schedule_signal_generation(self, strategy_name: str, gen_config: Dict[str, Any]):
        """
        Schedule signal generation job for a strategy
        
        Args:
            strategy_name: Name of the strategy
            gen_config: Signal generation configuration
        """
        frequency = gen_config.get('frequency', 'weekly')
        time_str = gen_config.get('time', '06:00')
        hour, minute = map(int, time_str.split(':'))
        
        job_id = f"generate_{strategy_name}"
        
        if frequency == 'weekly':
            day_of_week = gen_config.get('day_of_week', 'monday')
            day_map = {
                'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                'friday': 4, 'saturday': 5, 'sunday': 6
            }
            day_num = day_map.get(day_of_week.lower(), 0)
            
            trigger = CronTrigger(
                day_of_week=day_num,
                hour=hour,
                minute=minute,
                timezone=self.scheduler.timezone
            )
            
            self.scheduler.add_job(
                self.generate_signals_job,
                trigger=trigger,
                args=[strategy_name],
                id=job_id,
                name=f"Generate signals for {strategy_name}",
                replace_existing=True
            )
            
            self.logger.info(f"Scheduled signal generation for {strategy_name}: {day_of_week} at {time_str}")
            
        elif frequency == 'daily':
            skip_weekends = gen_config.get('skip_weekends', True)
            
            if skip_weekends:
                trigger = CronTrigger(
                    day_of_week='mon-fri',
                    hour=hour,
                    minute=minute,
                    timezone=self.scheduler.timezone
                )
            else:
                trigger = CronTrigger(
                    hour=hour,
                    minute=minute,
                    timezone=self.scheduler.timezone
                )
            
            self.scheduler.add_job(
                self.generate_signals_job,
                trigger=trigger,
                args=[strategy_name],
                id=job_id,
                name=f"Generate signals for {strategy_name}",
                replace_existing=True
            )
            
            self.logger.info(f"Scheduled signal generation for {strategy_name}: daily at {time_str}")
    
    def schedule_signal_execution(self, strategy_name: str, exec_config: Dict[str, Any]):
        """
        Schedule signal execution job for a strategy
        
        Args:
            strategy_name: Name of the strategy
            exec_config: Signal execution configuration
        """
        frequency = exec_config.get('frequency', 'weekly')
        time_str = exec_config.get('time', '10:00')
        hour, minute = map(int, time_str.split(':'))
        execution_rule = exec_config.get('execution_rule', 'first_trading_day_of_week')
        
        job_id = f"execute_{strategy_name}"
        
        if frequency == 'weekly':
            # For weekly execution, we schedule on Monday
            # The job itself will check if it's a trading day
            trigger = CronTrigger(
                day_of_week=0,  # Monday
                hour=hour,
                minute=minute,
                timezone=self.scheduler.timezone
            )
            
            self.scheduler.add_job(
                self.execute_signals_job,
                trigger=trigger,
                args=[strategy_name],
                id=job_id,
                name=f"Execute signals for {strategy_name}",
                replace_existing=True
            )
            
            self.logger.info(f"Scheduled signal execution for {strategy_name}: Monday at {time_str} ({execution_rule})")
            
        elif frequency == 'daily':
            # For daily execution, schedule on weekdays
            trigger = CronTrigger(
                day_of_week='mon-fri',
                hour=hour,
                minute=minute,
                timezone=self.scheduler.timezone
            )
            
            self.scheduler.add_job(
                self.execute_signals_job,
                trigger=trigger,
                args=[strategy_name],
                id=job_id,
                name=f"Execute signals for {strategy_name}",
                replace_existing=True
            )
            
            self.logger.info(f"Scheduled signal execution for {strategy_name}: daily at {time_str}")
    
    def schedule_data_fetch(self, strategy_name: str, fetch_config: Dict[str, Any]):
        """
        Schedule data fetch job for a strategy
        
        Args:
            strategy_name: Name of the strategy
            fetch_config: Data fetch configuration
        """
        frequency = fetch_config.get('frequency', 'daily')
        time_str = fetch_config.get('time', '16:00')
        hour, minute = map(int, time_str.split(':'))
        
        job_id = f"fetch_{strategy_name}"
        
        if frequency == 'daily':
            skip_weekends = fetch_config.get('skip_weekends', True)
            
            if skip_weekends:
                trigger = CronTrigger(
                    day_of_week='mon-fri',
                    hour=hour,
                    minute=minute,
                    timezone=self.scheduler.timezone
                )
            else:
                trigger = CronTrigger(
                    hour=hour,
                    minute=minute,
                    timezone=self.scheduler.timezone
                )
            
            self.scheduler.add_job(
                self.data_fetch_job,
                trigger=trigger,
                args=[strategy_name],
                id=job_id,
                name=f"Fetch data for {strategy_name}",
                replace_existing=True
            )
            
            self.logger.info(f"Scheduled data fetch for {strategy_name}: daily at {time_str}")
    
    def setup_all_jobs(self):
        """Setup all jobs for enabled strategies"""
        enabled_strategies = self.config_loader.get_enabled_strategies()
        
        self.logger.info(f"\nSetting up jobs for {len(enabled_strategies)} enabled strategies")
        self.logger.info(f"Enabled strategies: {', '.join(enabled_strategies)}\n")
        
        for strategy_name in enabled_strategies:
            strategy_config = self.config_loader.get_strategy_config(strategy_name)
            
            # Check if this is a signal generation/execution strategy
            if 'signal_generation' in strategy_config:
                # Schedule signal generation
                gen_config = strategy_config.get('signal_generation', {})
                self.schedule_signal_generation(strategy_name, gen_config)
                
                # Schedule signal execution
                exec_config = strategy_config.get('signal_execution', {})
                self.schedule_signal_execution(strategy_name, exec_config)
            
            # Check if this is a data fetch strategy
            elif 'data_fetch' in strategy_config:
                # Schedule data fetch
                fetch_config = strategy_config.get('data_fetch', {})
                self.schedule_data_fetch(strategy_name, fetch_config)
        
        self.logger.info("\nAll jobs scheduled successfully\n")
    
    def print_scheduled_jobs(self):
        """Print all scheduled jobs"""
        jobs = self.scheduler.get_jobs()
        
        print("\n" + "="*80)
        print("SCHEDULED JOBS")
        print("="*80)
        
        for job in jobs:
            print(f"\nJob ID: {job.id}")
            print(f"Name: {job.name}")
            try:
                # Try to get next run time
                next_run = self.scheduler.get_job(job.id).next_run_time if hasattr(job, 'next_run_time') else 'Not scheduled'
                print(f"Next run: {next_run}")
            except:
                print(f"Next run: Pending")
            print(f"Trigger: {job.trigger}")
        
        print("\n" + "="*80 + "\n")
    
    def start(self):
        """Start the scheduler"""
        try:
            self.logger.info("\n" + "="*80)
            self.logger.info("AUTOMATED SIGNAL SCHEDULER STARTING")
            self.logger.info("="*80 + "\n")
            
            # Setup all jobs
            self.setup_all_jobs()
            
            # Print scheduled jobs
            self.print_scheduled_jobs()
            
            self.logger.info("Scheduler is running. Press Ctrl+C to stop.\n")
            
            # Start scheduler
            self.scheduler.start()
            
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("\nShutting down scheduler...")
            self.shutdown()
    
    def shutdown(self):
        """Shutdown the scheduler gracefully"""
        self.logger.info("Scheduler shutdown complete")
        self.scheduler.shutdown()


if __name__ == "__main__":
    # Create and start automated scheduler
    scheduler = AutomatedScheduler()
    scheduler.start()
