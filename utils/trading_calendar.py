"""
Trading Calendar Utilities
Handles trading day calculations, holiday detection, and NSE calendar management
"""
import os
import json
from datetime import datetime, timedelta
from typing import List, Optional
import pytz


class TradingCalendar:
    """
    Trading calendar for NSE (National Stock Exchange of India)
    Handles holidays, weekends, and trading day calculations
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize trading calendar
        
        Args:
            config_path: Path to scheduler_config.json
        """
        if config_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, '..', 'scheduler_config.json')
        
        self.config_path = config_path
        self.timezone = pytz.timezone('Asia/Kolkata')
        self.holidays = []
        self.weekend_days = [5, 6]  # Saturday=5, Sunday=6
        self.load_calendar()
    
    def load_calendar(self):
        """Load calendar configuration from config file"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Get current year
            current_year = datetime.now().year
            
            # Load holidays for current year
            holiday_key = f'nse_holidays_{current_year}'
            calendar_config = config.get('trading_calendar', {})
            
            if holiday_key in calendar_config:
                self.holidays = calendar_config[holiday_key]
            else:
                # Fallback to any available year
                for key in calendar_config.keys():
                    if key.startswith('nse_holidays_'):
                        self.holidays = calendar_config[key]
                        break
            
            # Load weekend days
            self.weekend_days = calendar_config.get('weekend_days', [5, 6])
            
        except Exception as e:
            print(f"Warning: Could not load trading calendar: {e}")
            # Use default holidays for 2025
            self.holidays = [
                '2025-01-26', '2025-03-14', '2025-03-31', '2025-04-10',
                '2025-04-14', '2025-04-18', '2025-05-01', '2025-06-07',
                '2025-08-15', '2025-08-27', '2025-10-02', '2025-10-21',
                '2025-11-01', '2025-11-05', '2025-11-24', '2025-12-25'
            ]
    
    def is_weekend(self, date: datetime) -> bool:
        """
        Check if date is a weekend
        
        Args:
            date: Date to check
            
        Returns:
            True if weekend, False otherwise
        """
        return date.weekday() in self.weekend_days
    
    def is_holiday(self, date: datetime) -> bool:
        """
        Check if date is a NSE holiday
        
        Args:
            date: Date to check
            
        Returns:
            True if holiday, False otherwise
        """
        date_str = date.strftime('%Y-%m-%d')
        return date_str in self.holidays
    
    def is_trading_day(self, date: datetime) -> bool:
        """
        Check if date is a trading day (not weekend, not holiday)
        
        Args:
            date: Date to check
            
        Returns:
            True if trading day, False otherwise
        """
        return not self.is_weekend(date) and not self.is_holiday(date)
    
    def get_next_trading_day(self, date: datetime = None) -> datetime:
        """
        Get next trading day after given date
        
        Args:
            date: Starting date (default: today)
            
        Returns:
            Next trading day
        """
        if date is None:
            date = datetime.now(self.timezone)
        
        # Start from next day
        next_day = date + timedelta(days=1)
        
        # Keep incrementing until we find a trading day
        max_iterations = 30  # Prevent infinite loop
        iterations = 0
        
        while not self.is_trading_day(next_day) and iterations < max_iterations:
            next_day += timedelta(days=1)
            iterations += 1
        
        return next_day
    
    def get_previous_trading_day(self, date: datetime = None) -> datetime:
        """
        Get previous trading day before given date
        
        Args:
            date: Starting date (default: today)
            
        Returns:
            Previous trading day
        """
        if date is None:
            date = datetime.now(self.timezone)
        
        # Start from previous day
        prev_day = date - timedelta(days=1)
        
        # Keep decrementing until we find a trading day
        max_iterations = 30  # Prevent infinite loop
        iterations = 0
        
        while not self.is_trading_day(prev_day) and iterations < max_iterations:
            prev_day -= timedelta(days=1)
            iterations += 1
        
        return prev_day
    
    def get_first_trading_day_of_week(self, date: datetime = None) -> datetime:
        """
        Get first trading day of the week containing the given date
        
        Args:
            date: Reference date (default: today)
            
        Returns:
            First trading day of the week
        """
        if date is None:
            date = datetime.now(self.timezone)
        
        # Get Monday of current week
        days_since_monday = date.weekday()
        monday = date - timedelta(days=days_since_monday)
        
        # If Monday is a trading day, return it
        if self.is_trading_day(monday):
            return monday
        
        # Otherwise, get next trading day after Monday
        return self.get_next_trading_day(monday)
    
    def get_last_trading_day_of_week(self, date: datetime = None) -> datetime:
        """
        Get last trading day of the week containing the given date
        
        Args:
            date: Reference date (default: today)
            
        Returns:
            Last trading day of the week
        """
        if date is None:
            date = datetime.now(self.timezone)
        
        # Get Friday of current week
        days_until_friday = 4 - date.weekday()
        friday = date + timedelta(days=days_until_friday)
        
        # If Friday is a trading day, return it
        if self.is_trading_day(friday):
            return friday
        
        # Otherwise, get previous trading day before Friday
        return self.get_previous_trading_day(friday)
    
    def get_next_weekday(self, date: datetime, weekday: int) -> datetime:
        """
        Get next occurrence of a specific weekday
        
        Args:
            date: Starting date
            weekday: Target weekday (0=Monday, 1=Tuesday, ..., 6=Sunday)
            
        Returns:
            Next occurrence of the weekday
        """
        days_ahead = weekday - date.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        
        return date + timedelta(days=days_ahead)
    
    def adjust_for_trading_day(self, date: datetime, skip_holidays: bool = True) -> datetime:
        """
        Adjust date to next trading day if it falls on weekend/holiday
        
        Args:
            date: Date to adjust
            skip_holidays: If True, skip to next trading day; if False, return as-is
            
        Returns:
            Adjusted date
        """
        if not skip_holidays:
            return date
        
        if self.is_trading_day(date):
            return date
        
        return self.get_next_trading_day(date)
    
    def get_trading_days_between(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """
        Get all trading days between two dates (inclusive)
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of trading days
        """
        trading_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if self.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days
    
    def reload_calendar(self):
        """Reload calendar configuration from file"""
        self.load_calendar()


# Singleton instance
_calendar_instance = None


def get_trading_calendar(config_path: str = None) -> TradingCalendar:
    """
    Get singleton trading calendar instance
    
    Args:
        config_path: Path to scheduler_config.json
        
    Returns:
        TradingCalendar instance
    """
    global _calendar_instance
    
    if _calendar_instance is None:
        _calendar_instance = TradingCalendar(config_path)
    
    return _calendar_instance


# Convenience functions
def is_trading_day(date: datetime = None) -> bool:
    """Check if date is a trading day"""
    if date is None:
        date = datetime.now()
    return get_trading_calendar().is_trading_day(date)


def get_next_trading_day(date: datetime = None) -> datetime:
    """Get next trading day"""
    return get_trading_calendar().get_next_trading_day(date)


def get_first_trading_day_of_week(date: datetime = None) -> datetime:
    """Get first trading day of the week"""
    return get_trading_calendar().get_first_trading_day_of_week(date)


if __name__ == "__main__":
    # Test the trading calendar
    calendar = get_trading_calendar()
    
    print("Trading Calendar Test")
    print("=" * 60)
    
    # Test current date
    today = datetime.now(calendar.timezone)
    print(f"\nToday: {today.strftime('%Y-%m-%d %A')}")
    print(f"Is trading day: {calendar.is_trading_day(today)}")
    print(f"Is weekend: {calendar.is_weekend(today)}")
    print(f"Is holiday: {calendar.is_holiday(today)}")
    
    # Test next trading day
    next_td = calendar.get_next_trading_day(today)
    print(f"\nNext trading day: {next_td.strftime('%Y-%m-%d %A')}")
    
    # Test first trading day of week
    first_td = calendar.get_first_trading_day_of_week(today)
    print(f"First trading day of week: {first_td.strftime('%Y-%m-%d %A')}")
    
    # Test holiday
    holiday = datetime(2025, 1, 26)  # Republic Day
    print(f"\n2025-01-26 (Republic Day):")
    print(f"Is trading day: {calendar.is_trading_day(holiday)}")
    print(f"Next trading day: {calendar.get_next_trading_day(holiday).strftime('%Y-%m-%d %A')}")
