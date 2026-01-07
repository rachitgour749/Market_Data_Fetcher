# Automated Signal Scheduler

Automated signal generation and execution scheduler with configuration-driven timing, trading day awareness, and multi-strategy support.

## Features

- ✅ **Configuration-Driven**: All strategy schedules defined in `scheduler_config.json`
- ✅ **Trading Day Awareness**: Automatically skips NSE holidays and weekends
- ✅ **Multi-Strategy Support**: Easy to add new strategies with different timing requirements
- ✅ **Execution Tracking**: Tracks signal execution status (`pending`, `executed`, `failed`)
- ✅ **Flexible Scheduling**: Supports daily, weekly, and custom schedules
- ✅ **Robust Error Handling**: Retry logic and comprehensive logging

## Quick Start

### 1. Run Database Migration

First, add the new execution tracking fields to the database:

```bash
python migrate_signal_tables.py
```

### 2. Configure Strategies

Edit `scheduler_config.json` to enable/disable strategies and set their schedules:

```json
{
  "strategies": {
    "rotation_etf": {
      "enabled": true,
      "signal_generation": {
        "frequency": "weekly",
        "day_of_week": "monday",
        "time": "06:00",
        "skip_holidays": true
      },
      "signal_execution": {
        "frequency": "weekly",
        "execution_rule": "first_trading_day_of_week",
        "time": "10:00",
        "skip_holidays": true
      }
    }
  }
}
```

### 3. Run the Scheduler

**Automated Mode** (runs continuously):
```bash
python main.py --mode automated
```

**Manual Mode** (one-time generation):
```bash
python main.py --mode manual
```

Or run the scheduler directly:
```bash
python scheduler.py
```

## Configuration

### Strategy Configuration

Each strategy in `scheduler_config.json` has:

- **`enabled`**: Enable/disable the strategy
- **`signal_generation`**: When to generate signals
  - `frequency`: `"daily"`, `"weekly"`, `"monthly"`
  - `day_of_week`: For weekly schedules (`"monday"`, `"tuesday"`, etc.)
  - `time`: Time in HH:MM format (`"06:00"`)
  - `skip_holidays`: Skip to next trading day if scheduled day is a holiday
  - `skip_weekends`: For daily schedules, skip weekends
- **`signal_execution`**: When to execute signals
  - `frequency`: `"daily"`, `"weekly"`
  - `execution_rule`: 
    - `"first_trading_day_of_week"`: Execute on first trading day of the week
    - `"next_trading_day"`: Execute on next trading day after signal generation
    - `"same_day"`: Execute on same day as signal generation
  - `time`: Time in HH:MM format (`"10:00"`)
  - `skip_holidays`: Skip to next trading day if scheduled day is a holiday

### Trading Calendar

NSE holidays are defined in `scheduler_config.json`:

```json
{
  "trading_calendar": {
    "nse_holidays_2025": [
      "2025-01-26",
      "2025-03-14",
      ...
    ],
    "weekend_days": [5, 6]
  }
}
```

Update this list annually.

### Execution Settings

```json
{
  "execution_settings": {
    "max_retries": 3,
    "retry_delay_minutes": 5,
    "webhook_timeout_seconds": 30,
    "cleanup_executed_signals": false
  }
}
```

## Current Schedule

### Rotation ETF Strategy
- **Signal Generation**: Monday 6:00 AM (skips holidays)
- **Signal Execution**: First trading day of week at 10:00 AM

### Rotation Stocks Strategy
- **Signal Generation**: Monday 6:00 AM (skips holidays)
- **Signal Execution**: First trading day of week at 10:00 AM

## Adding New Strategies

1. Add strategy configuration to `scheduler_config.json`:

```json
{
  "strategies": {
    "my_new_strategy": {
      "enabled": true,
      "signal_generation": {
        "frequency": "daily",
        "time": "18:00",
        "skip_holidays": true,
        "skip_weekends": true
      },
      "signal_execution": {
        "frequency": "daily",
        "execution_rule": "next_trading_day",
        "time": "09:15",
        "skip_holidays": true
      },
      "generator_module": "Strategies.MyStrategy.services.signal_generator",
      "generator_class": "MySignalGenerator",
      "signal_table": "my_signal",
      "strategy_table": "my_saved_strategy"
    }
  }
}
```

2. Restart the scheduler - it will automatically pick up the new strategy!

## Execution Status Tracking

Signals now have execution status tracking:

- **`pending`**: Signal generated, waiting for execution
- **`executed`**: Signal successfully executed
- **`failed`**: Signal execution failed
- **`cancelled`**: Signal cancelled (manual intervention)

Query pending signals:
```python
from utils.signal_manager import get_signal_manager

manager = get_signal_manager()
pending = manager.fetch_pending_signals('rotation_etf')
```

## Logs

Logs are stored in the `logs/` directory:

- `scheduler_main.log`: Main scheduler events
- `signal_generation.log`: Signal generation events
- `signal_execution.log`: Signal execution events
- `etf_signals.log`: ETF-specific logs
- `stock_signals.log`: Stock-specific logs

## Utilities

### Trading Calendar

```python
from utils.trading_calendar import get_trading_calendar

calendar = get_trading_calendar()

# Check if today is a trading day
is_trading = calendar.is_trading_day(datetime.now())

# Get next trading day
next_day = calendar.get_next_trading_day()

# Get first trading day of week
first_day = calendar.get_first_trading_day_of_week()
```

### Configuration Loader

```python
from utils.config_loader import get_config_loader

loader = get_config_loader()

# Get enabled strategies
strategies = loader.get_enabled_strategies()

# Get strategy config
config = loader.get_strategy_config('rotation_etf')

# Reload config (hot-reload)
loader.reload_config()
```

### Signal Manager

```python
from utils.signal_manager import get_signal_manager

manager = get_signal_manager()

# Generate signals
result = manager.generate_signals('rotation_etf')

# Fetch pending signals
signals = manager.fetch_pending_signals('rotation_etf')

# Get statistics
stats = manager.get_signal_statistics('rotation_etf')
```

## Manual Execution

Execute signals manually:

```bash
python signal_executor.py
```

This will execute all pending signals for both ETF and Stock strategies.

## Troubleshooting

### Scheduler not starting
- Check `scheduler_config.json` for syntax errors
- Ensure at least one strategy is enabled
- Check logs in `logs/scheduler_main.log`

### Signals not executing
- Verify signals are in `pending` status
- Check if today is a trading day
- Review `logs/signal_execution.log`

### Database connection errors
- Verify database credentials in `.env`
- Run migration script if columns are missing

## Legacy Scheduler

The old scheduler is backed up as `scheduler_legacy.py` for reference.

## Requirements

Install required packages:

```bash
pip install apscheduler pytz
```

All other dependencies are already in `requirements.txt`.
