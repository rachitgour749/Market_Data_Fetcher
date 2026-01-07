"""
Main entry point for signal generation backend
Supports both manual signal generation and automated scheduling
"""
import sys
import os
import argparse
from datetime import datetime

# Add Databases path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'Databases'))

from Strategies.Rotation_ETF.services.signal_generator import LiveETFSignalGenerator
from Strategies.Rotation_Stocks.services.signal_generator import LiveStockSignalGenerator


def generate_etf_signals():
    """Generate ETF signals (manual mode)"""
    print(f"\n{'='*60}")
    print(f"ETF Signal Generation - {datetime.now()}")
    print(f"{'='*60}")
    
    generator = LiveETFSignalGenerator()
    
    try:
        result = generator.run_weekly_signal_generation()
        
        if result['success']:
            print(f"\n✅ ETF Signal Generation Successful")
            if 'signals_generated' in result:
                print(f"   Signals Generated: {result['signals_generated']}")
        else:
            print(f"\n❌ ETF Signal Generation Failed")
            print(f"   Error: {result.get('error', 'Unknown error')}")
    
    finally:
        generator.cleanup()


def generate_stock_signals():
    """Generate Stock signals (manual mode)"""
    print(f"\n{'='*60}")
    print(f"Stock Signal Generation - {datetime.now()}")
    print(f"{'='*60}")
    
    generator = LiveStockSignalGenerator()
    
    try:
        result = generator.run_weekly_signal_generation()
        
        if result['success']:
            print(f"\n✅ Stock Signal Generation Successful")
            if 'signals_generated' in result:
                print(f"   Signals Generated: {result['signals_generated']}")
        else:
            print(f"\n❌ Stock Signal Generation Failed")
            print(f"   Error: {result.get('error', 'Unknown error')}")
    
    finally:
        generator.cleanup()


def run_manual_generation():
    """Run manual signal generation for both ETF and Stock"""
    print("\n" + "="*80)
    print("MANUAL SIGNAL GENERATION MODE")
    print("="*80 + "\n")
    
    generate_etf_signals()
    generate_stock_signals()
    
    print("\n" + "="*80)
    print("MANUAL SIGNAL GENERATION COMPLETE")
    print("="*80 + "\n")


def run_automated_scheduler():
    """Run automated scheduler"""
    from scheduler import AutomatedScheduler
    
    print("\n" + "="*80)
    print("AUTOMATED SCHEDULER MODE")
    print("="*80 + "\n")
    
    scheduler = AutomatedScheduler()
    scheduler.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Signal Generation Backend')
    parser.add_argument(
        '--mode',
        choices=['manual', 'automated'],
        default='manual',
        help='Run mode: manual (one-time generation) or automated (scheduler)'
    )
    
    args = parser.parse_args()
    
    if args.mode == 'automated':
        run_automated_scheduler()
    else:
        run_manual_generation()
