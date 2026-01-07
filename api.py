"""
Signal API Server
Exposes endpoints for signal generation, execution, and retrieval.
Built with FastAPI.
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, List, Optional
import sys
import os
import uvicorn

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from Strategies.Rotation_ETF.services.signal_generator import LiveETFSignalGenerator
from Strategies.Rotation_Stocks.services.signal_generator import LiveStockSignalGenerator
from signal_executor import SignalExecutor

app = FastAPI(
    title="Signal Backend API",
    description="API for fetching, generating, and executing trading signals",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Executor
executor = SignalExecutor()

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "signal-backend"}

# ============================================================================
# FETCH SIGNALS
# ============================================================================

@app.get("/signals/generated/all")
def get_all_generated_signals():
    """Fetch all generated signals"""
    try:
        etf_signals = executor.fetch_etf_signals()
        stock_signals = executor.fetch_stock_signals()
        
        return {
            "success": True,
            "etf_signals": etf_signals,
            "stock_signals": stock_signals,
            "count": len(etf_signals) + len(stock_signals)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/signals/generated/{user_id}")
def get_user_generated_signals(user_id: str):
    """Fetch generated signals for a specific user"""
    try:
        etf_signals = executor.fetch_etf_signals(user_id=user_id)
        stock_signals = executor.fetch_stock_signals(user_id=user_id)
        
        return {
            "success": True,
            "user_id": user_id,
            "etf_signals": etf_signals,
            "stock_signals": stock_signals,
            "count": len(etf_signals) + len(stock_signals)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# GENERATE SIGNALS
# ============================================================================

def run_generation(user_id: Optional[str] = None):
    """Helper to run generation logic"""
    etf_gen = LiveETFSignalGenerator()
    stock_gen = LiveStockSignalGenerator()
    
    etf_result = etf_gen.run_weekly_signal_generation(user_id)
    etf_gen.cleanup()
    
    stock_result = stock_gen.run_weekly_signal_generation(user_id)
    stock_gen.cleanup()
    
    return {
        "etf_result": etf_result,
        "stock_result": stock_result
    }

@app.post("/signals/generate/all")
def generate_all_signals():
    """Generate signals for all users"""
    try:
        result = run_generation()
        return {
            "success": True,
            "message": "Signal generation complete",
            "details": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/signals/generate/{user_id}")
def generate_user_signals(user_id: str):
    """Generate signals for a specific user"""
    try:
        result = run_generation(user_id)
        return {
            "success": True,
            "message": f"Signal generation complete for user {user_id}",
            "details": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# EXECUTE SIGNALS
# ============================================================================

@app.post("/signals/execute/all")
def execute_all_signals():
    """Execute all signals via webhooks"""
    try:
        executor.execute_all_signals()
        return {
            "success": True,
            "message": "Signal execution initiated for all signals"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/signals/execute/{user_id}")
def execute_user_signals(user_id: str):
    """Execute signals for a specific user"""
    try:
        # Fetch filtered signals
        etf_signals = executor.fetch_etf_signals(user_id=user_id)
        stock_signals = executor.fetch_stock_signals(user_id=user_id)
        
        # Process ETF Signals
        etf_success = 0
        for signal in etf_signals:
            if _process_single_signal(signal):
                executor.update_etf_strategy(signal['user_id'], signal['strategy_name'])
                etf_success += 1
                
        # Process Stock Signals
        stock_success = 0
        for signal in stock_signals:
            if _process_single_signal(signal):
                executor.update_stock_strategy(signal['user_id'], signal['strategy_name'])
                stock_success += 1
        
        return {
            "success": True,
            "message": f"Execution complete for user {user_id}",
            "executed": {
                "etf": etf_success,
                "stock": stock_success
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _process_single_signal(signal):
    """Helper to process a single signal"""
    try:
        webhook_json = executor.prepare_webhook_json(signal)
        if webhook_json:
            return executor.execute_webhook(signal['webhook_url'], webhook_json)
    except:
        pass
    return False

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    print(f"Starting Signal API on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
