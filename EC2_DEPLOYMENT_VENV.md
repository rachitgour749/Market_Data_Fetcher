# EC2 Deployment with Virtual Environment (.venv)

## Complete Manual Steps for EC2 Deployment

### Step 1: Change EC2 Timezone to Asia/Kolkata

```bash
# SSH into EC2
ssh ubuntu@your-ec2-ip

# Change timezone
sudo timedatectl set-timezone Asia/Kolkata

# Verify
date
# Should show: IST (India Standard Time)
```

---

### Step 2: Upload Code to EC2

```bash
# From your local machine
scp -r signal-backend/ ubuntu@your-ec2-ip:/home/ubuntu/
```

---

### Step 3: Create Virtual Environment

```bash
# SSH into EC2
ssh ubuntu@your-ec2-ip

# Navigate to project
cd /home/ubuntu/signal-backend

# Install python3-venv if not installed
sudo apt update
sudo apt install python3-venv -y

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Your prompt should now show (.venv)
```

---

### Step 4: Install Dependencies in Virtual Environment

```bash
# Make sure .venv is activated (you should see (.venv) in prompt)
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install requirements
pip install -r requirements.txt

# Install additional packages
pip install apscheduler pytz yfinance
```

---

### Step 5: Configure Environment Variables

```bash
# Your .env file should already be uploaded
# Verify it exists
cat .env

# Should contain:
# DATABASE_URL=postgresql://...
# MARKET_DATABASE_URL=postgresql://...
```

---

### Step 6: Run Database Migration

```bash
# Make sure .venv is activated
source .venv/bin/activate

# Run migration
python migrate_signal_tables.py
```

Expected output:
```
✅ Database connection initialized
✅ Added 'executed_at' to etf_signal
✅ Added 'execution_status' to etf_signal
MIGRATION COMPLETE
```

---

### Step 7: Test the Scheduler

```bash
# Make sure .venv is activated
source .venv/bin/activate

# Test configuration
python test_scheduler.py

# Test manual mode
python main.py --mode manual
```

---

### Step 8: Create Systemd Service (with Virtual Environment)

```bash
# Create service file
sudo nano /etc/systemd/system/signal-scheduler.service
```

**Paste this content (note the ExecStart path uses .venv):**

```ini
[Unit]
Description=Automated Signal Generation and Execution Scheduler
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/signal-backend
Environment="PATH=/home/ubuntu/signal-backend/.venv/bin:/usr/bin:/usr/local/bin"
EnvironmentFile=/home/ubuntu/signal-backend/.env
ExecStart=/home/ubuntu/signal-backend/.venv/bin/python main.py --mode automated
Restart=always
RestartSec=10
StandardOutput=append:/home/ubuntu/signal-backend/logs/scheduler_main.log
StandardError=append:/home/ubuntu/signal-backend/logs/scheduler_error.log

[Install]
WantedBy=multi-user.target
```

**Key differences for .venv:**
- `Environment="PATH=/home/ubuntu/signal-backend/.venv/bin:..."`
- `ExecStart=/home/ubuntu/signal-backend/.venv/bin/python main.py --mode automated`

**Save:** `Ctrl+X`, then `Y`, then `Enter`

---

### Step 9: Enable and Start Service

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable auto-start on boot
sudo systemctl enable signal-scheduler

# Start the service
sudo systemctl start signal-scheduler

# Check status
sudo systemctl status signal-scheduler
```

---

### Step 10: Monitor the Scheduler

```bash
# View real-time logs
sudo journalctl -u signal-scheduler -f

# Check service status
sudo systemctl status signal-scheduler

# View application logs
tail -f logs/scheduler_main.log
```

---

## Your Schedule (IST)

### Signal Generation & Execution
- **Monday 6:00 AM**: Generate signals (ETF & Stock)
- **Monday 10:00 AM**: Execute signals (first trading day)

### Market Data Fetching
- **Daily 4:00 PM**: Fetch ETF data
- **Daily 4:30 PM**: Fetch Stock data
- **Daily 5:00 PM**: Fetch Index data

All jobs automatically skip weekends and NSE holidays.

---

## Useful Commands

```bash
# Activate virtual environment (when SSH into EC2)
cd /home/ubuntu/signal-backend
source .venv/bin/activate

# Stop scheduler
sudo systemctl stop signal-scheduler

# Restart scheduler
sudo systemctl restart signal-scheduler

# View logs
sudo journalctl -u signal-scheduler -f

# Check status
sudo systemctl status signal-scheduler

# Deactivate virtual environment
deactivate
```

---

## Troubleshooting

### Service Won't Start
```bash
# Check for errors
sudo journalctl -u signal-scheduler -n 50

# Test manually with venv
cd /home/ubuntu/signal-backend
source .venv/bin/activate
python main.py --mode automated
```

### Virtual Environment Issues
```bash
# Recreate virtual environment
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install apscheduler pytz yfinance
```

---

## Summary

✅ EC2 timezone set to Asia/Kolkata  
✅ Virtual environment created (.venv)  
✅ Dependencies installed in .venv  
✅ Database migration completed  
✅ Systemd service configured with .venv  
✅ Scheduler running in background  
✅ Auto-starts on reboot  
✅ Logs available for monitoring  

Your automated scheduler is now running 24/7 on AWS EC2 with virtual environment! 🚀
