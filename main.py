import os
import csv
import time
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import yfinance as yf
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """API Configuration - loads from environment variables"""
    API_KEY = os.getenv("KITE_API_KEY", "")
    API_SECRET = os.getenv("KITE_API_SECRET", "")
    STOCKS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", 
              "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "LT"]
    TICKER_MAP = {
        "RELIANCE": "RELIANCE.NS",
        "TCS": "TCS.NS",
        "INFY": "INFY.NS",
        "HDFCBANK": "HDFCBANK.NS",
        "ICICIBANK": "ICICIBANK.NS",
        "HINDUNILVR": "HINDUNILVR.NS",
        "ITC": "ITC.NS",
        "SBIN": "SBIN.NS",
        "BHARTIARTL": "BHARTIARTL.NS",
        "LT": "LT.NS"
    }
    DATA_DIR = "historical_data"
    TOKEN_FILE = "access_token.txt"


# ============================================================================
# AUTHENTICATION MANAGER
# ============================================================================

class AuthenticationManager:
    """Handles Zerodha Kite Connect authentication"""
    
    def __init__(self):
        self.kite = KiteConnect(api_key=Config.API_KEY)
        self.access_token = None
        self.authenticated = False
        self.load_existing_token()
    
    def load_existing_token(self):
        """Load access token from file if it exists"""
        if os.path.exists(Config.TOKEN_FILE):
            try:
                with open(Config.TOKEN_FILE, 'r') as f:
                    self.access_token = f.read().strip()
                    self.kite.set_access_token(self.access_token)
                    self.authenticated = True
                    print(f"✅ Loaded existing access token from {Config.TOKEN_FILE}")
                    return True
            except Exception as e:
                print(f"⚠️  Could not load token: {e}")
        return False
    
    def get_login_url(self):
        """Get the login URL for user authentication"""
        login_url = self.kite.login_url()
        print(f"\n🔐 Login URL (copy and paste in browser):")
        print(f"   {login_url}\n")
        return login_url
    
    def authenticate_with_token(self, request_token):
        """Exchange request_token for access_token"""
        try:
            print("\n🔄 Exchanging request_token for access_token...")
            data = self.kite.generate_session(
                request_token, 
                api_secret=Config.API_SECRET
            )
            self.access_token = data["access_token"]
            self.kite.set_access_token(self.access_token)
            self.authenticated = True
            
            # Save token to file
            with open(Config.TOKEN_FILE, 'w') as f:
                f.write(self.access_token)
            
            print(f"✅ Authentication successful!")
            print(f"✅ Access token saved to {Config.TOKEN_FILE}")
            print(f"✅ You're now registered as: {data.get('user_name', 'User')}")
            return True
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            self.authenticated = False
            return False
    
    def is_authenticated(self):
        """Check if we have a valid access token"""
        return self.authenticated


# ============================================================================
# MARKET DATA FETCHER
# ============================================================================

class MarketDataFetcher:
    """Fetches historical data from yfinance"""
    
    def __init__(self):
        self.rate_limit_delay = 0.5
    
    def fetch_historical_data(self, symbol, days=365):
        """Fetch historical OHLC data using yfinance"""
        try:
            ticker = Config.TICKER_MAP.get(symbol, symbol + ".NS")
            
            to_date = datetime.now()
            from_date = to_date - timedelta(days=days)
            
            print(f"   📥 Fetching {symbol} ({ticker})...", end=" ", flush=True)
            
            data = yf.download(
                ticker,
                start=from_date.strftime("%Y-%m-%d"),
                end=to_date.strftime("%Y-%m-%d"),
                interval="1d",
                progress=False
            )
            
            if data.empty:
                print(f"❌ No data found")
                return None
            
            print(f"✅ ({len(data)} candles)")
            
            # Convert to standard format
            result = []
            for idx, row in data.iterrows():
                result.append({
                    'date': idx,
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume']) if 'Volume' in row else 0
                })
            
            time.sleep(self.rate_limit_delay)
            return result
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def save_data_to_csv(self, symbol, data):
        """Save historical data to CSV file"""
        if not data:
            print(f"⚠️  No data to save for {symbol}")
            return False
        
        try:
            filepath = os.path.join(Config.DATA_DIR, f"{symbol}.csv")
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(
                    f, 
                    fieldnames=['date', 'open', 'high', 'low', 'close', 'volume']
                )
                writer.writeheader()
                
                for candle in data:
                    writer.writerow({
                        'date': candle['date'],
                        'open': candle['open'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'close': candle['close'],
                        'volume': candle['volume']
                    })
            
            print(f"   💾 Saved {symbol}.csv ({len(data)} rows)")
            return True
        except Exception as e:
            print(f"❌ Error saving {symbol}: {e}")
            return False


# ============================================================================
# DATA STORAGE MANAGER
# ============================================================================

class DataStorageManager:
    """Manages data directory and file operations"""
    
    @staticmethod
    def setup_directories():
        """Create necessary directories"""
        if not os.path.exists(Config.DATA_DIR):
            os.makedirs(Config.DATA_DIR)
            print(f"✅ Created {Config.DATA_DIR}/ directory")
        return True
    
    @staticmethod
    def load_csv(symbol):
        """Load stock data from CSV"""
        filepath = os.path.join(Config.DATA_DIR, f"{symbol}.csv")
        if not os.path.exists(filepath):
            print(f"⚠️  File not found: {filepath}")
            return None
        
        try:
            data = []
            with open(filepath, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.append({
                        'date': row['date'],
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': int(row['volume'])
                    })
            return data
        except Exception as e:
            print(f"❌ Error loading {symbol}: {e}")
            return None
    
    @staticmethod
    def get_status_report():
        """Check which files have been downloaded"""
        print("\n" + "="*60)
        print("DATA STATUS REPORT")
        print("="*60)
        
        if not os.path.exists(Config.DATA_DIR):
            print(f"⚠️  Directory {Config.DATA_DIR}/ does not exist")
            return
        
        files = os.listdir(Config.DATA_DIR)
        csv_files = [f for f in files if f.endswith('.csv')]
        
        print(f"\n✅ Found {len(csv_files)} CSV files:")
        for f in sorted(csv_files):
            filepath = os.path.join(Config.DATA_DIR, f)
            size = os.path.getsize(filepath) / 1024
            with open(filepath, 'r') as file:
                rows = len(file.readlines()) - 1
            print(f"   • {f} ({size:.1f} KB, {rows} rows)")
        
        missing = set(Config.STOCKS) - set(s.replace('.csv', '') for s in csv_files)
        if missing:
            print(f"\n⚠️  Missing data for: {', '.join(sorted(missing))}")
        else:
            print(f"\n✅ All {len(Config.STOCKS)} stocks have data!")


# ============================================================================
# MAIN WORKFLOW
# ============================================================================

class ZerodhaBacktestingWorkflow:
    """Dev workflow: Kite auth + yfinance data"""
    
    def __init__(self):
        self.auth_manager = AuthenticationManager()
        self.storage_manager = DataStorageManager()
        self.fetcher = MarketDataFetcher()
    
    def authenticate(self, request_token=None):
        """Handle Kite authentication"""
        if request_token:
            return self.auth_manager.authenticate_with_token(request_token)
        else:
            print("\n" + "="*60)
            print("KITE API - AUTHENTICATION FLOW")
            print("="*60)
            self.auth_manager.get_login_url()
            print("Steps:")
            print("1. Open the URL above in your browser")
            print("2. Log in with your Zerodha credentials")
            print("3. Complete 2FA if prompted")
            print("4. You'll be redirected to callback URL")
            print("5. Copy the 'request_token' from callback")
            print("\nThen run:")
            print("   authenticate_with_kite(request_token='YOUR_TOKEN')")
            return False
    
    def fetch_and_save_data(self, days=365):
        """Fetch data for all stocks"""
        
        if not self.auth_manager.is_authenticated():
            print("\n❌ NOT AUTHENTICATED")
            print("You must authenticate first:")
            print("   authenticate_with_kite()")
            return False
        
        print("\n" + "="*60)
        print("FETCHING HISTORICAL DATA")
        print("="*60)
        print(f"Status: ✅ Authenticated with Kite API")
        print(f"Data Source: yfinance (free)")
        
        self.storage_manager.setup_directories()
        
        print(f"\nFetching data for {len(Config.STOCKS)} stocks ({days} days)...\n")
        
        success_count = 0
        for symbol in Config.STOCKS:
            data = self.fetcher.fetch_historical_data(symbol, days=days)
            if self.fetcher.save_data_to_csv(symbol, data):
                success_count += 1
        
        print(f"\n✅ Successfully saved {success_count}/{len(Config.STOCKS)} stocks")
        return True
    
    def get_status_report(self):
        """Display data status"""
        self.storage_manager.get_status_report()
    
    def load_data(self, symbol):
        """Load a specific stock's data"""
        return self.storage_manager.load_csv(symbol)


# ============================================================================
# QUICK START FUNCTIONS
# ============================================================================

def authenticate_with_kite(request_token=None):
    """Authenticate with Kite API - optionally pass request_token"""
    workflow = ZerodhaBacktestingWorkflow()
    if request_token:
        workflow.authenticate(request_token=request_token)
    else:
        workflow.authenticate()


def fetch_data():
    """Fetch historical data (requires authentication first)"""
    workflow = ZerodhaBacktestingWorkflow()
    
    if not workflow.auth_manager.is_authenticated():
        print("❌ You must authenticate first!")
        print("Run: authenticate_with_kite()")
        return
    
    workflow.fetch_and_save_data(days=365)


def check_status():
    """Check downloaded data status"""
    workflow = ZerodhaBacktestingWorkflow()
    workflow.get_status_report()


def analyze_data():
    """Analyze downloaded data"""
    print("\n" + "="*60)
    print("DATA ANALYSIS")
    print("="*60)
    
    workflow = ZerodhaBacktestingWorkflow()
    
    for symbol in Config.STOCKS:
        data = workflow.load_data(symbol)
        if data:
            close_prices = [d['close'] for d in data]
            print(f"\n{symbol}:")
            print(f"  Records: {len(data)}")
            print(f"  Price Range: ₹{min(close_prices):.2f} - ₹{max(close_prices):.2f}")
            print(f"  Current: ₹{close_prices[-1]:.2f}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    print("\n" + "╔" + "="*58 + "╗")
    print("║" + " "*58 + "║")
    print("║" + "KITE API DEV MODE - Backtesting Setup".center(58) + "║")
    print("║" + "Practice authentication + yfinance data".center(58) + "║")
    print("║" + " "*58 + "║")
    print("╚" + "="*58 + "╝")
    
    print("\n📋 WORKFLOW:\n")
    print("1. Get login URL:")
    print("   >>> authenticate_with_kite()\n")
    print("2. After you get request_token from callback URL:")
    print("   >>> authenticate_with_kite(request_token='YOUR_TOKEN')\n")
    print("3. Fetch data:")
    print("   >>> fetch_data()\n")
    print("4. Check status:")
    print("   >>> check_status()\n")
    print("5. Analyze data:")
    print("   >>> analyze_data()\n")