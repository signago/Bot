# Telegram Token Price Monitoring Bot

This is a Telegram bot built with Python 3.11.9 and the `python-telegram-bot` library (v20.8). It allows users to monitor cryptocurrency token prices and market caps across multiple blockchains (Solana, Ethereum, Base, BSC, Polygon) using APIs like DexScreener, GMGN, Alchemy, and CoinGecko. Features include:

- **Monitor Tokens**: Track price changes, percentage increases/decreases, or market cap thresholds.
- **Watchlist**: View and manage a personal list of monitored tokens.
- **Top Monitored & Leaderboard**: See the most monitored tokens and top gainers/losers.
- **Admin Features**: Broadcast messages, post ads, clear user watchlists, and manage ads (post, list, delete).
- **Ads System**: Displays ads to users with configurable duration and view limits.
- **SQLite Database**: Stores user watchlists and ad data in `data.db`.

## Prerequisites

- **Python 3.11.9**: Required for compatibility (other versions may cause issues).
- **Telegram Bot Token**: Obtain from [BotFather](https://t.me/BotFather).
- **API Keys** (optional, for enhanced functionality):
  - Alchemy API key for Ethereum, Polygon, and Base token metadata.
  - CoinGecko Pro API key for additional price data.

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install Python 3.11.9**:
   - Download and install from [python.org](https://www.python.org/downloads/release/python-3119/).
   - Verify with:
     ```bash
     python --version
     ```

3. **Install Dependencies**:
   - Create a virtual environment (optional but recommended):
     ```bash
     python -m venv venv
     source venv/bin/activate  # On Windows: venv\Scripts\activate
     ```
   - Install required packages:
     ```bash
     pip install -r requirements.txt
     ```

4. **Configure Environment Variables**:
   - Copy `.env.example` to `.env`:
     ```bash
     cp .env.example .env
     ```
   - Edit `.env` with your keys:
     ```
     TELEGRAM_TOKEN=your_telegram_bot_token
     ALCHEMY_API_KEY=your_alchemy_api_key
     COINGECKO_API_KEY=your_coingecko_api_key
     ```
   - Obtain keys:
     - **Telegram Token**: Create a bot via [BotFather](https://t.me/BotFather).
     - **Alchemy API Key**: Sign up at [Alchemy](https://www.alchemy.com/) and create an app for Optimism Mainnet.
     - **CoinGecko API Key**: Get a Pro API key from [CoinGecko](https://www.coingecko.com/en/api).

5. **Run the Bot**:
   ```bash
   python bot.py
   ```
   - The bot will create a `data.db` SQLite database on first run to store watchlists and ads.
   - Logs are written to `bot.log` for debugging.

## Usage

- **Start the Bot**: Send `/start` to your bot on Telegram to access the main menu.
- **Commands**:
  - `/start`: Opens the main menu (Monitor, Unmonitor, Watchlist, etc.).
  - `/debug`: Logs the command for debugging (admin use).
- **Features**:
  - **Monitor**: Add a token by entering its contract address, selecting a chain, and setting a trigger (price, % change, market cap).
  - **Unmonitor**: Remove tokens from your watchlist.
  - **Watchlist**: View your monitored tokens with market cap and DexScreener links.
  - **Top Monitored**: See the most popular tokens among users.
  - **Leaderboard**: View top gainers and losers (24h price changes).
  - **Admin Actions** (for admin user IDs defined in `bot.py`):
    - Broadcast text, photo, or video messages to all users.
    - Post ads with a message, duration (1-30 days), and max views (1-10000).
    - List and delete ads.
    - Clear a user’s watchlist by user ID.

## Database

- **File**: `data.db`
- **Tables**:
  - `users`: Stores user IDs and their watchlists (JSON).
  - `ads`: Stores ad details (message, duration, views, status).
- The database is automatically initialized on bot startup.

## Logging

- Logs are saved to `bot.log` with timestamps, levels (DEBUG, INFO, ERROR), and messages.
- Includes API request outcomes, user interactions, and errors.
- Sensitive data (e.g., API keys) should not appear in logs.

## Notes

- **API Dependencies**:
  - DexScreener and GMGN APIs are used without keys for most chains.
  - Alchemy and CoinGecko require API keys for full functionality (set in `.env`).
- **Error Handling**:
  - The bot retries failed API calls (3 attempts per API).
  - Tokens failing 5 times are skipped, and admins are notified.
- **Performance**:
  - Token monitoring runs every 35 seconds.
  - Caches (price, symbol, market cap) expire after 300 seconds to reduce API calls.
- **Security**:
  - Keep `.env` and `data.db` out of version control (included in `.gitignore`).
  - Only authorized admin IDs can access sensitive features (broadcast, ad management).

## Troubleshooting

- **Bot Fails to Start**:
  - Verify Python 3.11.9 is installed (`python --version`).
  - Ensure all dependencies are installed (`pip install -r requirements.txt`).
  - Check `.env` for valid `TELEGRAM_TOKEN`.
- **API Errors**:
  - Confirm API keys in `.env` are correct.
  - Check `bot.log` for detailed error messages.
- **Database Issues**:
  - Delete `data.db` and restart the bot to reinitialize (note: this clears all data).
- **Rate Limits**:
  - If APIs return 429 (Too Many Requests), reduce monitoring frequency or add delays in `monitor_tokens`.

## Contributing

- Fork the repository and submit pull requests for enhancements.
- Report issues via GitHub Issues.

## License

This project is licensed under the MIT License.