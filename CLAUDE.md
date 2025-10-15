# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Telegram Parser Bot** - An asynchronous Telegram bot for collecting user data from Telegram groups with SQLite database, analytics, and multi-format export capabilities. Built with aiogram 3.4.1 for bot interactions and telethon 1.34.0 for parsing Telegram groups.

## Running the Bot

```bash
# Install dependencies
pip install -r requirements.txt

# Run the bot
python run.py
```

**First-time setup**: You must configure `bot/data/parser_cfg.py` with your credentials (BOT_TOKEN, API credentials). See `bot/data/parser_cfg.example.py` for template. The bot will prompt for Telegram verification codes on first run.

## Core Architecture

### Entry Points and Initialization Flow
- `run.py` - Entry point that configures Windows console encoding and launches `bot/main.py`
- `bot/main.py` - Main application orchestrator that:
  1. Initializes logging via `utils/logging_utils.py`
  2. Loads and validates config from `utils/config_manager.py`
  3. Initializes SQLite database via `utils/database.py`
  4. Registers all handlers from `handlers/` directory
  5. Starts aiogram polling with bot instance from `aiogram_loader.py`

### Configuration System
- `bot/data/parser_cfg.py` - Main config file (git-ignored, contains secrets)
- `bot/data/parser_cfg.example.py` - Template with example structure
- `bot/utils/config_manager.py` - Config loader with validation
- Config includes: BOT_TOKEN, accounts list with phone_number/api_id/api_hash per account

### Database Architecture (SQLite)
- **Location**: `bot/data/all_users.db`
- **Manager**: `bot/utils/database.py` - `DatabaseManager` class with connection pooling
- **Schema**: `users` table with fields: user_id, username, first_name, last_name, phone, is_premium, is_verified, is_bot, collected_at, source_group, etc.
- **Indexes**: Optimized for search by user_id, username, collected_at, source_group
- **Connection Pool**: Maintains pool of 5 connections with WAL mode for performance
- **Key Methods**:
  - `init_database()` - Creates schema and indexes
  - `insert_users(users_data, batch_size=1000)` - Batch inserts with IGNORE on duplicates
  - `search_users(search_term, limit=100)` - Searches by ID/username/name
  - `get_all_users(limit=None)` - Returns all users as pandas DataFrame
  - `backup_database()` - Creates timestamped backup in `bot/data/backups/`

### Telegram Parsing Flow
- **Parser**: `bot/utils/telegram_parser.py` - Core parsing logic using telethon
- **Process**:
  1. `get_users_from_chats_enhanced(account, date_target)` - Main entry point
  2. Connects via TelegramClient with session files (`session_*.session`)
  3. `find_date_boundaries(client, chat_id, target_date)` - Finds message ID range for date
  4. `process_dialog_enhanced()` - Processes each group with semaphore (max 3 concurrent)
  5. Collects unique senders from messages matching target date
  6. Deduplicates against existing database via `DatabaseManager.get_existing_user_ids()`
  7. Batch inserts new users, creates daily reply files in `bot/data/reply/`

### Handler System (aiogram 3.x)
All handlers register to shared dispatcher `dp` from `aiogram_loader.py`:

- `handlers/start.py` - `/start` command, main menu keyboard
- `handlers/parser.py` - Data collection flow with FSM states
- `handlers/stats.py` - Database statistics and analytics
- `handlers/export.py` - Multi-format exports (Excel, CSV, JSON, etc.)
- `handlers/search.py` - User search functionality
- `handlers/settings.py` - Bot configuration
- `handlers/file_manager.py` - File operations (list, download, delete exports)
- `handlers/missed_days.py` - Handles parsing gaps in collection history

### State Management (FSM)
- `bot/states/form_states.py` - Defines `Form` StatesGroup with states like:
  - `waiting_for_date` - Custom date input
  - `waiting_for_user_ids` - User ID search
  - `waiting_for_date_range` - Date range selection
- Uses aiogram's `MemoryStorage` (defined in `aiogram_loader.py`)

### Keyboards and UI
- `bot/keyboards/main_menu.py` - Primary menu with collection/stats/export options
- `bot/keyboards/date_selection.py` - Date picker (today, yesterday, custom, last 7 days)
- `bot/keyboards/export_menu.py` - Export format selection
- `bot/keyboards/settings_menu.py` - Settings options

### Utilities
- `bot/utils/analytics.py` - Data analytics and statistics
- `bot/utils/export_manager.py` - Handles Excel/CSV/JSON/ZIP exports
- `bot/utils/advanced_export.py` - Extended export formats (Markdown, HTML, etc.)
- `bot/utils/file_utils.py` - Directory management, ensures required folders exist
- `bot/utils/logging_utils.py` - Enhanced logging with banners and sections
- `bot/utils/metrics.py` - Performance metrics tracking
- `bot/utils/error_handler.py` - Retry logic with exponential backoff
- `bot/utils/validators.py` - Input validation and SQL injection prevention

### Data Flow for Collection
1. User clicks "🚀 Запустить сбор данных" → `handlers/parser.py`
2. Date selection keyboard shown → user picks date
3. `start_processing_enhanced(message, date_target)` initiated
4. For each account in `parser_cfg.accounts`:
   - `get_users_from_chats_enhanced(account, date_target)` called
   - Creates backup via `DatabaseManager.backup_database()`
   - Parses groups, collects users into set of tuples
   - Filters against existing DB users
   - Batch inserts new users via `DatabaseManager.insert_users()`
   - Creates reply file in `bot/data/reply/reply_{phone}_{date}.xlsx`
5. Exports updated database, sends files to user

### Global State Management
`bot/aiogram_loader.py` maintains global dictionaries:
- `active_tasks = {}` - Tracks running collection tasks by user_id
- `user_settings = {}` - Per-user configuration
- `pending_missed_days = []` - Queue of dates needing collection

## Directory Structure
```
bot/data/
  ├── parser_cfg.py          # Config (git-ignored)
  ├── all_users.db           # SQLite database
  ├── backups/               # Database backups
  ├── exports/               # User-requested exports
  ├── logs/                  # Application logs
  ├── reply/                 # Daily parsing results
  └── temp/                  # Temporary files
```

## Key Technical Details

### Username Handling
All usernames are prefixed with `@` during collection and search:
- `telegram_parser.py:43-45` - Adds `@` prefix during collection
- `telegram_parser.py:141-143` - Adds `@` during message processing
- `database.py:519-548` - Migration to add `@` to existing usernames

### Column Mapping
Database uses lowercase snake_case internally (`user_id`, `first_name`), but exports use Russian display names:
- `database.py:360-376` - Column mapping for search results
- `database.py:407-423` - Column mapping for get_all_users
- Exported columns: "User_id", "Username", "Имя", "Фамилия", etc.

### FloodWait Handling
Telethon automatically handles `FloodWaitError`:
- `telegram_parser.py:94-97` - Catches FloodWait, sleeps, retries
- Semaphore limits concurrent group processing to 3 (`telegram_parser.py:229`)

### Session Management
- Telethon sessions stored as `session_{phone_number}.session` files
- Sessions persist authentication, avoiding re-verification
- On `SessionPasswordNeededError`, bot notifies user to disable 2FA or provide password

### Database Performance Optimizations
- WAL mode: `PRAGMA journal_mode=WAL` (`database.py:49`)
- Connection pooling with max 5 connections (`database.py:18`)
- Batch inserts with 1000 records per batch (`database.py:261`)
- Indexes on user_id, username, collected_at, source_group (`database.py:114-119`)

## Common Tasks

### Testing Database Operations
```python
from bot.utils.database import DatabaseManager

# Get stats
stats = DatabaseManager.get_database_stats()

# Search users
results = DatabaseManager.search_users("@username")

# Export to Excel
file_path = DatabaseManager.export_to_excel()
```

### Adding a New Handler
1. Create file in `bot/handlers/`
2. Import `dp` from `bot.aiogram_loader`
3. Register handlers with `@dp.message()` or `@dp.callback_query()` decorators
4. Import handler in `bot/main.py` (line 22-25) to register routes

### Adding Export Format
1. Extend `bot/utils/export_manager.py` or `bot/utils/advanced_export.py`
2. Add format option to `bot/keyboards/export_menu.py`
3. Handle callback in `bot/handlers/export.py`

## Dependencies
- **aiogram 3.4.1** - Async Telegram Bot API framework
- **telethon 1.34.0** - Telegram MTProto client for parsing
- **pandas 2.1.4** - Data manipulation for exports
- **openpyxl 3.1.2** - Excel file operations
- **SQLite** - Built into Python stdlib, no extra dependency

## Important Notes
- Bot requires both aiogram (for bot interactions) and telethon (for group parsing)
- Multiple accounts supported for parallel parsing across different rate limits
- All parsing operations create automatic database backups before modifications
- The bot filters groups to only process those with >10 participants (`telegram_parser.py:216`)
- Windows console encoding configured in `run.py` for Cyrillic character support
