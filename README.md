# GroupMe Backup & Analytics Tool

A comprehensive CLI tool to backup GroupMe group chats to PostgreSQL with powerful analytics capabilities.

## Features

- **Incremental Backups**: Efficiently syncs only new messages since last backup
- **Complete Metadata**: Preserves all message data including likes, attachments, mentions, and more
- **Powerful Analytics**: Query for popular messages, user activity, conversation patterns, and more
- **Rate Limiting**: Built-in API rate limiting to respect GroupMe's limits
- **Rich CLI**: Beautiful terminal output with progress bars and formatted tables

## Requirements

- Python 3.10 or higher
- PostgreSQL 12 or higher
- GroupMe account with API access token

## Installation

### 1. Clone or download this repository

```bash
cd ~/groupme-backup
```

### 2. Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Install the package

```bash
pip install -e .
```

## Configuration

### 1. Get your GroupMe API Access Token

1. Go to https://dev.groupme.com
2. Log in with your GroupMe account
3. Click on "Access Token" in the top right
4. Copy your access token

### 2. Set up PostgreSQL

Create a PostgreSQL database:

```bash
createdb groupme_backup
```

Or use an existing database with custom credentials.

### 3. Configure environment variables

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` and add your credentials:

```env
# GroupMe API Configuration
GROUPME_ACCESS_TOKEN=your_access_token_here

# PostgreSQL Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=groupme_backup
DB_USER=postgres
DB_PASSWORD=your_password_here
```

### 4. Initialize the database

Run Alembic migrations to create tables:

```bash
alembic upgrade head
```

## Usage

### Backup Commands

#### List all available groups

```bash
groupme-backup list-groups
```

#### Backup a specific group

```bash
groupme-backup backup --group-id YOUR_GROUP_ID
```

#### Backup all groups

```bash
groupme-backup backup --all
```

### Analytics Commands

All analytics commands take a GROUP_ID as an argument. Use `list-groups` to find group IDs.

#### Most popular messages (by likes)

```bash
# Show top 10 most liked messages from last 7 days
groupme-backup popular GROUP_ID --days 7 --limit 10
```

#### Longest consecutive message streak

Find who sent the most messages in a row:

```bash
groupme-backup consecutive GROUP_ID
```

#### Most active users

Show users with the most messages:

```bash
# Top 10 users in last 30 days
groupme-backup active GROUP_ID --days 30 --limit 10
```

#### Most liked users

Show users whose messages receive the most likes:

```bash
groupme-backup liked GROUP_ID --days 30 --limit 10
```

#### Group statistics

View general statistics about a group:

```bash
groupme-backup stats GROUP_ID
```

Shows:
- Total messages
- Total users
- Total likes
- Average messages per day
- Date range
- Last sync time

#### Conversation pace analysis

Analyze the time between messages:

```bash
groupme-backup response-time GROUP_ID
```

### Other Commands

#### Show version

```bash
groupme-backup version
```

#### Enable verbose logging

Add the `-v` or `--verbose` flag to any command:

```bash
groupme-backup -v backup --group-id GROUP_ID
```

## How Incremental Sync Works

The tool uses GroupMe's `since_id` parameter to efficiently fetch only new messages:

1. **First Backup**: Fetches all messages from the group history using pagination
2. **Subsequent Backups**: Only fetches messages created after the last synced message
3. **Tracking**: Stores `last_synced_message_id` in the database for each group
4. **Efficiency**: Dramatically reduces API calls and sync time for large groups

## Database Schema

The tool creates the following tables:

- **groups**: Group metadata with sync tracking
- **users**: User profiles (denormalized)
- **messages**: All message data with sender snapshots
- **message_favorites**: Like/favorite relationships
- **attachments**: Images, locations, emojis, etc. with full JSON preservation
- **mentions**: @mention tracking
- **sync_logs**: Backup operation history

All metadata is preserved for future analytics capabilities.

## Advanced Usage

### Custom Analytics Queries

You can write custom SQL queries against the PostgreSQL database:

```bash
psql groupme_backup -c "
  SELECT name, COUNT(*) as message_count
  FROM messages
  WHERE group_id = 'YOUR_GROUP_ID'
  GROUP BY name
  ORDER BY message_count DESC
  LIMIT 10;
"
```

### Automated Backups

Set up a cron job for regular backups:

```bash
# Edit crontab
crontab -e

# Add a line to run backup daily at 2 AM
0 2 * * * cd /path/to/groupme-backup && source venv/bin/activate && groupme-backup backup --all
```

### Exporting Data

Export messages to CSV:

```bash
psql groupme_backup -c "
  COPY (
    SELECT created_at, name, text,
           (SELECT COUNT(*) FROM message_favorites WHERE message_id = messages.id) as likes
    FROM messages
    WHERE group_id = 'YOUR_GROUP_ID'
    ORDER BY created_at
  ) TO '/tmp/messages.csv' WITH CSV HEADER;
"
```

## Troubleshooting

### "Access token invalid" error

- Regenerate your access token at https://dev.groupme.com
- Update the token in your `.env` file

### "Database connection failed" error

- Ensure PostgreSQL is running: `pg_ctl status` or `brew services list`
- Verify database credentials in `.env`
- Check that the database exists: `psql -l | grep groupme_backup`

### "Rate limit exceeded" error

The tool has built-in rate limiting, but if you still see this:
- Reduce `GROUPME_RATE_LIMIT_CALLS` in `.env` (try 50 instead of 100)
- The tool will automatically retry with exponential backoff

### Messages not syncing

- Check the last sync time: `groupme-backup stats GROUP_ID`
- Verify there are new messages in the GroupMe app
- Try running with verbose logging: `groupme-backup -v backup --group-id GROUP_ID`

### Database migrations failed

Reset and recreate the database:

```bash
dropdb groupme_backup
createdb groupme_backup
alembic upgrade head
```

## Development

### Running tests

```bash
pytest
```

### Code formatting

```bash
black groupme_backup/
ruff check groupme_backup/
```

### Type checking

```bash
mypy groupme_backup/
```

### Creating a new migration

After modifying models:

```bash
alembic revision --autogenerate -m "Description of changes"
alembic upgrade head
```

## Architecture

```
groupme_backup/
├── api/          # GroupMe API client with rate limiting
├── db/           # SQLAlchemy models and session management
├── sync/         # Incremental and full backup logic
├── analytics/    # Analytics query functions
├── cli/          # Click CLI commands
├── config/       # Pydantic settings
└── utils/        # Logging and utilities
```

## Limitations

- GroupMe API returns a maximum of 100 messages per request
- The tool respects API rate limits (default: 100 calls per 60 seconds)
- Historical data before first backup cannot be recovered if deleted from GroupMe

## Future Enhancements

Potential features for future versions:

- Web interface for analytics visualization
- Real-time sync with webhooks
- Export to other formats (JSON, Excel)
- Sentiment analysis on messages
- Message search functionality
- Group comparison analytics
- Backup restoration capability

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:

- Open an issue on GitHub
- Check the troubleshooting section above
- Review GroupMe API documentation: https://dev.groupme.com/docs/v3

## Acknowledgments

- Built with [SQLAlchemy](https://www.sqlalchemy.org/), [Click](https://click.palletsprojects.com/), and [Rich](https://rich.readthedocs.io/)
- Powered by the [GroupMe API](https://dev.groupme.com/)
