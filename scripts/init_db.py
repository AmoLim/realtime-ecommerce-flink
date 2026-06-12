import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.database import DB_PATH, init_db  # noqa: E402


if __name__ == "__main__":
    init_db()
    print(f"SQLite database initialized: {DB_PATH}")
