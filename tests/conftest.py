"""Thiết lập đường dẫn import cho kiểm thử Backup Node."""

import sys
from pathlib import Path

import pytest

BACKUP_DIR = Path(__file__).resolve().parents[1] / "backup_node"
if str(BACKUP_DIR) not in sys.path:
    sys.path.insert(0, str(BACKUP_DIR))


def pytest_configure(config):
    """Đăng ký marker cho các kiểm thử cần khởi chạy cụm thật."""
    config.addinivalue_line(
        "markers", "integration: kiểm thử tích hợp với tiến trình node thật"
    )
