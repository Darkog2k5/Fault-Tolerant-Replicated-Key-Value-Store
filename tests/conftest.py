"""Thiết lập đường dẫn import cho kiểm thử Backup Node, Primary Node và Client Proxy.

`backup_node/`, `primary_node/` và `client_proxy/` là thư mục script phẳng có
module trùng tên (`config`, `main`). Chạy chung một phiên pytest thì module nạp
trước sẽ nằm trong `sys.modules` và file test sau import nhầm. Các hook bên dưới
đổi bộ module đang hoạt động theo file test trước khi thu thập và trước khi chạy
từng ca.

File test proxy cần cả `main` của Primary lẫn `proxy` (mỗi bên một `config`) nên
tự sắp `sys.path` khi import; conftest chỉ tách bộ module đã nạp của nó khỏi hai
node còn lại và không đổi `sys.path`.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
COMPONENT_DIRS = {
    "backup": ROOT / "backup_node",
    "primary": ROOT / "primary_node",
    "proxy": None,  # file test tự sắp sys.path, xem docstring
}
# Tên module có thể bị trùng giữa các thư mục node.
SHARED_MODULE_NAMES = ("config", "main", "node", "replication", "proxy")

_stashed: dict[str, dict[str, object]] = {name: {} for name in COMPONENT_DIRS}
_active = "backup"

if str(COMPONENT_DIRS["backup"]) not in sys.path:
    sys.path.insert(0, str(COMPONENT_DIRS["backup"]))


def _component_for(path: Path) -> str:
    if path.name.startswith("test_primary"):
        return "primary"
    if path.name.startswith("test_proxy"):
        return "proxy"
    return "backup"


def _activate(component: str) -> None:
    """Cất module của node đang dùng và khôi phục module của node cần chạy."""
    global _active
    if component != _active:
        for name in SHARED_MODULE_NAMES:
            module = sys.modules.pop(name, None)
            if module is not None:
                _stashed[_active][name] = module
        sys.modules.update(_stashed[component])
        _stashed[component].clear()
        _active = component
    if COMPONENT_DIRS[component] is None:
        return
    directory = str(COMPONENT_DIRS[component])
    while directory in sys.path:
        sys.path.remove(directory)
    sys.path.insert(0, directory)


@pytest.hookimpl(hookwrapper=True)
def pytest_make_collect_report(collector):
    if isinstance(collector, pytest.Module):
        _activate(_component_for(collector.path))
    yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    _activate(_component_for(item.path))
    yield


def pytest_configure(config):
    """Đăng ký marker cho các kiểm thử cần khởi chạy cụm thật."""
    config.addinivalue_line(
        "markers", "integration: kiểm thử tích hợp với tiến trình node thật"
    )
