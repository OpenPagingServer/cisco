import importlib.util
from pathlib import Path


def module_web():
    module_dir = Path(__file__).resolve().parent
    spec = importlib.util.spec_from_file_location("cisco_web", module_dir / "web.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def handle_request(endpoint_id, request, conn_factory, page, user):
    return module_web().render_action("delete", endpoint_id, request, conn_factory, page, user)
