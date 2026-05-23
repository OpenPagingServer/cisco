
FORM_TYPE = "enterprise"

def module_web():
    import importlib.util
    from pathlib import Path
    module_dir = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("cisco_web", module_dir / "web.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def forms():
    return module_web().forms()

def handle_request(request, conn_factory, page, user):
    if FORM_TYPE in {"forms", "index"}:
        return forms()
    return module_web().render_form(FORM_TYPE, request, conn_factory, page, user)

