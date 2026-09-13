"""Load the desktop compatibility API by path, independent of core sys.path order."""
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "_v88_desktop_subscription_adapter", Path(__file__).with_name("gpt_subscription.py"))
_adapter = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_adapter)
MODEL = _adapter.MODEL
configured = _adapter.configured
api_key = _adapter.api_key
model_name = _adapter.model_name
message_text = _adapter.message_text
chat_completion = _adapter.chat_completion
complete = _adapter.complete
