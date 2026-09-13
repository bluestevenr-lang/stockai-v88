"""Resolve the installed desktop/core pair without assuming a user's Desktop."""
import os
from pathlib import Path


def core_root():
    configured = os.environ.get('V88_CORE_ROOT')
    if configured:
        return Path(configured).expanduser().resolve()
    here = Path(__file__).resolve().parent
    return here.parent if here.name == 'src' else here.parent / 'ai-daily-report-v2'


CORE = core_root()
DATA = CORE / 'data'
