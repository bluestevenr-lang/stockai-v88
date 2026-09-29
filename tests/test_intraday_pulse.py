from datetime import datetime
import importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location('pulse',Path(__file__).parents[1]/'scripts/intraday_pulse.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)


def test_tick_boundaries_and_next_day():
    for source,expected in [('2026-09-29T11:06:59+08:00','2026-09-29T11:07:00+08:00'),
      ('2026-09-29T11:07:00+08:00','2026-09-29T11:37:00+08:00'),
      ('2026-09-29T23:58:00+08:00','2026-09-30T00:07:00+08:00')]:
        assert p.next_tick(datetime.fromisoformat(source)).isoformat()==expected
