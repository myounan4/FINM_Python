
from pathlib import Path
from finm325_assn6.main import run
def test_run_smoke():
    out=run(str(Path(__file__).resolve().parents[1]))
    assert 'orders' in out
