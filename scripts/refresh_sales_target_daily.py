"""Build today's Sales Target from verified sources; retain the prior generation."""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid


def refresh(root, runtime, day, *, runner=subprocess.run, verifier=None):
    datetime.datetime.strptime(day, '%Y%m%d')
    root, runtime = Path(root), Path(runtime)
    snapshot = runtime / 'output' / f'booking_snapshot_result_{day}.csv'
    for path in (snapshot, runtime / 'output' / f'_cache_{day}.parquet',
                 runtime / 'output' / f'BSA_raw_monthly3W_{day}.csv'):
        if not path.is_file():
            raise FileNotFoundError(f'Dated Sales Target source required: {path}')
    site = runtime / 'dist' / 'sales-target'
    stage = site.with_name('.sales-target-stage-' + uuid.uuid4().hex)
    stage.mkdir(parents=True)
    env = dict(os.environ, DASHBOARD_RUNTIME_ROOT=str(runtime), SKIP_SALES_TARGET_BUILD='0')
    command = [sys.executable, '-u', str(root / 'scripts/build_sales_target_drill_data.py'),
               '--monthly-target-from-existing-sources', '--as-of', day,
               '--snapshot', str(snapshot), '--out', str(stage)]
    result = runner(command, cwd=str(root), env=env)
    if result.returncode:
        raise subprocess.CalledProcessError(result.returncode, command)
    if verifier is None:
        from sales_target_release import verify_company_release
        verifier = verify_company_release
    proof = verifier(stage, day)
    # Only validated output replaces the active staging directory. This does
    # not change the public pointer; Drive sync verifies every dependency first.
    previous = site.with_name('sales-target.previous-' + uuid.uuid4().hex)
    had_previous = site.exists()
    if had_previous:
        site.rename(previous)
    try:
        stage.rename(site)
    except BaseException:
        if had_previous:
            previous.rename(site)
        raise
    value = {'dataset': day, 'verification': 'passed', 'release': proof,
             'site': str(site), 'previous': str(previous) if had_previous else None}
    print('[SalesTargetDaily] ' + json.dumps(value), flush=True)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    runtime = Path(os.environ['DASHBOARD_RUNTIME_ROOT'])
    from dashboard_write_lock import PublishLock
    with PublishLock(r'Global\KMTC_3W_SalesTarget_Build_v1', 30):
        refresh(root, runtime, args.dataset)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
