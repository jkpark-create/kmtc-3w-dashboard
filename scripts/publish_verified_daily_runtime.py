"""Publish one dated 3W/OBT/Sales Target generation; retain owner audit history."""
import argparse
from pathlib import Path
import os


def publish(runtime, day, sync, release):
    runtime = Path(runtime)
    site = runtime / 'dist/sales-target'
    release.verify_company_release(site, day)
    groups = sync.runtime_groups(skip_sales_target=True)
    if not any(path.name == f'dashboard_summary_{day}.json' for path, _ in groups['main_history']):
        raise ValueError('Main dashboard and Sales Target dates differ')
    groups.update(release.release_entries(site))
    groups['sales'].append((site / 'office_affiliation.json', 'office_affiliation.json'))
    # Owner maintenance is separate from performance generation. Publish the
    # daily source-quality checks, retaining the previous dated owner audits.
    for name in (f'customer_code_quality_{day}.json', f'customer_code_errors_{day}.csv'):
        path = runtime / 'output' / name
        if not path.is_file():
            raise FileNotFoundError(path)
        groups['main_source'].append((path, name))
    token = sync.refresh_access_token()
    sync.ensure_company_reader(token)
    synced = {}
    for group in ('main', 'main_history', 'main_source', 'obt', 'sales_data', 'sales'):
        synced[group] = sync.sync_group(token, group_name=group, entries=groups[group], workers=4)
    sync.verify_remote_groups(token, synced)
    pointer = release.publish_release_pointer(token, sync.DRIVE_FOLDERS['sales'], site,
                sync.list_files, sync.sync_one, sync.request, sync.DRIVE_API)
    synced['sales'].append(pointer)
    count = sync.verify_remote_groups(token, synced)
    for group, entries in synced.items():
        sync.merge_state_group(group, entries)
    print(f'[DailyPublish] {day}: all runtime groups verified, including Sales Target; files={count}', flush=True)
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    args = parser.parse_args()
    import sync_dashboard_data_to_gdrive as sync
    import sales_target_release as release
    from dashboard_write_lock import PublishLock
    with PublishLock():
        publish(Path(os.environ['DASHBOARD_RUNTIME_ROOT']), args.dataset, sync, release)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
