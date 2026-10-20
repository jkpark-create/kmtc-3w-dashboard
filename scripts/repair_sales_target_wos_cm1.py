"""Add missing booking-cohort CM1 to a downloaded, immutable Sales Target release.

Never changes the source date, goals, roster, physical B/L actuals, or old files.
The caller must upload and verify every new file before publishing the pointer.
"""
import argparse
import hashlib
import json
from pathlib import Path


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


def repair_booking(row, source):
    if row.get('performance_metric_kind') != 'booking' or not row.get('is_w3'):
        return False
    key = row.get('bkg_no')
    if key not in source:
        raise ValueError(f'Booking absent from same-date source: {key}')
    evidence = source[key]
    if isinstance(evidence, tuple):
        evidence = dict(zip(('BKG_SHPR_CST_NO', 'POL_PORT_CD', 'POD_PORT_CD',
                             'WOS_LST_Status', 'WOS_LST_TEU', 'WOS_CM1'), evidence))
    for target, original in [('shipper_no', 'BKG_SHPR_CST_NO'), ('pol', 'POL_PORT_CD'), ('pod', 'POD_PORT_CD')]:
        if str(row.get(target) or '') != str(evidence.get(original) or ''):
            raise ValueError(f'Booking lineage differs: {key} / {target}')
    normal = str(evidence.get('WOS_LST_Status') or '').strip().lower() == 'normal'
    def number(value):
        return float(str(value or '0').replace(',', ''))
    teu = number(evidence.get('WOS_LST_TEU')) if normal else 0.
    if abs(teu - float(row.get('wos_norm_lst_teu') or 0)) > 1e-8:
        raise ValueError(f'Booking WOS lifting differs: {key}')
    row['wos_norm_cm1'] = number(evidence.get('WOS_CM1')) if normal else 0.
    return True


def repair(site, snapshot):
    import pandas as pd
    site = Path(site)
    read = lambda name: json.loads((site / name).read_text(encoding='utf-8'))
    pointer = read('sales-target-release.json')
    if pointer['data_date'] not in Path(snapshot).stem:
        raise ValueError('Recovery snapshot must have the published source date')
    cols = ['BKG_NO', 'BKG_SHPR_CST_NO', 'POL_PORT_CD', 'POD_PORT_CD',
            'WOS_LST_Status', 'WOS_LST_TEU', 'WOS_CM1']
    frame = pd.read_parquet(snapshot, columns=cols).fillna('')
    frame = frame.loc[frame.BKG_NO.ne('')]
    if frame.BKG_NO.duplicated().any():
        raise ValueError('Ambiguous booking source keys')
    source = {r[0]: r[1:] for r in frame.itertuples(index=False, name=None)}
    del frame
    print('Same-date booking source indexed', len(source), flush=True)
    docs = {key: read(key + '.json') for key in ('index', 'manifest', 'base2025')}
    if any(d['data_date'] != pointer['data_date'] or d['release_id'] != pointer['release_id'] for d in docs.values()):
        raise ValueError('Downloaded release metadata differs')
    changed = []; count = 0
    descriptors = docs['manifest']['chunks'] + docs['manifest'].get('capacity_reserve_chunks', [])
    for position, desc in enumerate(descriptors):
        if desc['yyyymm'] < '202607':
            continue
        chunk = read(desc['file'])
        rows = chunk.get('bookings', [])
        repaired = sum(repair_booking(row, source) for row in rows)
        if repaired:
            payload = encode(chunk)
            name = 'data/woscm1-' + hashlib.sha256(payload).hexdigest()[:20] + '-' + Path(desc['file']).name
            (site / name).write_bytes(payload)
            changed.append({'old': desc['file'], 'new': name, 'rows': repaired})
            desc['file'] = name
            count += repaired
        if position % 100 == 0:
            print('Validated chunks', position, 'repaired bookings', count, flush=True)
    digest = hashlib.sha256(encode(docs)).hexdigest()[:16]
    release_id = '20261021-woscm1-' + digest
    files = {}; checksums = {}
    for key, doc in docs.items():
        doc['release_id'] = release_id
        name = key + '.v' + release_id + '.json'
        payload = encode(doc); (site / name).write_bytes(payload)
        files[key] = name; checksums[name] = hashlib.md5(payload).hexdigest()
    for desc in descriptors:
        name = desc['file']; checksums[name] = hashlib.md5((site / name).read_bytes()).hexdigest()
    result = dict(_format=pointer['_format'], release_id=release_id, data_date=pointer['data_date'], files=files, checksums=checksums)
    (site / 'repaired-release.json').write_bytes(encode(result))
    audit = dict(source_date=pointer['data_date'], snapshot=str(snapshot), prior_release=pointer['release_id'],
                 release_id=release_id, booking_rows=count, changed_chunks=changed)
    (site / 'repair-audit.json').write_bytes(encode(audit))
    print(json.dumps(dict(release_id=release_id, booking_rows=count, changed_chunks=len(changed))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--site', required=True); parser.add_argument('--snapshot', required=True)
    args = parser.parse_args(); repair(args.site, args.snapshot)
