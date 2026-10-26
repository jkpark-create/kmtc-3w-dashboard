"""Attach reconciled own-B/L shipper facts for the existing dashboard tables."""
import datetime
import gzip
import json
from collections import defaultdict

from component_json_stream import Stream
from independent_component_bsa import digest

COLUMNS = ['team', 'origin', 'ori_port', 'dest', 'dst_port', 'YYYYMM',
           'week_start_date', 'BKG_SHPR_CST_NO', 'BKG_SHPR_CST_ENM', 'Salesman_POR',
           'norm_bl', 'norm_lst', 'cm1_norm', 'lst_norm']


def attach(source, value):
    from independent_component_bl import validate_bl
    import math
    manifest_path = source / 'manifest.json'
    if digest(manifest_path) != value['manifestSha256']:
        raise ValueError('BL manifest changed before detail read')
    details = {}
    with manifest_path.open(encoding='utf-8-sig') as reader:
        stream = Stream(reader)
        for key in stream.members():
            if key == 'detailFiles':
                details = stream.value()
            else:
                stream.skip()
        stream.finish()
    if not details:
        return  # Legacy aggregate-only contracts remain readable.
    acc = defaultdict(lambda: [0., 0., 0., 0.])
    lane = defaultdict(lambda: [0., 0., 0., 0.])
    seen = set()
    proofs = {}
    for month in value['scopeMonths']:
        path = (source / details[month]).resolve()
        if not path.is_relative_to(source.resolve()) or path.is_symlink():
            raise ValueError('Unsafe BL detail path')
        before = digest(path)
        observed = None
        with gzip.open(path, 'rt', encoding='utf-8') as reader:
            stream = Stream(reader)
            for key in stream.members():
                if key == 'sourceMeta':
                    observed = stream.value()
                elif key == 'bookingDetailRows':
                    for row in stream.items():
                        identifier = str(row.get('blNo') or '').strip()
                        if identifier.lower() in ('', '-', 'nan', 'none', 'null'):
                            continue
                        if identifier in seen:
                            raise ValueError('Duplicate BL identifier in own detail')
                        seen.add(identifier)
                        if row.get('sourceType') != 'icc_identifier_direct':
                            raise ValueError('Unverified BL detail source')
                        quantities = [row.get(k) for k in ('blTeu', 'blCm1', 'blFreight', 'blCm1BasisTeu')]
                        if any(not isinstance(v, (float, int)) or not math.isfinite(v) for v in quantities):
                            raise ValueError('Unknown BL detail quantity')
                        teu, cm1, freight, basis = quantities
                        if teu < 0 or basis < 0:
                            raise ValueError('Negative BL quantity')
                        week = str(row.get('week') or '')
                        if row.get('month') != month or not week.startswith(month) or not week[6:].isdigit():
                            raise ValueError('Invalid BL detail period')
                        ww = int(week[6:])
                        if not 1 <= ww <= 53 or month[:4] != '2026':
                            raise ValueError('Unsupported fiscal week')
                        start = datetime.date(2026, 1, 4) + datetime.timedelta(weeks=ww - 1)
                        week_text = f'{start.year}년 {start.month:02d}월 {start.day:02d}일'
                        team, origin, por, dest, dly = [str(row.get(k) or '').strip() for k in ('team', 'origin', 'por', 'dest', 'dly')]
                        if team not in ('OBT', 'EST', 'IST', 'JBT') or not all((origin, por, dest, dly)):
                            raise ValueError('Missing BL detail dimensions')
                        dims = (team, origin, por, dest, dly, month, week_text,
                                str(row.get('shipperCode') or '').strip(),
                                str(row.get('shipperName') or row.get('bookingShipperName') or '').strip(),
                                str(row.get('salesman') or '').strip())
                        metrics = (1., teu, cm1, basis)
                        acc[dims] = [a + b for a, b in zip(acc[dims], metrics)]
                        lane_key = (dly, por, str(ww), month, dest, origin, team)
                        lane[lane_key] = [a + b for a, b in zip(lane[lane_key], quantities)]
                else:
                    stream.skip()
            stream.finish()
        if not observed or observed.get('generatedAt') != value['generatedAt'] or validate_bl(observed, []) != value['sourceDate']:
            raise ValueError('Mixed BL detail generation')
        if digest(path) != before:
            raise ValueError('BL detail changed during read')
        proofs[details[month]] = {'sha256': before, 'bytes': path.stat().st_size}
        print(f'Verified BL shipper detail {month}: {len(seen)} identifiers', flush=True)
    expected = {tuple(row[:7]): row[7:] for row in value['table']['r']}
    for key in set(expected) | set(lane):
        if any(abs(a - b) > .01 for a, b in zip(expected.get(key, [0.] * 4), lane.get(key, [0.] * 4))):
            raise ValueError(f'BL detail does not reconcile to verified lane: {key}')
    if digest(manifest_path) != value['manifestSha256'] or any(digest(source / k) != p['sha256'] for k, p in proofs.items()):
        raise ValueError('BL generation changed after detail reconciliation')
    dictionaries = {name: [] for name in COLUMNS[:10]}
    indices = {name: {} for name in dictionaries}
    packed = []
    for dimensions, metrics in sorted(acc.items()):
        row = []
        for name, text in zip(COLUMNS, dimensions):
            lookup = indices[name]
            if text not in lookup:
                lookup[text] = len(dictionaries[name])
                dictionaries[name].append(text)
            row.append(lookup[text])
        packed.append(row + metrics)
    value['dashboardDetail'] = {'version': 1, 'identifierCount': len(seen),
                                'table': {'c': COLUMNS, 'r': packed, 'd': dictionaries}}
    value['sourceFiles'].update(proofs)
    print(f'BL dashboard detail: {len(acc)} rows, {len(json.dumps(value, ensure_ascii=False).encode())} bytes', flush=True)
