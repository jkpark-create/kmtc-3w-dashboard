"""Preserve observed owner transitions without inventing effective dates."""
from datetime import datetime, timedelta

HISTORY_FIELDS = ('OWNER_OBSERVED_DATE', 'OWNER_EFFECTIVE_DATE', 'OWNER_EVIDENCE_DATE',
                  'OWNER_DATE_BASIS', 'OWNER_EFFECTIVE_END_DATE')


def transition_rows(previous, replacement, observed_date):
    observed = datetime.strptime(str(observed_date), '%Y%m%d')
    effective = str(replacement.get('OWNER_EFFECTIVE_DATE') or '').strip()
    boundary = datetime.strptime(effective, '%Y%m%d') if effective else observed
    if boundary > observed:
        raise ValueError('Future effective date cannot replace an active owner')
    incoming = dict(replacement)
    incoming['OWNER_OBSERVED_DATE'] = str(observed_date)
    incoming['OWNER_EFFECTIVE_DATE'] = effective
    incoming['OWNER_DATE_BASIS'] = 'confirmed_effective' if effective else 'observed_only_effective_unknown'
    # SALES_START_DATE remains an operational active-row boundary. Its meaning
    # is explicitly recorded; unknown effective dates must not enter targets.
    incoming['SALES_START_DATE'] = boundary.strftime('%Y%m%d')
    incoming['SALES_END_DATE'] = incoming.get('SALES_END_DATE') or '99991231'
    archived = []
    for old in previous:
        start = datetime.strptime(str(old['SALES_START_DATE']).removesuffix('.0'), '%Y%m%d')
        if start >= boundary:
            raise ValueError('Same-day/retroactive owner overlap requires reviewed history')
        row = dict(old)
        row['SALES_END_DATE'] = (boundary-timedelta(days=1)).strftime('%Y%m%d')
        row['OWNER_OBSERVED_DATE'] = str(observed_date)
        row['OWNER_EFFECTIVE_END_DATE'] = row['SALES_END_DATE'] if effective else ''
        if not effective:
            row['OWNER_DATE_BASIS'] = 'observed_only_effective_unknown'
        archived.append(row)
    event = {'observed_date': str(observed_date), 'effective_date': effective or None,
             'evidence_date': replacement.get('OWNER_EVIDENCE_DATE') or None,
             'effective_date_status': 'confirmed' if effective else 'unknown',
             'previous_rows': [dict(row) for row in previous], 'new_row': dict(incoming)}
    return archived, incoming, event
