"""Distinguish verified new customers from missing historical owner evidence."""
from __future__ import annotations
import re,sys
from pathlib import Path


def evidence_day(value):
    text=str(value or '').strip()
    parts=re.search(r'(20\d{2})\D+(\d{1,2})\D+(\d{1,2})',text)
    if parts:text=f'{parts.group(1)}{int(parts.group(2)):02d}{int(parts.group(3)):02d}'
    digits=''.join(re.findall(r'\d',text))
    if len(digits)<8:return None
    from datetime import datetime
    try:datetime.strptime(digits[:8],'%Y%m%d')
    except ValueError:return None
    return digits[:8]


def classify_customer(customer,baseline_customers,registration=None,first_observed_trade=None):
    if customer in baseline_customers:
        return {'class':'existing','reason':'present_in_2025_full_baseline'}
    registration=registration or {}
    created=evidence_day(registration.get('registered_date'))
    member=evidence_day(registration.get('older_business_date'))
    trade=evidence_day(first_observed_trade)
    if registration.get('verification')=='confirmed' and created and created<='20251231':
        return {'class':'existing','reason':'customer_master_registered_before_2026'}
    if registration.get('verification')=='confirmed' and created and created>'20251231':
        if member and member<='20251231':
            return {'class':'existing','reason':'older_customer_master_business_date'}
        if trade and created>trade:
            return {'class':'history_unconfirmed','reason':'registration_after_observed_trade'}
        return {'class':'confirmed_new','reason':'customer_master_registered_after_2025_and_absent_from_full_2025_baseline',
                'registered_date':created,'source':registration.get('source')}
    return {'class':'history_unconfirmed','reason':'baseline_absence_alone_does_not_confirm_new_customer'}


def fetch_registration_evidence(customers,batch_size=200):
    """Read only requested customers from the existing approved DB connection.

    The customer master first-entry datetime is registration evidence, never
    a salesman effective date. Stop on the first bounded-query failure.
    """
    project=Path(r'C:\Users\JKPARK\OneDrive\Documents\Claude\Integrated dashboard project')
    original=list(sys.path)
    try:
        sys.path[:0]=[str(project),str(project/'scripts')]
        from scripts import build_oracle_dashboard_data as dashboard
    finally:sys.path[:]=original
    output={};requested=sorted(set(customers));audit={'requested':len(requested),'read':0,'unavailable':None}
    with dashboard.connect(attempts=1) as connection:
        connection.call_timeout=5000
        cursor=connection.cursor()
        for offset in range(0,len(requested),batch_size):
            batch=requested[offset:offset+batch_size]
            binds={f'c{i}':customer for i,customer in enumerate(batch)}
            placeholders=','.join(':'+key for key in binds)
            try:
                cursor.execute(f'SELECT cst_no,fst_ent_dtm,co_mem_dt FROM ods_icc.sa001m WHERE cst_no IN ({placeholders})',binds)
                rows=cursor.fetchmany(len(batch)+1)
            except Exception as exc:
                audit['unavailable']=str(exc).splitlines()[0];break
            for customer,created,member in rows:
                key=str(customer).strip().upper()
                record={'registered_date':created,'older_business_date':member,'source':'ODS_ICC.SA001M FST_ENT_DTM / CO_MEM_DT','verification':'confirmed'}
                if key in output and output[key]!=record:record['verification']='conflict'
                output[key]=record
            audit['read']=len(output)
    return output,audit


def target_eligible_bookings(frame):
    if 'Target_Eligible' not in frame:return frame
    return frame.loc[frame.Target_Eligible.fillna(True).astype(bool)]
