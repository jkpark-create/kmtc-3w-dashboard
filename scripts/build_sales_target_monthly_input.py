"""Produce monthly target inputs from existing sources entirely in memory.

Default CLI verifies only. No source/report/CSV copies or publication occurs.
Recorded history and authorized current-owner fallback are audited separately.
"""
from __future__ import annotations
import argparse,csv,io,json,os
from collections import defaultdict,Counter
from pathlib import Path
import pandas as pd
import openpyxl
from google.auth.transport.requests import Request
from google_verified_transport import GoogleSession
from sales_target_customer_cohorts import classify_customer,fetch_registration_evidence,evidence_day

ROOT=Path(__file__).resolve().parents[1]
DEFAULT_BASELINE_ID='1dMZ5s2xoiHZ_LLD79CPUuUVThowQnhmQ'
DEFAULT_MONTHS=('202607','202608','202609','202610','202611','202612')


def input_values(rows,parse_pct):
    return {str(row[0]).strip():tuple(parse_pct(row[i] if len(row)>i else None) for i in (1,3,5))
            for row in rows[3:] if row and row[0]}


def roster(rows):
    return {(row['tab'],row['name']) for row in rows if row['row_type']=='SALES'}



def recorded_source_origin(row, tab_key):
    # Shipment UNKNOWN is a useful bucket; an empty customer master office is
    # optional metadata and must remain empty for roster-based resolution.
    country=str(row.get('COUNTRY') or '').strip()
    return tab_key(country,row.get('PORT')) if country else ''


def recorded_period_agreement(baseline,current,month,old_roster,new_roster,tab_key):
    """Agreement of unchanged recorded intervals plus archived/current rosters.

    No observation date or last-booking date is promoted to an effective date.
    Multiple intervals need separate review; they are not reduced to one row.
    """
    old=defaultdict(list);new=defaultdict(list)
    for row in baseline:old[str(row['CUSTOMER_NO']).strip().upper()].append(row)
    for row in current:new[str(row['CUSTOMER_NO']).strip().upper()].append(row)
    fields=('SALESMAN_NO','SALES_START_DATE','SALES_END_DATE')
    approved={};blocked={}
    for customer,records in new.items():
        previous=old.get(customer,[])
        if len(records)!=1 or len(previous)!=1:blocked[customer]='missing_or_multiple_periods';continue
        row=records[0];prior=previous[0]
        if any(str(row.get(key) or '').strip()!=str(prior.get(key) or '').strip() for key in fields):
            blocked[customer]='changed_record_effective_month_unconfirmed';continue
        if str(row.get('OWNER_DATE_BASIS') or '').startswith('observed_only'):
            blocked[customer]='observed_date_only';continue
        start=str(row['SALES_START_DATE']).strip().removesuffix('.0')
        end=str(row['SALES_END_DATE']).strip().removesuffix('.0') or '99991231'
        if len(start)!=8 or not start.isdigit() or len(end)!=8 or not end.isdigit():
            blocked[customer]='invalid_period';continue
        if not start[:6]<=month<=end[:6]:blocked[customer]='period_not_covered';continue
        owner=str(row['SALESMAN_NO']).strip()
        current_origins=sorted(origin for origin,name in new_roster if name==owner)
        pair=(current_origins[0] if len(current_origins)==1 else '',owner)
        if not current_origins or not any(name==owner for origin,name in old_roster):
            blocked[customer]='archived_or_current_office_roster_missing';continue
        approved[customer]=(pair,{**dict(prior),'TARGET_MONTH_OWNER_BASIS':'recorded_owner_period_current_office',
            'TARGET_CURRENT_OWNER_ORIGINS':current_origins})
    return approved,blocked



def resolve_month_owners(baseline, current, month, asof, old_roster, new_roster, tab_key):
    """Keep proven monthly history; audit current-owner fallback separately.

    A last booking/observation is never interpreted as an effective boundary.
    Current ownership must resolve to one active source row, including office.
    """
    approved, blocked = recorded_period_agreement(baseline,current,month,old_roster,new_roster,tab_key)
    new_owner_names={owner for origin,owner in new_roster}
    history_owner_names=new_owner_names|{owner for origin,owner in old_roster}
    old=defaultdict(list); records=defaultdict(list)
    for row in baseline: old[str(row['CUSTOMER_NO']).strip().upper()].append(row)
    for row in current: records[str(row['CUSTOMER_NO']).strip().upper()].append(row)
    audit=[]
    for customer, history in records.items():
        if customer in approved: continue
        active=[]
        for row in history:
            start=str(row.get('SALES_START_DATE') or '').strip().removesuffix('.0')
            end=str(row.get('SALES_END_DATE') or '99991231').strip().removesuffix('.0')
            start_known=len(start)==8 and start.isdigit()
            end_known=len(end)==8 and end.isdigit()
            if end_known and end>=asof and (not start_known or start<=asof):
                active.append(row)
        # COUNTRY/PORT is overwritten with latest booking POR by the existing
        # owner refresh. It is not evidence of an employee's current office.
        pairs={('',str(r.get('SALESMAN_NO') or '').strip()) for r in active}
        if not pairs or (len(pairs)==1 and not next(iter(pairs))[1]):
            blocked[customer]='current_source_owner_missing';continue
        if len(pairs)!=1:
            blocked[customer]='current_source_owner_missing_or_ambiguous';continue
        current_pair=next(iter(pairs))
        current_origins=sorted(origin for origin,name in new_roster if name==current_pair[1])
        if len(current_origins)==1:current_pair=(current_origins[0],current_pair[1])
        if current_pair[1] not in new_owner_names:
            blocked[customer]='current_source_owner_outside_workbook_roster';continue
        chosen=active[0];basis='current_owner_fallback'
        # An explicit confirmed effective date and the archived prior owner are
        # sufficient for one transition. Unknown or multiple changes fallback.
        confirmed=[r for r in history if r.get('OWNER_DATE_BASIS')=='confirmed_effective' and
                   str(r.get('OWNER_EFFECTIVE_DATE') or '').isdigit() and len(str(r['OWNER_EFFECTIVE_DATE']))==8]
        if len(confirmed)==1 and len(old.get(customer,[]))==1:
            incoming=confirmed[0]; prior=old[customer][0]
            effective=str(incoming['OWNER_EFFECTIVE_DATE'])
            candidate=incoming if month>=effective[:6] else prior
            candidate_owner=str(candidate.get('SALESMAN_NO') or '').strip()
            candidate_roster=new_roster if month>=effective[:6] else old_roster
            confirmed_origins=sorted(origin for origin,owner in candidate_roster if owner==candidate_owner)
            pair=(confirmed_origins[0] if len(confirmed_origins)==1 else '',candidate_owner)
            if candidate_owner and confirmed_origins:
                chosen={**candidate,'TARGET_CONFIRMED_OWNER_ORIGINS':confirmed_origins}
                current_pair=pair;basis='confirmed_effective_change'
        approved[customer]=(current_pair,{**dict(chosen),'TARGET_MONTH_OWNER_BASIS':basis,
            'TARGET_CURRENT_OWNER_ORIGINS':sorted(origin for origin,owner in new_roster if owner==current_pair[1])})
        audit.append({'month':month,'customer':customer,'origin':current_pair[0], 'owner':current_pair[1],
                      'basis':basis,'reason':blocked.get(customer),'source_asof':asof,
                      'effective_date':str(chosen.get('OWNER_EFFECTIVE_DATE') or '') if basis=='confirmed_effective_change' else None})
        blocked.pop(customer,None)
    return approved,blocked,audit



def owner_matches_origin(entry, origin, roster):
    """Blank source office is resolved by shipment origin plus approved roster.

    COUNTRY/PORT can be legitimately blank in the canonical customer mapping.
    An explicit conflicting office or an absent owner is never synthesized.
    """
    pair,record=entry
    if record.get('TARGET_MONTH_OWNER_BASIS')=='confirmed_effective_change' and origin not in record.get('TARGET_CONFIRMED_OWNER_ORIGINS',[]):
        return False
    if record.get('TARGET_MONTH_OWNER_BASIS') in ('current_owner_fallback','recorded_owner_period_current_office') and origin not in record.get('TARGET_CURRENT_OWNER_ORIGINS',[]):
        return False
    return pair[0] in ('',origin) and (origin,pair[1]) in roster


def read_existing_evidence(builder,dataset_id,months=DEFAULT_MONTHS,official_projection=None,official_meta=None):
    runtime=Path(os.environ.get('DASHBOARD_RUNTIME_ROOT',str(Path(os.environ['LOCALAPPDATA'])/'KMTC'/'3w-dashboard-runtime')))
    output=runtime/'output'
    with (ROOT/'salesman.csv').open(encoding='utf-8-sig',newline='') as handle:current=list(csv.DictReader(handle))
    session=GoogleSession();session.prefer_verified_stdlib=True
    credentials=builder.get_creds();credentials.refresh(Request(session=session))
    session.headers['Authorization']='Bearer '+credentials.token
    try:
        response=session.get('https://sheets.googleapis.com/v4/spreadsheets/'+builder.DEFAULT_WORKBOOK+'/values:batchGet',
            params={'ranges':['Summary_All!A1:AC400','Target_Input!A1:H100'],'valueRenderOption':'UNFORMATTED_VALUE'},timeout=60)
        response.raise_for_status();ranges=response.json()['valueRanges']
        current_rows=builder.parse_summary(ranges[0]['values'])
        current_roster=roster(current_rows)
        deltas=input_values(ranges[1]['values'],builder.parse_pct)
        totals={str(row[0]):{key:builder.parse_pct(row[i] if len(row)>i else None) for key,i in [('booking',25),('lifting',26),('high_profit',27)]}
                for row in ranges[0]['values'][4:] if len(row)>28 and str(row[28]).upper()=='TOTAL'}
        archives={};archive_inputs={}
        paths={'20260818':ROOT/'output'/'target_workbook_before_current_customer_owner_update_20260818.xlsx',
               '20260930':output/'target_workbook_before_current_customer_owner_update_20260930.xlsx'}
        for date,path in paths.items():
            if not path.exists():raise FileNotFoundError('Existing archived roster required: '+str(path))
            workbook=openpyxl.load_workbook(path,read_only=True,data_only=True)
            try:
                archives[date]=roster(builder.parse_summary(list(workbook['Summary_All'].values)))
                archive_inputs[date]=input_values(list(workbook['Target_Input'].values),builder.parse_pct)
            finally:workbook.close()
        response=session.get('https://www.googleapis.com/drive/v3/files/'+DEFAULT_BASELINE_ID,params={'alt':'media'},timeout=60)
        response.raise_for_status()
        baseline=list(csv.DictReader(io.StringIO(response.content.decode('utf-8-sig'))))
        if official_projection is None:
            import update_target_workbook_from_current_customer_owners as official_parser
            official_id=official_parser.SALES_OWNER_SOURCE_ID
            metadata=session.get('https://www.googleapis.com/drive/v3/files/'+official_id,
                params={'supportsAllDrives':'true','fields':'id,name,mimeType,modifiedTime,driveId'},timeout=30)
            metadata.raise_for_status();official_file=metadata.json()
            if official_file['mimeType']=='application/vnd.google-apps.spreadsheet':
                response=session.get('https://www.googleapis.com/drive/v3/files/'+official_id+'/export',
                    params={'mimeType':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},timeout=60)
            else:
                response=session.get('https://www.googleapis.com/drive/v3/files/'+official_id,
                    params={'supportsAllDrives':'true','alt':'media'},timeout=60)
            response.raise_for_status()
            workbook=openpyxl.load_workbook(io.BytesIO(response.content),read_only=True,data_only=True)
            official_projection=[]
            try:
                for sheet in official_parser.CONTACT_POINT_SHEETS:
                    if sheet not in workbook.sheetnames:continue
                    header=None;port='';dept=''
                    for row_num,row in enumerate(official_parser.contact_sheet_rows(workbook[sheet]),start=1):
                        found=official_parser.contact_header(row)
                        if found:header=found;port='';dept='';continue
                        if header is None:continue
                        def value(key):
                            index=header.get(key)
                            return row[index] if index is not None and index<len(row) else ''
                        candidate=official_parser.contact_port_key(value('port'))
                        if candidate in official_parser.PORT_TARGET_MAP:port=candidate
                        if value('dept'):dept=value('dept')
                        name=official_parser.contact_name(row,header);job=value('job')
                        if official_parser.looks_like_contact_name(name) and official_parser.contact_saleslike(dept,job):
                            official_projection.append([sheet,row_num,port,dept,name,value('icc'),job])
            finally:workbook.close()
            official_meta={'verification':'confirmed','source_id':official_file['id'],'title':official_file['name'],
                'modified_time':official_file['modifiedTime'],'drive_id':official_file.get('driveId'),
                'read_basis':'fresh shared-drive metadata + native export/bytes parsed entirely in memory'}
    finally:session.close()
    wanted=['BKG_SHPR_CST_NO','POR_CTR_CD','POR_PLC_CD','DLY_CTR_CD','DLY_PLC_CD','FST_TEU','LST_TEU','LST_Status','Lead_time (BKG_Sche)','고/저','YYYYMM','Booking_date','Salesman_POR']
    cache2025=output/'_cache_2025.parquet'
    if not cache2025.exists():cache2025=ROOT/'output'/'_cache_2025.parquet'
    # Existing caches only; deliberately do not call ensure_2025_cache.
    base=pd.read_parquet(cache2025,columns=wanted)
    baseline_customers=set(base.BKG_SHPR_CST_NO.fillna('').astype(str).str.strip().str.upper())-{''}
    live=pd.read_parquet(output/f'_cache_{dataset_id}.parquet',columns=[c for c in wanted if c!='고/저'])
    def prepare(frame):
        frame=frame.copy()
        frame['customer']=frame.BKG_SHPR_CST_NO.fillna('').astype(str).str.strip().str.upper()
        frame['tab']=[builder.tab_key(c,p) for c,p in zip(frame.POR_CTR_CD,frame.POR_PLC_CD)]
        frame['team']=[builder.classify_team(c,d) for c,d in zip(frame.POR_CTR_CD,frame.DLY_CTR_CD)]
        frame=frame.loc[builder.target_team_mask(frame)&frame.customer.ne('')].copy()
        fst=pd.to_numeric(frame.FST_TEU,errors='coerce').fillna(0)
        lst=pd.to_numeric(frame.LST_TEU,errors='coerce').fillna(0)
        frame['nlst']=lst.where(frame.LST_Status.eq('Normal'),0)
        w3=frame['Lead_time (BKG_Sche)'].eq('WOS-3')
        frame['w3f']=fst.where(w3,0)
        frame['w3l']=lst.where(w3&frame.LST_Status.eq('Normal'),0)
        frame['w3h']=fst.where(w3&frame.get('고/저',pd.Series('',index=frame.index)).eq('고수익'),0)
        frame['dest']=frame.DLY_CTR_CD.fillna('').astype(str)
        frame['dst_port']=frame.DLY_PLC_CD.fillna('').astype(str)
        return frame
    base=prepare(base);live=prepare(live)
    candidates=set(live.customer)-baseline_customers
    registrations,registration_audit=fetch_registration_evidence(candidates,batch_size=100)
    date_lookup={value:evidence_day(value) for value in live.Booking_date.dropna().unique()}
    observed=live.assign(trade_day=live.Booking_date.map(date_lookup).fillna('99999999'))
    first_trade=observed.groupby('customer').trade_day.min().to_dict()
    customer_classes={customer:classify_customer(customer,baseline_customers,registrations.get(customer),first_trade.get(customer))
                      for customer in set(live.customer)}
    bsa=builder.load_bsa_routes(output/f'BSA_raw_monthly3W_{dataset_id}.csv',set(months)|set(builder.MONTHS_2025))
    official_fields={}
    if official_projection is not None:
        from sales_target_company_2025 import official_roster_from_projected
        import update_target_workbook_from_current_customer_owners as official_parser
        if not official_meta or official_meta.get('verification')!='confirmed' or official_meta.get('source_id')!=official_parser.SALES_OWNER_SOURCE_ID:
            raise ValueError('Connected latest official source provenance must be confirmed')
        official_pairs,unlinked=official_roster_from_projected(official_projection,current)
        official_fields=dict(official_roster=official_pairs,official_roster_meta=official_meta,official_unlinked_roles=unlinked,
            target_basis_policy='company_2025_actuals_official_office_attribution')
    return dict(baseline=baseline,current=current,current_roster=current_roster,archives=archives,archive_inputs=archive_inputs,
                current_summary_rows=current_rows,**official_fields,
                deltas=deltas,totals=totals,base=base,live=live,bsa=bsa,months=months,dataset_id=dataset_id,
                customer_classes=customer_classes,registration_audit=registration_audit)



def add_existing_activity_lane_basis(routes, live, new_customers):
    """Existing fallback: current Normal LST only for lanes without 2025 basis.

    Confirmed new customers have no goal weight. Adding them cannot change any
    existing allocation. No-activity lanes remain explicitly unassigned.
    """
    existing=live.loc[~live.customer.isin(new_customers)&live.nlst.gt(0)]
    fallback=existing.groupby(['tab','dest','dst_port','customer']).nlst.sum().reset_index()
    if fallback.empty:return routes
    known=set(routes[['tab','dest','dst_port']].itertuples(index=False,name=None))
    fallback=fallback.loc[[tuple(row) not in known for row in fallback[['tab','dest','dst_port']].itertuples(index=False,name=None)]].copy()
    fallback['share']=fallback.nlst/fallback.groupby(['tab','dest','dst_port']).nlst.transform('sum')
    return pd.concat([routes,fallback],ignore_index=True)



def unassigned_goal_customer(customer, approved, blocked):
    # A missing current assignment is excluded by explicit user policy.
    # Conflicting owners/office evidence remains a blocker, never an exclusion.
    return customer not in approved and blocked.get(customer) in (None,'current_source_owner_missing')



def monthly_bsa_ledger(capacity, allocated, approved, blocked, roster, month, new_customers):
    """Keep pre-exclusion route shares; excluded capacity is never reassigned.

    Reserves have no customer goal or real salesperson. Pending ownership still
    blocks publishing. This ledger is physical BSA, separate from goal volumes.
    """
    assigned=[];reserved=[];pending=[]
    for row in allocated.itertuples(index=False):
        quantity=float(row.route_bsa if pd.isna(row.customer) else row.denominator)
        if quantity<=0:continue
        cell={'tab':row.tab,'YYYYMM':month,'dest':row.dest,'dst_port':row.dst_port,'allocated_bsa':quantity}
        customer=None if pd.isna(row.customer) else str(row.customer)
        if customer is None or customer in new_customers or unassigned_goal_customer(customer,approved,blocked):
            cell['reason']='no_customer_basis' if customer is None else 'confirmed_new_no_goal' if customer in new_customers else 'unassigned_current_owner'
            reserved.append(cell)
        elif customer in approved and owner_matches_origin(approved[customer],row.tab,roster):
            cell['Salesman_POR']=approved[customer][0][1];assigned.append(cell)
        else:
            cell['reason']=blocked.get(customer,'office_origin_mismatch');pending.append(cell)
    raw=float(capacity.route_bsa.sum())
    totals={key:sum(cell['allocated_bsa'] for cell in cells) for key,cells in [('assigned',assigned),('reserved',reserved),('pending',pending)]}
    if abs(raw-sum(totals.values()))>1e-6:
        raise ValueError('Route BSA ledger does not conserve physical capacity')
    return {'assigned':assigned,'reserved':reserved,'pending':pending,'totals':{'raw':raw,**totals}}


def produce_monthly_input(builder,evidence):
    """Construct numeric input fields; unresolved cases remain explicit blockers.

    Booking denominator uses actual route BSA allocated by 2025 Normal LST;
    lifting/high-profit denominator uses the observed monthly WOS-3 FST cohort.
    No future cohort or unverified owner/date is invented.
    """
    if evidence.get('target_basis_policy')=='company_2025_actuals_official_office_attribution':
        from sales_target_company_2025 import build_company_2025_source
        return build_company_2025_source(builder,evidence)
    base=evidence['base'];live=evidence['live'];bsa=evidence['bsa']
    customer_classes=evidence.get('customer_classes',{})
    new_customers={customer for customer,state in customer_classes.items() if state['class']=='confirmed_new'}
    annual=base.groupby(['tab','customer'])[['nlst','w3f','w3l','w3h']].sum()
    routes=base.loc[base.nlst.gt(0)].groupby(['tab','dest','dst_port','customer'])['nlst'].sum().reset_index()
    route_totals=routes.groupby(['tab','dest','dst_port'])['nlst'].transform('sum')
    routes['share']=routes.nlst/route_totals
    annual_capacity=bsa.loc[bsa.YYYYMM.astype(str).str.startswith('2025')].groupby(['tab','dest','dst_port']).route_bsa.sum().reset_index()
    annual_alloc=annual_capacity.merge(routes,on=['tab','dest','dst_port'],how='left')
    annual_alloc['bsa25']=annual_alloc.route_bsa*annual_alloc['share']
    annual_customer_bsa=annual_alloc.loc[annual_alloc.customer.notna()].groupby(['tab','customer']).bsa25.sum()
    if 'nlst' in live:
        routes=add_existing_activity_lane_basis(routes,live,new_customers)
    source={'format':'sales-target-monthly-input-v1','verification':'pending_history','months':{},'owner_intervals':[],
            'performance_only_customers':sorted(new_customers),'performance_only_by_month':{},
            'owner_maps_by_month':{},'owner_resolution_audit':[],
            'goal_exclusion_policy':'exclude_unassigned_without_redistribution',
            'excluded_unassigned_by_month':{},'goal_exclusion_audit':[],'bsa_route_goal_ledger':{},
            'performance_observations_by_month':{},'unverified_new_performance_owner_groups':[],
            'performance_owner_basis':'dated_snapshot_owner_not_claimed_as_historical_effective_date',
            'customer_classes':customer_classes,'registration_audit':evidence.get('registration_audit'),
            'unresolved_customers':[],'dataset_id':evidence['dataset_id'],'source_references':{'baseline_drive_id':DEFAULT_BASELINE_ID,
            'workbook_id':builder.DEFAULT_WORKBOOK,'cache_kind':'existing immutable 2025 cache and dated current cache'},
            'denominator_kinds':{'booking':'monthly_route_bsa_2025_normal_lst_share','lifting':'observed_monthly_w3_fst','high_profit':'observed_monthly_w3_fst'}}
    seen_intervals=set();report=[];basis_missing=0
    for month in evidence['months']:
        archive='20260818' if month<'202609' else '20260930'
        old_roster=evidence['archives'][archive] if month<'202610' else evidence['current_roster']
        monthly_roster=evidence['current_roster']|old_roster
        approved,blocked,resolutions=resolve_month_owners(evidence['baseline'],evidence['current'],month,evidence['dataset_id'],old_roster,evidence['current_roster'],builder.tab_key)
        source['owner_maps_by_month'][month]={customer:pair[1] for customer,(pair,record) in approved.items()}
        source['owner_resolution_audit'].extend(resolutions)
        period=live.loc[live.YYYYMM.astype(str).eq(month)]
        goal_period=period.loc[~period.customer.isin(new_customers)]
        performance_only={}
        if 'Salesman_POR' in period:
            for (origin,customer),group in period.loc[period.customer.isin(new_customers)].groupby(['tab','customer']):
                owners={str(value).strip() for value in group.Salesman_POR.dropna() if str(value).strip()}
                if len(owners)==1:performance_only.setdefault(origin,{})[customer]=next(iter(owners))
        source['performance_observations_by_month'][month]=performance_only
        verified={origin:dict(customers) for origin,customers in evidence.get('verified_performance_owners',{}).get(month,{}).items()}
        for origin,customers in performance_only.items():
            for customer in customers:
                if customer not in verified.get(origin,{}) and customer in approved and owner_matches_origin(approved[customer],origin,monthly_roster):
                    verified.setdefault(origin,{})[customer]=approved[customer][0][1]
        if month<evidence['dataset_id'][:6]:
            for origin,customers in performance_only.items():
                for customer in customers:
                    if customer not in verified.get(origin,{}):
                        source['unverified_new_performance_owner_groups'].append({'month':month,'origin':origin,'customer':customer})
            performance_only=verified
        source['performance_only_by_month'][month]=performance_only
        capacity=bsa.loc[bsa.YYYYMM.eq(month)]
        allocated=capacity.merge(routes,on=['tab','dest','dst_port'],how='left')
        allocated['denominator']=allocated.route_bsa*allocated['share']
        source['bsa_route_goal_ledger'][month]=monthly_bsa_ledger(capacity,allocated,approved,blocked,monthly_roster,month,new_customers)
        missing_routes=allocated.loc[allocated.customer.isna(),['tab','dest','dst_port','route_bsa']].drop_duplicates()
        # Existing fallback routes need separately verified live activity before
        # they can become target basis. Keep the missing quantity, never drop it.
        monthly_bsa=allocated.loc[allocated.customer.notna()].groupby(['tab','customer']).denominator.sum()
        monthly_w3=goal_period.groupby(['tab','customer']).w3f.sum()
        active_customers=set(period.customer)
        basis_pairs=set(annual.index)|set(monthly_bsa.index)|set(monthly_w3.index)
        relevant=set(c for origin,c in basis_pairs if origin in evidence['totals'])
        eligible={c for c in relevant if c in approved}
        reasons=Counter();source['months'][month]={};source['excluded_unassigned_by_month'][month]={}
        for origin in sorted(evidence['totals']):
            base_rates=evidence['totals'][origin];deltas=evidence['deltas'].get(origin)
            if deltas is None or any(value is None for value in deltas) or any(v is None for v in base_rates.values()):
                reasons['missing_goal_input_or_base']+=1;continue
            if month<'202610' and evidence['archive_inputs'][archive].get(origin)!=deltas:
                reasons['historical_target_input_changed']+=1;continue
            members=sorted(c for o,c in basis_pairs if o==origin and c not in new_customers)
            if not members:continue
            roster_set={owner for tab,owner in evidence['current_roster'] if tab==origin}
            roster_set.update(entry[0][1] for entry in approved.values() if owner_matches_origin(entry,origin,monthly_roster))
            inputs={'approved_salespeople':sorted(roster_set),'kpis':{}}
            owner_stats=defaultdict(lambda:{'w3f':0.,'w3l':0.,'w3h':0.,'bsa25':0.})
            for customer in members:
                if customer not in approved or not owner_matches_origin(approved[customer],origin,monthly_roster):continue
                owner=approved[customer][0][1]
                for field in ('w3f','w3l','w3h'):
                    owner_stats[owner][field]+=float(annual.loc[(origin,customer),field]) if (origin,customer) in annual.index else 0.
                owner_stats[owner]['bsa25']+=float(annual_customer_bsa.get((origin,customer),0))
            excluded_members=set()
            for index,key in enumerate(('booking','lifting','high_profit')):
                cells=[];excluded_denominator=0.0
                for customer in members:
                    denominator=float(monthly_bsa.get((origin,customer),0)) if key=='booking' else float(monthly_w3.get((origin,customer),0))
                    if denominator<=0:continue
                    if unassigned_goal_customer(customer,approved,blocked):
                        excluded_members.add(customer);excluded_denominator+=denominator
                        continue
                    if customer not in approved or not owner_matches_origin(approved[customer],origin,monthly_roster):
                        reason=blocked.get(customer,'current_source_customer_missing' if customer not in approved else 'office_origin_mismatch')
                        source['unresolved_customers'].append({'month':month,'origin':origin,'customer':customer,'reason':reason})
                        reasons[reason]+=1
                    else:
                        record=approved[customer][1]
                        token=(customer,record['SALESMAN_NO'],record['SALES_START_DATE'],record['SALES_END_DATE'])
                        if token not in seen_intervals:source['owner_intervals'].append(record);seen_intervals.add(token)
                    basis_numerator=None
                    if customer in approved and owner_matches_origin(approved[customer],origin,monthly_roster):
                        stats=owner_stats[approved[customer][0][1]]
                        numerator=stats['w3f'] if key=='booking' else stats['w3l'] if key=='lifting' else stats['w3h']
                        base_den=stats['bsa25'] if key=='booking' else stats['w3f']
                        individual_base=numerator/base_den if base_den else base_rates[key]
                        basis_numerator=(individual_base+deltas[index])*denominator
                    else:basis_missing+=1
                    cells.append({'customer':customer,'denominator':denominator,'basis_numerator':basis_numerator,
                        'owner_confirmed':customer in approved and owner_matches_origin(approved[customer],origin,monthly_roster)})
                denominator_total=sum(cell['denominator'] for cell in cells)
                goal_rate=base_rates[key]+deltas[index]
                confirmed_denominator=sum(cell['denominator'] for cell in cells if cell['owner_confirmed'])
                pending_denominator=denominator_total-confirmed_denominator
                source['goal_exclusion_audit'].append({'month':month,'origin':origin,'kpi':key,
                    'reference_before_exclusion_denominator':denominator_total+excluded_denominator,
                    'excluded_denominator':excluded_denominator,'assigned_denominator':confirmed_denominator,
                    'pending_denominator':pending_denominator,
                    'reference_before_exclusion_numerator':goal_rate*(denominator_total+excluded_denominator),
                    'excluded_reference_numerator':goal_rate*excluded_denominator,
                    'assigned_numerator':goal_rate*confirmed_denominator,'pending_reference_numerator':goal_rate*pending_denominator,
                    'basis':'existing_team_rate_reference_not_an_individual_manual_target',
                    'redistributed_excluded_quantity':0.0})
                if not cells:
                    reasons['monthly_denominator_unavailable:'+key]+=1;continue
                # The configured goal is base + the explicit origin quantum.
                # Zero is valid; blank inputs cannot silently become zero.
                inputs['kpis'][key]={'team_numerator':(base_rates[key]+deltas[index])*denominator_total,
                                    'shipper_measures':cells,'explicit_rates':{},
                                    'basis_state':'monthly_owner_2025_portfolio_ratio_plus_origin_input'}
            source['months'][month][origin]=inputs
            source['excluded_unassigned_by_month'][month][origin]=sorted(excluded_members)
        if len(missing_routes):reasons['route_without_2025_normal_lst_basis']+=len(missing_routes)
        scope_breakdown={}
        for scope,subset in [('OBT',period.loc[period.team.eq('OBT')] if 'team' in period else period),
                             ('JP_JBT',period.loc[period.team.eq('JBT')&period.tab.eq('JP')] if 'team' in period else period.iloc[0:0])]:
            scoped_customers=set(subset.customer)
            scoped_new=scoped_customers&new_customers
            scoped_approved=scoped_customers&set(approved)
            scope_breakdown[scope]={'active_customers':len(scoped_customers),'confirmed_new_customers':len(scoped_new),
                'confirmed_new_w3_fst_teu':float(subset.loc[subset.customer.isin(new_customers),'w3f'].sum()),
                'pending_before_new_rule':len(scoped_customers-set(approved)),
                'new_removed_from_goal_history_pending':len(scoped_new-set(approved)),
                'pending_existing_or_unclassified':len(scoped_customers-set(approved)-new_customers),
                'existing_period_roster_agreement':len(scoped_approved-new_customers)}
        report.append({'month':month,'owner_resolution_counts':dict(Counter(r['basis'] for r in resolutions)),
                       'scope_breakdown':scope_breakdown,'source_period_roster_agreement_customers':len(eligible),
                       'active_customers':len(active_customers),'active_agreement_customers':len(active_customers&eligible),
                       'confirmed_new_performance_only_customers':len(active_customers&new_customers),
                       'active_unresolved_existing_or_unclassified_customers':len(active_customers-eligible-new_customers),
                       'performance_only_w3_fst_teu':float(period.loc[period.customer.isin(new_customers),'w3f'].sum()),
                       'performance_only_confirmed_owner_customers':sum(len(values) for values in verified.values()),
                       'performance_only_snapshot_owner_customers':sum(len(values) for values in source['performance_observations_by_month'][month].values()),
                       'route_bsa_teu':float(capacity.route_bsa.sum()),'unassigned_route_bsa_teu':float(missing_routes.route_bsa.sum()),
                       'observed_w3_fst_teu':float(period.w3f.sum()),'candidate_origins':len(source['months'][month]),
                       'blocking_reasons':dict(reasons)})
    source['basis_reconciliation_required']=bool(basis_missing)
    complete_fields=all(len(inputs['kpis'])==3 for origins in source['months'].values() for inputs in origins.values())
    no_blockers=all(not month['blocking_reasons'] for month in report)
    if not source['unresolved_customers'] and not source['unverified_new_performance_owner_groups'] and not basis_missing and complete_fields and no_blockers:
        source['verification']='confirmed'
    return source,report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset-id',required=True)
    parser.add_argument('--verify-only',action='store_true',default=True)
    args=parser.parse_args()
    import build_sales_target_drill_data as builder
    source,report=produce_monthly_input(builder,read_existing_evidence(builder,args.dataset_id))
    print(json.dumps({'verification':source['verification'],'source_references':source['source_references'],
        'owner_intervals':len(source['owner_intervals']),'owner_resolution_counts':dict(Counter(r['basis'] for r in source['owner_resolution_audit'])),'unresolved_customer_month_origin_groups':len({(r['month'],r['origin'],r['customer']) for r in source['unresolved_customers']}),
        'basis_reconciliation_required':source['basis_reconciliation_required'],'months':report},ensure_ascii=False))


if __name__=='__main__':main()
