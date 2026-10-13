"""Company-owned 2025 target basis, independent of 2026 actual performance.

Physical route capacity remains conserved. Official offices only decide goal
attribution; verified zero, no history and missing source are distinct states.
"""
from collections import defaultdict,Counter
import pandas as pd

def refresh_official_memory(evidence,builder):
    """Refresh just the directory; keep existing performance/cohort evidence."""
    import io
    import update_target_workbook_from_current_customer_owners as parser
    from google_verified_transport import GoogleSession
    from google.auth.transport.requests import Request
    session=GoogleSession();session.prefer_verified_stdlib=True
    try:
        credentials=builder.get_creds();credentials.refresh(Request(session=session))
        session.headers['Authorization']='Bearer '+credentials.token
        response=session.get('https://www.googleapis.com/drive/v3/files/'+parser.SALES_OWNER_SOURCE_ID,
            params={'supportsAllDrives':'true','fields':'id,name,mimeType,modifiedTime,driveId'},timeout=30)
        response.raise_for_status();meta=response.json()
        if meta['mimeType']=='application/vnd.google-apps.spreadsheet':
            response=session.get('https://www.googleapis.com/drive/v3/files/'+meta['id']+'/export',
                params={'mimeType':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},timeout=60)
        else:response=session.get('https://www.googleapis.com/drive/v3/files/'+meta['id'],params={'supportsAllDrives':'true','alt':'media'},timeout=60)
        response.raise_for_status()
        entries=parser.load_owner_entries(io.BytesIO(response.content),pd.DataFrame(evidence['current']),{})
        pairs=set();unlinked=[]
        for origin,people in entries.items():
            for entry in people:
                if entry.match_status!='unmatched':pairs.add((origin,entry.resolved_sales))
                else:
                    pairs.add((origin,entry.input_name))
                    unlinked.append(dict(origin=origin,name=entry.input_name,customer_owner_link='unlinked'))
        evidence.update(official_roster=pairs,official_unlinked_roles=unlinked,
            official_roster_meta=dict(verification='confirmed',source_id=meta['id'],title=meta['name'],modified_time=meta['modifiedTime'],
                drive_id=meta.get('driveId'),read_basis='fresh shared-drive native source, memory only'))
    finally:session.close()

def expose_verified_memory_source(evidence,source):
    """Optional read-only loopback bridge for a --keepalive diagnostic session."""
    import json,threading
    from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
    if source['verification']!='confirmed':return
    if '_goal_memory_bridge' in evidence:
        evidence['_goal_memory_bridge']['source']=source;return
    state={'source':source}
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path!='/monthly-source.json':self.send_error(404);return
            body=json.dumps(state['source'],ensure_ascii=False,separators=(',',':'),allow_nan=False).encode('utf-8')
            self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',42127),Handler)
    state['server']=server;evidence['_goal_memory_bridge']=state
    threading.Thread(target=server.serve_forever,daemon=True).start()
    print('READ_ONLY_MEMORY_SOURCE: http://127.0.0.1:42127/monthly-source.json',flush=True)

APPROVED_BLANK_STATES={'no_historical_performance','zero_baseline_denominator','no_historical_month_weight','no_assigned_goal_basis'}
KPI_KEYS=('booking','lifting','high_profit')

def official_roster_from_projected(records,current):
    import update_target_workbook_from_current_customer_owners as parser
    available={parser.norm_key(r['SALESMAN_NO']):r['SALESMAN_NO'] for r in current if parser.norm_key(r.get('SALESMAN_NO'))}
    pairs=set();unmatched=[]
    for sheet,row,port,dept,name,icc,job in records:
        if sheet not in parser.CONTACT_POINT_SHEETS or not parser.contact_saleslike(dept,job):continue
        origin=parser.PORT_TARGET_MAP.get(parser.contact_port_key(port),parser.SHEET_DEFAULT_TARGET.get(sheet))
        if not origin or not parser.looks_like_contact_name(name):continue
        actor,status=parser.resolve_sales_from_keys([parser.id_key(icc),parser.person_key(name),parser.norm_key(name)],available)
        if actor and status!='unmatched':pairs.add((origin,actor))
        else:
            # This is a real person in the official sales directory, not a
            # fabricated customer-owner ID. No customer is assigned by name.
            pairs.add((origin,name))
            unmatched.append(dict(sheet=sheet,row=row,origin=origin,name=name,icc=icc,customer_owner_link='unlinked'))
    return pairs,unmatched

def official_priority_roster(current_roster,official_roster):
    official_names={owner for origin,owner in official_roster}
    return {(origin,owner) for origin,owner in current_roster if owner not in official_names}|set(official_roster)

def home_for_entry(entry,physical_origin):
    if entry is None:return None
    pair,record=entry
    if pair[0]:return pair[0]
    origins=record.get('TARGET_CONFIRMED_OWNER_ORIGINS') or record.get('TARGET_CURRENT_OWNER_ORIGINS') or []
    return physical_origin if physical_origin in origins else None

def build_company_2025_source(builder,evidence):
    from build_sales_target_monthly_input import resolve_month_owners
    import sys
    diagnostic_memory='--keepalive' in sys.argv and hasattr(builder,'get_creds')
    if diagnostic_memory:refresh_official_memory(evidence,builder)
    base=evidence['base'];bsa=evidence['bsa'];live=evidence['live']
    official=set(evidence.get('official_roster',[]))
    if not official or evidence.get('official_roster_meta',{}).get('verification')!='confirmed':
        raise ValueError('Latest official office roster must be verified before 2025 company target allocation')
    current_roster=official_priority_roster(evidence['current_roster'],official)
    classes=evidence.get('customer_classes',{})
    new={c for c,s in classes.items() if s['class']=='confirmed_new'}
    annual=base.groupby('customer')[['nlst','w3f','w3l','w3h']].sum()
    historical_rows=base.groupby('customer').size().to_dict()
    routes=base.loc[base.nlst.gt(0)].groupby(['tab','dest','dst_port','customer']).nlst.sum().reset_index()
    routes['share']=routes.nlst/routes.groupby(['tab','dest','dst_port']).nlst.transform('sum')
    old_capacity=bsa.loc[bsa.YYYYMM.astype(str).str.startswith('2025')]
    old_alloc=old_capacity.merge(routes,on=['tab','dest','dst_port'],how='left')
    old_alloc['customer_bsa']=old_alloc.route_bsa*old_alloc['share']
    annual_bsa=old_alloc.loc[old_alloc.customer.notna()].groupby('customer').customer_bsa.sum().to_dict()
    old_bsa_by_month=old_alloc.loc[old_alloc.customer.notna()].groupby(['YYYYMM','customer']).customer_bsa.sum().to_dict()
    has_month='YYYYMM' in base
    month_weights=base.groupby(['YYYYMM','customer']).w3f.sum().to_dict() if has_month else {}
    available_old_months=set(base.YYYYMM.astype(str)) if has_month else None
    no_history=set(live.customer)-set(annual.index)-new
    source=dict(format='sales-target-monthly-input-v1',verification='pending_history',months={},owner_intervals=[],
        owner_maps_by_month={},owner_origins_by_month={},owner_offices_by_month={},owner_resolution_audit=[],unresolved_customers=[],
        performance_only_customers=sorted(new),no_historical_goal_customers=sorted(no_history),performance_only_by_month={},performance_observations_by_month={},
        unverified_new_performance_owner_groups=[],customer_classes=classes,registration_audit=evidence.get('registration_audit'),
        goal_exclusion_policy='exclude_unassigned_without_redistribution',excluded_unassigned_by_month={},goal_exclusion_audit=[],
        bsa_route_goal_ledger={},annual_bsa_goal_allocations=[],basis_reconciliation_required=False,dataset_id=evidence['dataset_id'],
        target_basis_policy='company_2025_actuals_official_office_attribution',approved_blank_states=sorted(APPROVED_BLANK_STATES),
        official_roster_meta=evidence['official_roster_meta'],official_roster=sorted(official),approved_goal_roster=sorted(pair for pair in current_roster if pair[0] in evidence['totals']),
        source_references=dict(workbook_id=builder.DEFAULT_WORKBOOK,cache_kind='existing 2025 company actuals; 2026 actuals used for performance only'),
        denominator_kinds=dict(booking='monthly_route_bsa_2025_normal_lst_share',lifting='same_month_2025_company_w3_fst',high_profit='same_month_2025_company_w3_fst'))
    reports=[]
    for month in evidence['months']:
        historical_month='2025'+month[-2:]
        archive='20260818' if month<'202609' else '20260930'
        approved,blocked,audit=resolve_month_owners(evidence['baseline'],evidence['current'],month,evidence['dataset_id'],evidence['archives'][archive],current_roster,builder.tab_key)
        # Explicit official-office preference also applies to proven prior owners.
        for customer,(pair,record) in list(approved.items()):
            offices=sorted(origin for origin,owner in official if owner==pair[1])
            if offices:
                record=dict(record,TARGET_CURRENT_OWNER_ORIGINS=offices)
                if record.get('TARGET_MONTH_OWNER_BASIS')=='confirmed_effective_change':record['TARGET_CONFIRMED_OWNER_ORIGINS']=offices
                approved[customer]=((offices[0] if len(offices)==1 else '',pair[1]),record)
        relevant_customers=set(base.customer)|set(live.customer)
        source['owner_maps_by_month'][month]={c:entry[0][1] for c,entry in approved.items() if c in relevant_customers}
        source['owner_resolution_audit'].extend(audit)
        source['owner_origins_by_month'][month]={c:entry[0][0] for c,entry in approved.items() if entry[0][0] and c in relevant_customers}
        source['owner_offices_by_month'][month]={c:entry[1].get('TARGET_CONFIRMED_OWNER_ORIGINS') or entry[1].get('TARGET_CURRENT_OWNER_ORIGINS',[]) for c,entry in approved.items() if c in relevant_customers}
        if month==evidence['dataset_id'][:6]:
            annual_slots=defaultdict(float)
            for row in old_alloc.loc[old_alloc.customer.notna()].itertuples(index=False):
                home=home_for_entry(approved.get(row.customer),row.tab)
                if home:
                    annual_slots[(home,approved[row.customer][0][1],row.dest,row.dst_port)]+=float(row.customer_bsa)
            source['annual_bsa_goal_allocations']=[dict(tab=home,Salesman_POR=owner,dest=dest,dst_port=port,allocated_bsa=qty)
                for (home,owner,dest,port),qty in annual_slots.items() if qty>0]
        by_home_weight=defaultdict(float)
        old_period=base.loc[base.YYYYMM.astype(str).eq(historical_month)] if has_month else base
        for row in old_period[['tab','customer','w3f']].itertuples(index=False):
            home=home_for_entry(approved.get(row.customer),row.tab)
            if home:by_home_weight[(home,row.customer)]+=float(row.w3f)
        source['excluded_unassigned_by_month'][month]={}
        reasons=Counter();source['months'][month]={}
        capacity=bsa.loc[bsa.YYYYMM.astype(str).eq(month)]
        alloc=capacity.merge(routes,on=['tab','dest','dst_port'],how='left')
        alloc['allocated_bsa']=alloc.route_bsa*alloc['share']
        assigned=[];reserved=[];pending=[];booking_weights=defaultdict(float)
        for row in alloc.itertuples(index=False):
            customer=None if pd.isna(row.customer) else str(row.customer)
            qty=float(row.route_bsa if customer is None else row.allocated_bsa)
            if qty<=0:continue
            cell=dict(tab=row.tab,physical_tab=row.tab,YYYYMM=month,dest=row.dest,dst_port=row.dst_port,allocated_bsa=qty,shipper_no=customer or '')
            home=home_for_entry(approved.get(customer),row.tab)
            if customer is None or customer in new or (customer not in approved and blocked.get(customer) in (None,'current_source_owner_missing','current_source_owner_outside_workbook_roster')):
                cell['reason']='no_2025_customer_route_basis' if customer is None else 'confirmed_new_no_goal' if customer in new else 'owner_outside_verified_roster' if blocked.get(customer)=='current_source_owner_outside_workbook_roster' else 'unassigned_current_owner'
                reserved.append(cell)
                if customer is not None and customer not in new:source['excluded_unassigned_by_month'][month].setdefault(row.tab,[]).append(customer)
            elif home is None:
                cell['reason']=blocked.get(customer,'official_office_unresolved');pending.append(cell)
                reasons[cell['reason']]+=1;source['unresolved_customers'].append(dict(month=month,origin=row.tab,customer=customer,reason=cell['reason']))
            else:
                cell.update(tab=home,Salesman_POR=approved[customer][0][1]);assigned.append(cell)
                booking_weights[(home,customer)]+=qty
        totals={k:sum(c['allocated_bsa'] for c in rows) for k,rows in [('assigned',assigned),('reserved',reserved),('pending',pending)]}
        raw=float(capacity.route_bsa.sum())
        if abs(raw-sum(totals.values()))>1e-6:raise ValueError('Physical BSA conservation failed')
        source['bsa_route_goal_ledger'][month]=dict(assigned=assigned,reserved=reserved,pending=pending,totals=dict(raw=raw,**totals))
        historical_members=defaultdict(set)
        for row in base[['tab','customer']].drop_duplicates().itertuples(index=False):
            if row.customer in new:continue
            home=home_for_entry(approved.get(row.customer),row.tab)
            if home:historical_members[home].add(row.customer)
            elif row.customer not in approved and blocked.get(row.customer) in (None,'current_source_owner_missing','current_source_owner_outside_workbook_roster'):
                source['excluded_unassigned_by_month'][month].setdefault(row.tab,[]).append(row.customer)
        for origin,c in booking_weights:historical_members[origin].add(c)
        for row in live[['tab','customer']].drop_duplicates().itertuples(index=False):
            if row.customer in new:continue
            home=home_for_entry(approved.get(row.customer),row.tab)
            if home:historical_members[home].add(row.customer)
            elif row.customer not in approved and blocked.get(row.customer) in (None,'current_source_owner_missing','current_source_owner_outside_workbook_roster'):
                source['excluded_unassigned_by_month'][month].setdefault(row.tab,[]).append(row.customer)
        for home,members in historical_members.items():
            if home not in evidence['totals'] and members:
                reasons['goal_office_not_configured']+=len(members)
        for origin in sorted(evidence['totals']):
            sales=sorted({owner for tab,owner in current_roster if tab==origin}|{entry[0][1] for entry in approved.values() if home_for_entry(entry,origin)==origin})
            inputs=dict(approved_salespeople=sales,kpis={})
            pp=evidence['deltas'].get(origin)
            for index,key in enumerate(KPI_KEYS):
                cells=[];blank_counts=Counter();blank_customers=[]
                for customer in sorted(historical_members[origin]):
                    if historical_rows.get(customer,0)==0:
                        blank_counts['no_historical_performance']+=1
                        blank_customers.append(dict(customer=customer,owner=approved[customer][0][1],state='no_historical_performance'))
                        continue
                    den=booking_weights.get((origin,customer),0.) if key=='booking' else by_home_weight.get((origin,customer),0.)
                    if has_month and key!='booking' and historical_month not in available_old_months:
                        reasons['historical_month_source_missing']+=1;continue
                    stats=annual.loc[customer]
                    base_den=annual_bsa.get(customer,0.) if key=='booking' else float(stats.w3f)
                    numerator=float(stats['w3f' if key=='booking' else 'w3l' if key=='lifting' else 'w3h'])
                    if base_den<=0:
                        blank_counts['zero_baseline_denominator']+=1
                        blank_customers.append(dict(customer=customer,owner=approved[customer][0][1],state='zero_baseline_denominator',historical_numerator=numerator,historical_denominator=base_den))
                        continue
                    if den<=0:
                        blank_counts['no_historical_month_weight']+=1
                        blank_customers.append(dict(customer=customer,owner=approved[customer][0][1],state='no_historical_month_weight',historical_numerator=numerator,historical_denominator=base_den))
                        continue
                    if pp is None or pp[index] is None:reasons['missing_explicit_goal_quantum']+=1;continue
                    goal=(numerator/base_den+pp[index])*den
                    cells.append(dict(customer=customer,denominator=float(den),basis_numerator=goal,owner_confirmed=True,
                        company_2025_numerator=numerator,company_2025_denominator=base_den))
                state='known' if cells else next(iter(blank_counts),'no_assigned_goal_basis')
                inputs['kpis'][key]=dict(team_numerator=sum(c['basis_numerator'] for c in cells),shipper_measures=cells,
                    explicit_rates={},state=state,blank_states=dict(blank_counts),blank_customer_states=blank_customers,basis_state='company_2025_ratio_plus_explicit_origin_quantum')
            source['months'][month][origin]=inputs
        period=live.loc[live.YYYYMM.astype(str).eq(month)]
        observations={}
        for (physical,customer),g in period.loc[period.customer.isin(new|no_history)].groupby(['tab','customer']):
            entry=approved.get(customer);home=home_for_entry(entry,physical)
            owners={str(x).strip() for x in g.Salesman_POR.dropna() if str(x).strip()} if 'Salesman_POR' in g else set()
            owner=entry[0][1] if entry else next(iter(owners)) if len(owners)==1 else None
            if owner:observations.setdefault(home or physical,{})[customer]=owner
        source['performance_only_by_month'][month]=observations
        source['performance_observations_by_month'][month]=observations
        for o,codes in source['excluded_unassigned_by_month'][month].items():source['excluded_unassigned_by_month'][month][o]=sorted(set(codes))
        reports.append(dict(month=month,blocking_reasons=dict(reasons),route_bsa_teu=raw,
            target_basis='2025_company_actuals',blank_kpi_states=dict(Counter(spec['state'] for x in source['months'][month].values() for spec in x['kpis'].values()))))
    if not source['unresolved_customers'] and all(not r['blocking_reasons'] for r in reports):source['verification']='confirmed'
    if diagnostic_memory:expose_verified_memory_source(evidence,source)
    return source,reports
