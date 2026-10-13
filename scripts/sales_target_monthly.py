"""Pure monthly target allocation; no IO, runtime writes or implicit history.

Inputs use confirmed owner history or explicitly authorized current-owner fallback.
Fallback is audited without claiming historical effective dates. Rates use measures.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime

UNASSIGNED = "(미지정)"


def optional_number(value):
    if value is None or value == "":
        return None
    if isinstance(value, str) and not value.strip():
        return None
    if isinstance(value, bool):
        raise ValueError("Boolean is not a target")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Target must be finite")
    return number


def month_owner_mapping(records, month):
    """One transition in a month takes effect for that entire month.

    Several distinct incoming owners in one month, overlapping assignments or
    expiry without a successor need audited history; they cannot be guessed.
    Blank end dates mean open ended. Blank start dates cannot establish history.
    """
    datetime.strptime(month, "%Y%m")
    by_customer = defaultdict(list)
    for record in records:
        if str(record.get('OWNER_DATE_BASIS') or '').startswith('observed_only'):
            raise ValueError('Observed owner date is not confirmed effective history')
        customer = str(record.get("CUSTOMER_NO") or "").strip().upper()
        if not customer:
            raise ValueError("Blank customer in owner history")
        start = str(record.get("SALES_START_DATE") or "").strip().removesuffix(".0")
        end = str(record.get("SALES_END_DATE") or "").strip().removesuffix(".0") or "99991231"
        datetime.strptime(start, "%Y%m%d")
        datetime.strptime(end, "%Y%m%d")
        if start > end:
            raise ValueError(f"Invalid owner interval: {customer}")
        by_customer[customer].append((start, end, str(record.get("SALESMAN_NO") or UNASSIGNED).strip()))
    result = {}
    for customer, intervals in by_customer.items():
        incoming = [(start, end, owner) for start, end, owner in intervals if start[:6] == month]
        owners = {owner for _, _, owner in incoming}
        if len(owners) > 1:
            raise ValueError(f"Multiple owner changes in {month}: {customer}")
        if incoming:
            new_start = min(start for start, _, _ in incoming)
            if any(start < new_start <= end and owner not in owners for start, end, owner in intervals):
                raise ValueError(f"Overlapping owner intervals: {customer}")
            result[customer] = next(iter(owners))
            continue
        active = {owner for start, end, owner in intervals if start[:6] < month <= end[:6]}
        if len(active) > 1:
            raise ValueError(f"Overlapping owners in {month}: {customer}")
        if any(end[:6] == month for start, end, _ in intervals if start[:6] < month):
            raise ValueError(f"Owner expiry without successor in {month}: {customer}")
        result[customer] = next(iter(active), UNASSIGNED)
    return result


def allocate_month_targets(shipper_measures, owners, team_numerator, explicit_rates=None):
    """Allocate a fixed monthly goal by customer measures with explicit rates.

    Each unique customer has a denominator and an automatic numerator basis.
    The remainder after explicit rates is distributed by automatic numerator
    basis (denominator fallback). Missing owners remain visible as UNASSIGNED.
    Conflicting overrides fail instead of changing a fixed goal or override.
    """
    total = optional_number(team_numerator)
    if total is None or total < 0:
        raise ValueError("Monthly team numerator must be nonnegative")
    groups = defaultdict(lambda: {"denominator": 0.0, "basis": 0.0, "customers": []})
    seen = set()
    for row in shipper_measures:
        customer = str(row["customer"]).strip().upper()
        if not customer or customer in seen:
            raise ValueError(f"Duplicate or blank shipper: {customer}")
        seen.add(customer)
        den = optional_number(row["denominator"])
        basis = optional_number(row.get("basis_numerator"))
        if den is None or den < 0 or (basis is not None and basis < 0):
            raise ValueError(f"Invalid target basis: {customer}")
        owner = owners.get(customer) or UNASSIGNED
        group = groups[owner]
        group["denominator"] += den
        group["basis"] += basis if basis is not None else den
        group["customers"].append(customer)
    explicit = {owner: optional_number(rate) for owner, rate in (explicit_rates or {}).items()}
    if any(rate is not None and rate < 0 for rate in explicit.values()):
        raise ValueError("Explicit target rate must be nonnegative")
    if any(rate is not None and owner not in groups for owner, rate in explicit.items()):
        raise ValueError("Override owner missing from monthly roster")
    fixed = sum(rate * groups[owner]["denominator"] for owner, rate in explicit.items() if rate is not None)
    remainder = total - fixed
    if remainder < -1e-9:
        raise ValueError("Explicit target exceeds preserved team goal")
    automatic = [owner for owner in groups if explicit.get(owner) is None]
    basis = sum(groups[owner]["basis"] for owner in automatic)
    if basis == 0:
        for owner in automatic:
            groups[owner]["basis"] = groups[owner]["denominator"]
        basis = sum(groups[owner]["basis"] for owner in automatic)
    if remainder > 1e-9 and basis <= 0:
        raise ValueError("No allocation basis for remaining goal")
    output = {}
    for owner, group in groups.items():
        rate = explicit.get(owner)
        num = rate * group["denominator"] if rate is not None else (max(0.0, remainder) * group["basis"] / basis if basis else 0.0)
        den = group["denominator"]
        output[owner] = {"numerator": num, "denominator": den,
                         "target": num / den if den else None,
                         "explicit": rate is not None, "customers": sorted(group["customers"])}
    if not math.isclose(sum(cell["numerator"] for cell in output.values()), total, abs_tol=1e-8):
        raise ValueError("Monthly target conservation failed")
    return output


APPROVED_TARGET_BLANKS={'no_historical_performance','zero_baseline_denominator','no_historical_month_weight','no_assigned_goal_basis','not_current_office'}


def blank_target_cell(state, blank_states=None):
    if state not in APPROVED_TARGET_BLANKS:
        raise ValueError('Unverified missing source is not an approved blank')
    return {'numerator':None,'denominator':None,'target':None,'basis_state':state,
            'blank_states':blank_states or {state:1},'coverage':'verified_blank'}


def quarter_target(monthly_cells):
    numerator=0.;denominator=0.;blank_states={};known=0
    for cell in monthly_cells:
        num=optional_number(cell.get('numerator'));den=optional_number(cell.get('denominator'))
        if num is None or den is None:
            if cell.get('basis_state') not in APPROVED_TARGET_BLANKS:
                raise ValueError('Quarter contains unverified missing target basis')
        else:
            if num<0 or den<0:raise ValueError('Negative target measures')
            numerator+=num;denominator+=den;known+=1
        for state,count in cell.get('blank_states',{}).items():
            if state not in APPROVED_TARGET_BLANKS:raise ValueError('Unknown target blank reason')
            blank_states[state]=blank_states.get(state,0)+count
    if not known and blank_states:
        return blank_target_cell(next(iter(blank_states)),blank_states)
    result={'numerator':numerator,'denominator':denominator,'target':numerator/denominator if denominator else None}
    if blank_states:result.update(blank_states=blank_states,coverage='known_with_verified_blanks',basis_state='known')
    return result


def merge_month_history(previous, calculated, change_month):
    """Freeze historical months and explicit values; never infer missing history."""
    datetime.strptime(change_month, "%Y%m")
    merged = dict(previous)
    for month, cell in calculated.items():
        if month < change_month:
            if month not in previous:
                raise ValueError(f"Missing frozen month history: {month}")
            continue
        old = previous.get(month)
        if old and old.get("explicit"):
            continue
        merged[month] = cell
    return merged


def attach_verified_target_source(rows, source):
    """Connect reviewed monthly source measures to the production index rows.

    The source supplies owner intervals or resolved audited monthly owner maps
    and office rosters. Numeric inputs must be confirmed before attachment.
    """
    if source.get('format') != 'sales-target-monthly-input-v1' or source.get('verification') != 'confirmed':
        raise ValueError('Monthly source must be confirmed, not an observed estimate')
    if source.get('unresolved_customers'):
        raise ValueError('Unresolved owner treatment awaits a decision')
    by_pair = {(row['tab'], row['name']): row for row in rows if row.get('row_type') == 'SALES'}
    performance_only=set(source.get('performance_only_customers',[]))
    performance_only.update(source.get('no_historical_goal_customers',[]))
    company_basis=source.get('target_basis_policy')=='company_2025_actuals_official_office_attribution'
    totals = {row['tab']: row for row in rows if row.get('row_type') == 'TOTAL'}
    owners_by_month = {}; roster_by_month = {}
    for month, origins in sorted(source['months'].items()):
        mapping = source.get('owner_maps_by_month', {}).get(month)
        if mapping is None:
            mapping = month_owner_mapping(source['owner_intervals'], month)
        else:
            mapping = dict(mapping)
            if any(not str(customer).strip() or not str(owner).strip() for customer,owner in mapping.items()):
                raise ValueError('Monthly owner map has blank customer or owner')
        owners_by_month[month] = mapping
        roster_by_month[month] = {}
        for origin, inputs in origins.items():
            approved = set(inputs['approved_salespeople'])
            roster_by_month[month][origin] = approved
            allocations = {}
            for kpi, spec in inputs['kpis'].items():
                if kpi not in ('booking', 'lifting', 'high_profit'):
                    raise ValueError('Unknown monthly KPI')
                customers = {str(cell['customer']).strip().upper() for cell in spec['shipper_measures']}
                if customers & performance_only:
                    raise ValueError('Confirmed new customers must never receive targets')
                excluded=set(source.get('excluded_unassigned_by_month',{}).get(month,{}).get(origin,[]))
                if customers & excluded:
                    raise ValueError('Excluded unassigned customers must never receive targets')
                if any(customer not in mapping or mapping[customer] == UNASSIGNED for customer in customers):
                    raise ValueError('Unconfirmed customer allocation awaits a decision')
                if any(mapping[customer] not in approved for customer in customers):
                    raise ValueError('Monthly customer owner outside verified office roster')
                if company_basis and not spec['shipper_measures']:
                    if spec.get('state') not in set(source.get('approved_blank_states',[])) & APPROVED_TARGET_BLANKS:
                        raise ValueError('Empty KPI without verified approved blank state')
                    allocations[kpi]={}
                else:
                    allocations[kpi] = allocate_month_targets(spec['shipper_measures'], mapping, spec['team_numerator'], spec.get('explicit_rates'))
            if set(allocations) != {'booking', 'lifting', 'high_profit'}:
                raise ValueError('Monthly source must contain all three KPIs')
            for owner in approved:
                pair = (origin, owner)
                row = by_pair.get(pair)
                if row is None:
                    row = {'tab': origin, 'name': owner, 'row_type': 'SALES', 'accounts': {},
                           'kpi': {key: {q: {'target': None, ('perform' if q=='q1' else 'progress'): None, 'gap': None} for q in ('q1','q2','q3','q4')} for key in allocations}}
                    by_pair[pair] = row; rows.append(row)
                cells = row.setdefault('month_targets', {}).setdefault(month, {})
                for key, per_owner in allocations.items():
                    if month < source.get('recalculate_from_month', '000000') and key in cells:
                        continue
                    if company_basis:
                        spec=inputs['kpis'][key]
                        blanks={}
                        for blank in spec.get('blank_customer_states',[]):
                            if blank['owner']==owner:blanks[blank['state']]=blanks.get(blank['state'],0)+1
                        if owner in per_owner:
                            cells[key]=dict(per_owner[owner],basis_state='known')
                            if blanks:cells[key].update(blank_states=blanks,coverage='known_with_verified_blanks')
                        else:
                            cells[key]=blank_target_cell(next(iter(blanks),'no_assigned_goal_basis'),blanks or None)
                    else:
                        cells[key] = per_owner.get(owner, {'numerator': 0.0, 'denominator': 0.0, 'target': None})
            if origin not in totals:
                raise ValueError('Monthly source missing workbook team total row')
            total_cells = totals[origin].setdefault('month_targets', {}).setdefault(month, {})
            for key, per_owner in allocations.items():
                if month < source.get('recalculate_from_month', '000000') and key in total_cells:
                    continue
                if company_basis:
                    spec=inputs['kpis'][key]
                    components=list(per_owner.values())+[blank_target_cell(state,{state:count}) for state,count in spec.get('blank_states',{}).items()]
                    total_cells[key]=quarter_target(components) if components else blank_target_cell(spec['state'])
                else:
                    total_cells[key] = quarter_target(list(per_owner.values()))
        for origin,customers in source.get('performance_only_by_month',{}).get(month,{}).items():
            for customer,owner in customers.items():
                if customer not in performance_only:
                    raise ValueError('Performance-only owner mapping requires confirmed new status')
                roster_by_month[month].setdefault(origin,set()).add(owner)
                pair=(origin,owner)
                if pair not in by_pair:
                    row={'tab':origin,'name':owner,'row_type':'PERFORMANCE_ONLY','accounts':{},
                         'target_eligible':False,'performance_owner_basis':source.get('performance_owner_basis'),
                         'kpi':{key:{q:{'target':None,('perform' if q=='q1' else 'progress'):None,'gap':None} for q in ('q1','q2','q3','q4')} for key in ('booking','lifting','high_profit')}}
                    rows.append(row);by_pair[pair]=row
    if company_basis:
        for row in rows:
            if row.get('row_type')=='PERFORMANCE_ONLY':continue
            for month in source['months']:
                monthly=row.setdefault('month_targets',{}).setdefault(month,{})
                for key in ('booking','lifting','high_profit'):
                    monthly.setdefault(key,blank_target_cell('not_current_office'))
            row['goal_roster_quarters']=['q1','q2']+[q for q,months in {'q3':('202607','202608','202609'),'q4':('202610','202611','202612')}.items()
                if any(row.get('name') in roster_by_month.get(m,{}).get(row.get('tab'),set()) for m in months) or row.get('row_type')=='TOTAL']
    for row in rows:
        for quarter, months in {'q3': ('202607','202608','202609'), 'q4': ('202610','202611','202612')}.items():
            for key in ('booking', 'lifting', 'high_profit'):
                cell = row.setdefault('kpi', {}).setdefault(key, {}).setdefault(quarter, {'target': None, 'progress': None, 'gap': None})
                # Existing nonblank values, including 0, remain authoritative.
                if optional_number(cell.get('target')) is not None and cell.get('target_source') != 'verified_monthly_allocation':
                    cell['target_source'] = 'explicit'
                    continue
                measures = [row.get('month_targets', {}).get(month, {}).get(key) for month in months]
                if all(measure is not None for measure in measures):
                    cell.update(quarter_target(measures)); cell['target_source'] = 'verified_monthly_allocation'
    return owners_by_month, roster_by_month
