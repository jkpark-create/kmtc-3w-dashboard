"""Validate goal roster and performance-only roster independently."""


def pairs(values):
    result=[tuple(value) for value in values]
    if len(result)!=len(set(result)):
        raise ValueError('Duplicate roster pair')
    return set(result)


def validate_rosters(index,manifest,approved):
    approved=set(approved)
    declared=index.get('current_target_roster_pairs')
    if declared is not None and pairs(declared)!=approved:
        raise ValueError("Sales Target current goal roster does not match today's owner roster")
    historical=pairs(index.get('historical_target_roster_pairs',[]))
    if historical:
        if index.get('monthly_target_source_verification')!='confirmed':
            raise ValueError('Historical goal roster is not verified')
        monthly={(origin,owner) for roster in index.get('monthly_target_rosters',{}).values()
                 for origin,owners in roster.items() for owner in owners}
        native=set()
        if index.get('target_basis_policy')=='company_2025_actuals_official_office_attribution':
            if index.get('historical_roster_source')!='preserved_native_Summary_All_Q1_Q2':
                raise ValueError('Native frozen quarter roster provenance missing')
            for quarter,values in index.get('historical_roster_by_quarter',{}).items():
                if quarter not in ('q1','q2'):raise ValueError('Only native Q1/Q2 roster may be frozen')
                native|=pairs(values)
        if not historical<=monthly|native:
            raise ValueError('Historical goal roster lacks monthly/native frozen quarter evidence')
    goal_rows=[row for row in index['rows'] if row.get('row_type')=='SALES']
    goals=pairs([(str(row.get('tab','')),str(row.get('name',''))) for row in goal_rows])
    if goals!=approved|historical:
        raise ValueError("Sales Target index does not match today's owner roster")
    actual_rows=[row for row in index['rows'] if row.get('row_type')=='PERFORMANCE_ONLY']
    performance=pairs([(str(row.get('tab','')),str(row.get('name',''))) for row in actual_rows])
    if performance&goals:
        raise ValueError('Performance-only row duplicates a goal row')
    if pairs(index.get('performance_only_roster_pairs',[]))!=performance:
        raise ValueError('Performance-only roster evidence missing')
    for row in actual_rows:
        if row.get('target_eligible') is not False:
            raise ValueError('Performance-only row is target eligible')
        if any(cell.get('target') is not None for quarters in row.get('kpi',{}).values() for cell in quarters.values()):
            raise ValueError('Performance-only row received a target')
        if any(cell.get('numerator',0)!=0 or cell.get('denominator',0)!=0
               for kpis in row.get('month_targets',{}).values() for cell in kpis.values()):
            raise ValueError('Performance-only row received target measures')
    shown={(origin,name) for origin,names in manifest['salespeople_by_origin'].items() for name in names}
    if shown!=goals|performance:
        raise ValueError("Sales Target filter does not match goal and performance rosters")
    for key in ('current_target_roster_pairs','historical_target_roster_pairs','performance_only_roster_pairs'):
        if key in index and manifest.get(key)!=index[key]:
            raise ValueError('Index and manifest roster metadata differ')
    return goals,performance
