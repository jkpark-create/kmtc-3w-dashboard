"""Immutable Sales Target generations with a checksum-verified last-write pointer."""
from pathlib import Path
import hashlib,json

def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def encoded(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':')).encode('utf-8')

def prepare_release(site):
    site=Path(site)
    documents={key:read(site/(key+'.json')) for key in ('index','manifest','base2025')}
    index=documents['index']; manifest=documents['manifest']
    if index.get('target_basis_policy')!='company_2025_actuals_official_office_attribution' or index.get('monthly_target_source_verification')!='confirmed':
        raise ValueError('Sales Target company-2025 source has not passed verification')
    date=index['data_date']
    if any(value.get('data_date')!=date for value in documents.values()):
        raise ValueError('Sales Target metadata dates differ')
    descriptors=manifest['chunks']+manifest.get('capacity_reserve_chunks',[])
    if len({d['file'] for d in descriptors})!=len(descriptors):
        raise ValueError('Duplicate Sales Target chunk reference')
    digest=hashlib.sha256(encoded(documents))
    for descriptor in descriptors:
        rel=Path(descriptor['file'])
        if rel.parent!=Path('data') or rel.name!=str(rel).replace('data/','').replace('data\\',''):
            raise ValueError('Invalid Sales Target chunk path')
        digest.update((site/rel).read_bytes())
    release_id=date+'-'+digest.hexdigest()[:16]
    for descriptor in descriptors:
        old=Path(descriptor['file']); new=old.with_name(release_id+'-'+old.name)
        (site/old).rename(site/new)
        descriptor['file']=new.as_posix()
    files={}; checksums={}
    for key,value in documents.items():
        value['release_id']=release_id
        payload=encoded(value); name=key+'.v'+release_id+'.json'
        (site/name).write_bytes(payload)
        (site/(key+'.json')).write_bytes(payload)
        files[key]=name
        checksums[name]=hashlib.md5(payload).hexdigest()
    for descriptor in descriptors:
        name=descriptor['file']; checksums[name]=hashlib.md5((site/name).read_bytes()).hexdigest()
    pointer={'_format':'sales-target-release-v1','release_id':release_id,'data_date':date,'files':files,'checksums':checksums}
    (site/'sales-target-release.json').write_bytes(encoded(pointer))
    return pointer

def release_entries(site):
    site=Path(site); index=read(site/'index.json')
    if index.get('target_basis_policy')!='company_2025_actuals_official_office_attribution':
        return None
    pointer=read(site/'sales-target-release.json')
    if pointer.get('_format')!='sales-target-release-v1':
        raise ValueError('Invalid Sales Target release pointer')
    documents={key:read(site/name) for key,name in pointer['files'].items()}
    if set(documents)!= {'index','manifest','base2025'} or any(d.get('release_id')!=pointer['release_id'] or d.get('data_date')!=pointer['data_date'] for d in documents.values()):
        raise ValueError('Sales Target release documents differ')
    if index['release_id']!=pointer['release_id'] or index['data_date']!=pointer['data_date']:
        raise ValueError('Sales Target canonical generation differs')
    manifest=documents['manifest']
    descriptors=manifest['chunks']+manifest.get('capacity_reserve_chunks',[])
    entries={'sales':[(site/name,name) for name in pointer['files'].values()],
             'sales_data':[(site/d['file'],Path(d['file']).name) for d in descriptors]}
    for group,pairs in entries.items():
        for path,name in pairs:
            expected=pointer['checksums'][('data/'+name) if group=='sales_data' else name]
            if hashlib.md5(path.read_bytes()).hexdigest()!=expected:
                raise ValueError('Sales Target release content changed: '+name)
    return entries

def publish_release_pointer(token,folder_id,site,list_files,sync_one,request,api):
    """Call only after ALL data/root uploads passed remote checksum verification."""
    site=Path(site); pointer=read(site/'sales-target-release.json')
    release_entries(site)  # reject local mutations between upload and pointer commit
    existing={item['name']:item for item in list_files(token,folder_id)}
    result=sync_one(token,folder_id=folder_id,path=site/'sales-target-release.json',remote_name='sales-target-release.json',existing=existing)
    actual=request(token,'GET',f"{api}/files/{result['id']}",label='verify Sales Target release pointer',params={'alt':'media'}).json()
    if actual!=pointer:
        raise ValueError('Published Sales Target pointer differs')
    return result

def verify_company_release(site,dataset):
    """Read final website files; modern source verification replaces legacy CSV audits."""
    import math
    from collections import defaultdict
    from sales_target_monthly import APPROVED_TARGET_BLANKS
    from sales_target_roster_contract import validate_rosters
    site=Path(site); entries=release_entries(site)
    if not entries:raise ValueError('Not a company-2025 release')
    pointer=read(site/'sales-target-release.json')
    index=read(site/pointer['files']['index']);manifest=read(site/pointer['files']['manifest'])
    base=read(site/pointer['files']['base2025']);affiliation=read(site/'office_affiliation.json')
    if any(str(value)!=str(dataset) for value in (index['data_date'],manifest['data_date'],base['data_date'],affiliation['dataset_id'])):
        raise ValueError('Sales Target source/date differs')
    if index.get('monthly_target_source_verification')!='confirmed' or index.get('official_roster_meta',{}).get('verification')!='confirmed':
        raise ValueError('Sales Target source verification incomplete')
    validate_rosters(index,manifest,{tuple(pair) for pair in affiliation['approved_pairs']})
    for row in index['rows']:
        if row.get('row_type')=='PERFORMANCE_ONLY':continue
        for key in ('booking','lifting','high_profit'):
            for quarter in ('q3','q4'):
                cell=row['kpi'][key][quarter];value=cell['target']
                if value is None:
                    if cell.get('basis_state') not in APPROVED_TARGET_BLANKS:raise ValueError('Unknown target gap')
                elif isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):
                    raise ValueError('Invalid target value')
    totals=defaultdict(float);booking_rows=0
    for path,_ in entries['sales_data']:
        chunk=read(path);totals[chunk['yyyymm']]+=chunk['totals'].get('allocated_bsa',0)
        booking_rows+=len(chunk.get('bookings',[]))
    for month,capacity in manifest['bsa_capacity_reconciliation'].items():
        if capacity['pending'] or not math.isclose(totals[month],capacity['raw'],abs_tol=1e-6):
            raise ValueError('Sales Target capacity reconciliation differs')
    if booking_rows!=manifest['bkg_rows']:raise ValueError('Sales Target booking count differs')
    return {'dataset':str(dataset),'chunks':len(entries['sales_data']),'booking_rows':booking_rows}
