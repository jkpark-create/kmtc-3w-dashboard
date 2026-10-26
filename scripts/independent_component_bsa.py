"""Read only completed BSA; preserve all Booking-derived published measures."""
import datetime,gzip,hashlib,json,math,pathlib
from collections import defaultdict
from component_json_stream import Stream,read_contract

def digest(path):
 with pathlib.Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def validate_bsa(meta,months):
 day=meta.get('bsaSourceAsOfDate'); proof=meta.get('bsaRenderConsistency') or {}
 if any(str(v).upper()=='BSA' for v in meta.get('pendingSources',[])):raise ValueError('BSA own source remains pending')
 if not isinstance(day,str) or len(day)!=8 or not day.isdigit():raise ValueError('BSA own source date missing')
 if meta.get('sourceUsable') is not True or meta.get('sandboxBsaSourceFresh') is not True:raise ValueError('BSA own freshness unavailable')
 if proof.get('passed') is not True or not set(months)<=set(proof.get('months') or []):raise ValueError('BSA reconciliation coverage missing')
 if any(not isinstance(proof.get(k),(int,float)) or abs(proof[k])>0.0011 for k in ('maxExactDiff','maxSourceTotalDiff')):raise ValueError('BSA reconciliation failed')
 if meta.get('bsaSourceModeResolved')!='sandbox_only':raise ValueError('Unsupported BSA source mode')
 if not meta.get('generatedAt') or not meta.get('bsaScope'):raise ValueError('BSA generation/scope missing')
 return day

def stage(source,out):
 source=pathlib.Path(source);out=pathlib.Path(out)
 manifest,manifest_sha=read_contract(source/'manifest.json');meta=manifest['sourceMeta']
 months=sorted(k for k in manifest['monthFiles'] if k>='202607' and k<='202612')
 if len(months)!=6:raise ValueError('Required July-December BSA missing')
 if sum((source/manifest['monthFiles'][month]).stat().st_size for month in months)>256*1024**2:raise ValueError('BSA compressed input exceeds 256MiB budget')
 day=validate_bsa(meta,months); totals={};proofs={};acc=defaultdict(float)
 for month in months:
  relative=manifest['monthFiles'][month];p=(source/relative).resolve()
  if not p.is_relative_to(source.resolve()) or p.is_symlink():raise ValueError('Unsafe source path')
  before=digest(p); observed_meta=None;rows=0;total=0.0
  with gzip.open(p,'rt',encoding='utf-8') as f:
   s=Stream(f)
   for key in s.members():
    if key=='sourceMeta':observed_meta=s.value()
    elif key=='weeklyLaneRows':
     for row in s.items():
      value=row.get('bsaTeu')
      if value is None:raise ValueError('Unknown BSA may not become zero')
      value=float(value)
      if not math.isfinite(value) or value<0:raise ValueError('Invalid BSA quantity')
      if not value:continue
      m=str(row.get('month') or '');w=str(row.get('week') or '');ww=w[len(m):] if w.startswith(m) else ''
      team=str(row.get('team') or '').strip().upper();origin=str(row.get('origin') or '').strip();dest=str(row.get('dest') or '').strip()
      if m!=month or not ww.isdigit() or not 1<=int(ww)<=53:raise ValueError('BSA period key invalid')
      if team not in ('OBT','EST','IST','JBT'):raise ValueError('Unrecognized BSA team '+team)
      if not origin or not dest or len(origin)>3 or len(dest)>3:raise ValueError('Invalid BSA country key')
      k=(str(row.get('dly') or '').strip(),str(row.get('por') or '').strip(),str(int(ww)),m,dest,origin,team)
      if not k[0] or not k[1]:raise ValueError('BSA port key missing')
      acc[k]+=value;total+=value;rows+=1
      if len(acc)>200000:raise ValueError('BSA result exceeds bounded component row budget')
    else:s.skip()
  if not observed_meta or observed_meta.get('generatedAt')!=meta['generatedAt'] or validate_bsa(observed_meta,months)!=day:raise ValueError('Mixed source generation')
  if digest(p)!=before:raise ValueError('Source changed during read')
  expected=float(manifest['kpiByMonth'][month]['bsaTeu'])
  if abs(total-expected)>0.01:raise ValueError(f'BSA monthly total mismatch {month}: {total} != {expected}')
  proofs[relative]={'sha256':before,'bytes':p.stat().st_size};totals[month]={'sourceRows':rows,'teu':round(total,6),'manifestTeu':expected}
  print('Verified independent BSA '+month+' '+str(round(total,3))+' TEU',flush=True)
 if digest(source/'manifest.json')!=manifest_sha or any(digest(source/n)!=v['sha256'] for n,v in proofs.items()):raise ValueError('Producer generation changed; stage rejected')
 records=[list(k)+( [v] ) for k,v in sorted(acc.items())]
 table={'c':['DLY_PORT','POR_PORT','WW','YYYYMM','dest','origin','team','teu_bsa'],'r':records}
 result={'format':'independent-bsa-v1','sourceDate':day,'generatedAt':meta['generatedAt'],'verifiedAt':datetime.datetime.now().astimezone().isoformat(),'scopeMonths':months,'manifestSha256':manifest_sha,'sourceFiles':proofs,'monthlyChecks':totals,'bookingRequired':False,'table':table}
 encoded=json.dumps(result,ensure_ascii=False,separators=(',',':'))
 if len(encoded.encode('utf-8'))>16*1024**2:raise ValueError('BSA result exceeds 16MiB staging budget')
 out.write_text(encoded,encoding='utf-8')
 return result

