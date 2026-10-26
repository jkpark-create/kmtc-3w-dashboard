"""Freeze BL-only quantities from verified same-generation monthly files."""
import datetime,gzip,hashlib,json,math,pathlib
from collections import defaultdict
from component_json_stream import Stream,read_contract
from independent_component_bsa import digest
def validate_bl(meta,months):
 day=meta.get('blSourceAsOfDate');v=meta.get('canonicalIdentifierValidation') or {}
 if any(str(x).upper() in ('BL','B/L','LIFTING') for x in meta.get('pendingSources',[])):raise ValueError('BL own source remains pending')
 if not isinstance(day,str) or len(day)!=8 or not day.isdigit() or not str(meta.get('canonicalBlLoadDtm','')).startswith(day):raise ValueError('BL own load date not verified')
 if meta.get('sourceUsable') is not True or meta.get('canonicalIdentifierSourceMode')!='icc_direct':raise ValueError('BL source mode unavailable')
 if v.get('passed') is not True or v.get('mismatchCount')!=0 or v.get('multiMonthIdentifierCount')!=0 or not v.get('blIdentifiers'):raise ValueError('BL identifier reconciliation failed')
 if meta.get('iccClosedWeekReconciliationApplied') is not True or meta.get('iccClosedWeekReconciliationSource')!='DM_SALES.DMSAB_102DS' or meta.get('iccClosedWeekReconciliationMaxAbsDiff')!=0:raise ValueError('BL closing reconciliation failed')
 if meta.get('singleIndividualRouteGrainReconciled') is not True or not set(months)<=set(meta.get('singleIndividualRouteGrainMonths') or []):raise ValueError('BL route grain coverage failed')
 if not meta.get('generatedAt'):raise ValueError('BL generation missing')
 return day
def stage(source,out):
 source=pathlib.Path(source);m,manifest_sha=read_contract(source/'manifest.json');meta=m['sourceMeta'];months=['202607','202608','202609','202610','202611','202612'];day=validate_bl(meta,months)
 if sum((source/m['monthFiles'][month]).stat().st_size for month in months)>256*1024**2:raise ValueError('BL compressed input exceeds 256MiB budget')
 fields=['blTeu','blCm1','blFreight','blCm1BasisTeu'];acc=defaultdict(lambda:[0.,0.,0.,0.]);proofs={};checks={}
 for month in months:
  relative=m['monthFiles'][month];p=(source/relative).resolve()
  if not p.is_relative_to(source.resolve()) or p.is_symlink():raise ValueError('Unsafe BL path')
  before=digest(p);sums=[0.,0.,0.,0.];count=0;observed=None
  with gzip.open(p,'rt',encoding='utf-8') as f:
   stream=Stream(f)
   for key in stream.members():
    if key=='sourceMeta':observed=stream.value()
    elif key=='weeklyLaneRows':
     for row in stream.items():
      if any(row.get(k) is None for k in fields):raise ValueError('Unknown BL quantity may not become zero')
      values=[float(row[k]) for k in fields]
      if not all(math.isfinite(v) for v in values) or values[0]<0 or values[3]<0:raise ValueError('Invalid BL quantities')
      sums=[a+b for a,b in zip(sums,values)]
      if not any(values):continue
      mo=str(row.get('month') or '');week=str(row.get('week') or '');ww=week[len(mo):] if week.startswith(mo) else ''
      if mo!=month or not ww.isdigit() or not 1<=int(ww)<=53:raise ValueError('BL period key missing')
      k=(str(row.get('dly') or ''),str(row.get('por') or ''),str(int(ww)),mo,str(row.get('dest') or ''),str(row.get('origin') or ''),str(row.get('team') or ''))
      if not all(k) or k[6] not in ('OBT','EST','IST','JBT'):raise ValueError('BL dimensional key missing')
      acc[k]=[a+b for a,b in zip(acc[k],values)];count+=1
      if len(acc)>200000:raise ValueError('BL result exceeds bounded component row budget')
    else:stream.skip()
  # Only the manifest carries the all-month coverage list. Each member carries
  # its own reconciled flag; its period is checked on every row above.
  if not observed or observed.get('generatedAt')!=meta['generatedAt'] or validate_bl(observed,[])!=day or digest(p)!=before:raise ValueError('Mixed BL generation')
  expected=[float(m['kpiByMonth'][month][k]) for k in fields]
  if any(abs(a-b)>.01 for a,b in zip(sums,expected)):raise ValueError('BL monthly KPI reconciliation mismatch')
  proofs[relative]={'sha256':before,'bytes':p.stat().st_size};checks[month]={'sourceRows':count,'teu':sums[0],'manifestTeu':expected[0],'cm1':sums[1],'manifestCm1':expected[1],'freight':sums[2],'manifestFreight':expected[2],'cm1BasisTeu':sums[3],'manifestCm1BasisTeu':expected[3]}
  print('Verified independent BL '+month+' '+str(sums[0])+' TEU',flush=True)
 if digest(source/'manifest.json')!=manifest_sha or any(digest(source/n)!=v['sha256'] for n,v in proofs.items()):raise ValueError('BL producer changed during freeze')
 result={'format':'independent-bl-v1','sourceDate':day,'generatedAt':meta['generatedAt'],'verifiedAt':datetime.datetime.now().astimezone().isoformat(),'scopeMonths':months,'manifestSha256':manifest_sha,'sourceFiles':proofs,'monthlyChecks':checks,'bookingRequired':False,'table':{'c':['DLY_PORT','POR_PORT','WW','YYYYMM','dest','origin','team','blTeu','blCm1','blFreight','blCm1BasisTeu'],'r':[list(k)+v for k,v in sorted(acc.items())]}}
 from independent_bl_detail import attach
 attach(source,result)
 encoded=json.dumps(result,ensure_ascii=False,separators=(',',':'))
 if len(encoded.encode('utf-8'))>16*1024**2:raise ValueError('BL result exceeds 16MiB staging budget')
 pathlib.Path(out).write_text(encoded,encoding='utf-8');return result
