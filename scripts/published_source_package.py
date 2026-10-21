"""Published immutable source packages for 3W. Never runs or writes Integrated."""
import datetime,gzip,hashlib,json,os,pathlib,shutil,time,urllib.request,uuid
FORMAT='threew-published-source-v1'
INDEX_URL='https://jkpark-create.github.io/kmtc-weekly-bsa-review/protected-assets.json'
PREFIX='data/oracle-dashboard/'
def sha(b):return hashlib.sha256(b).hexdigest()
def canonical(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
def atomic(path,value):
 path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True);temp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
 with temp.open('xb') as f:f.write(canonical(value));f.flush();os.fsync(f.fileno())
 os.replace(temp,path)
def relative(value):
 p=pathlib.PurePosixPath(value)
 if not value or '\\' in value or p.is_absolute() or '..' in p.parts or ':' in value:raise ValueError('Unsafe source member')
 return str(p)
def required(manifest,year):
 names={'manifest.json'}
 for month,name in (manifest.get('monthFiles') or {}).items():
  if str(month).startswith(str(year)) and month>=str(year)+'07':
   names.add(relative(name));detail=(manifest.get('detailFiles') or {}).get(month)
   if not detail:raise ValueError('Required canonical detail missing: '+month)
   names.add(relative(detail))
 if len(names)<3:raise ValueError('Required month/detail members missing')
 return names
def source_identity(success,raw,expected):
 m=json.loads(raw);meta=m.get('sourceMeta') or {}
 if (success.get('status')!='success' or success.get('productionVerified') is not True or success.get('outcome') not in ('updated','already_current') or success.get('manifestSha256')!=sha(raw) or not meta.get('generatedAt') or success.get('generatedAt')!=meta['generatedAt'] or success.get('sourceAsOfDate')!=meta.get('sourceAsOfDate') or meta.get('refreshState')!='complete' or meta.get('pendingSources') or meta.get('sourceUsable') is False):raise ValueError('Complete published generation identity unavailable')
 if meta.get('sourceAsOfDate')!=expected:raise ValueError('source_date_pending: expected='+expected+', published='+str(meta.get('sourceAsOfDate')))
 return {'sourceDate':expected,'manifestSha256':sha(raw),'generatedAt':meta['generatedAt']}
def check_entry(body,entry):
 if len(body)!=int(entry['size']) or hashlib.md5(body).hexdigest()!=entry['md5']:raise ValueError('Published source body hash mismatch')
def resolve_member(name,index):
 for path in [name] if name.endswith('.gz') else [name,name+'.gz']:
  entry=index['files'].get(PREFIX+path)
  if entry:return path,entry
 raise ValueError('Required source not in published index: '+name)
def validate(package,expected,year=2026):
 package=pathlib.Path(package).resolve();cert=json.loads((package/'source-package.json').read_text(encoding='utf-8'));descriptor={k:v for k,v in cert.items() if k!='generationId'}
 if cert.get('format')!=FORMAT or cert.get('generationId')!=sha(canonical(descriptor)) or package.name!=cert['generationId']:raise ValueError('Source package generation ID mismatch')
 idxraw=(package/'published-index.json').read_bytes();successraw=(package/'publication-success.json').read_bytes()
 if sha(idxraw)!=cert['publishedIndexSha256'] or sha(successraw)!=cert['publicationSuccessSha256']:raise ValueError('Publication evidence changed')
 idx=json.loads(idxraw);success=json.loads(successraw);raw=(package/'oracle-dashboard/manifest.json').read_bytes();identity=source_identity(success,raw,expected)
 if identity!=cert['source'] or year!=cert['datasetYear']:raise ValueError('Source package identity/year mismatch')
 manifest=json.loads(raw);expected_names={resolve_member(n,idx)[0] for n in required(manifest,year)}
 if set(cert['files'])!=expected_names:raise ValueError('Missing or extra required source members')
 for name,proof in cert['files'].items():
  name=relative(name);p=package/'oracle-dashboard'/name
  if p.is_symlink() or not p.resolve().is_relative_to(package):raise ValueError('Unsafe source path')
  body=p.read_bytes();entry=idx['files'][PREFIX+name];check_entry(body,entry)
  if sha(body)!=proof['sha256'] or len(body)!=proof['size']:raise ValueError('Pinned source member changed: '+name)
 return {**identity,'generationId':cert['generationId'],'packageRoot':str(package)}
def from_pin(pin,expected,year=2026):
 pin=pathlib.Path(pin);value=json.loads(pin.read_text(encoding='utf-8'));result=validate(value['packageRoot'],expected,year)
 if value!=result:raise ValueError('Analysis pin identity changed')
 return result
_PINNED={}
_CERTS={}
def pinned(expected,year=2026):
 pin=os.environ.get('KMTC_THREEW_SOURCE_PIN','')
 if not pin:raise ValueError('Immutable consumer source pin is required')
 key=(str(pathlib.Path(pin).resolve()),expected,year)
 if key in _PINNED:
  if json.loads(pathlib.Path(pin).read_text(encoding='utf-8'))!=_PINNED[key]:raise ValueError('Analysis source generation changed')
  return _PINNED[key]
 result=from_pin(pin,expected,year);_PINNED[key]=result
 _CERTS[result['packageRoot']]=json.loads((pathlib.Path(result['packageRoot'])/'source-package.json').read_text(encoding='utf-8'))
 return result

def read_pinned_json(path):
 path=pathlib.Path(path);pin=os.environ.get('KMTC_THREEW_SOURCE_PIN','');pin_key=str(pathlib.Path(pin).resolve());values=[v for k,v in _PINNED.items() if k[0]==pin_key]
 if not values:raise ValueError('Read requires a verified analysis pin')
 value=json.loads(pathlib.Path(pin).read_text(encoding='utf-8'))
 if any(v!=value for v in values):raise ValueError('Analysis source generation changed')
 root=pathlib.Path(value['packageRoot']);cert=_CERTS[str(root)];name=path.resolve().relative_to((root/'oracle-dashboard').resolve()).as_posix();body=path.read_bytes();proof=cert['files'].get(name)
 if not proof or sha(body)!=proof['sha256'] or len(body)!=proof['size']:raise ValueError('Source bytes differ from analysis pin')
 return json.loads(gzip.decompress(body) if path.suffix=='.gz' else body)

def prepare(integrated,runtime,expected,pin,*,year=2026,index_reader=None,member_reader=None,report=print):
 integrated=pathlib.Path(integrated);runtime=pathlib.Path(runtime);pin=pathlib.Path(pin);deadline=time.monotonic()+180
 def download(url,headers=None):
  remaining=deadline-time.monotonic()
  if remaining<=0:raise TimeoutError('Source preparation time budget exhausted')
  with urllib.request.urlopen(urllib.request.Request(url,headers=headers or {}),timeout=min(45,remaining)) as r:return r.read()
 successraw=(integrated/'integrated-dashboard-last-success.json').read_bytes();success=json.loads(successraw)
 if success.get('sourceAsOfDate')!=expected:raise ValueError('source_date_pending: expected='+expected+', published='+str(success.get('sourceAsOfDate')))
 if index_reader is None:index_reader=lambda:download(INDEX_URL)
 indexraw=index_reader();idx=json.loads(indexraw)
 if idx.get('driveFolderId')!='1lccizv8st6zJ-1mFSAvHZlx5kF8dYH7r':raise ValueError('Unexpected published source store')
 # Reuse a complete matching consumer checkpoint; no source-body download replay.
 folder=runtime/'source-packages'
 if folder.exists():
  for prior in sorted(folder.iterdir()):
   if not prior.is_dir() or len(prior.name)!=64:continue
   try:cert=json.loads((prior/'source-package.json').read_text(encoding='utf-8'))
   except (OSError,ValueError):continue
   if cert.get('publishedIndexSha256')!=sha(indexraw) or cert.get('publicationSuccessSha256')!=sha(successraw) or cert.get('source',{}).get('sourceDate')!=expected:continue
   result=validate(prior,expected,year)
   if pin.exists():
    if from_pin(pin,expected,year)!=result:raise ValueError('An analysis pin cannot change generations')
   else:atomic(pin,result)
   report('Reusing fully verified consumer source package '+prior.name)
   return result
 token=[]
 def member(name,entry):
  if member_reader is not None:return member_reader(name,entry)
  local=integrated/'oracle-dashboard'/name
  if local.is_file():
   b=local.read_bytes()
   if len(b)==int(entry['size']) and hashlib.md5(b).hexdigest()==entry['md5']:return b
  if not token:
   from scripts.sync_dashboard_data_to_gdrive import refresh_access_token
   token.append(refresh_access_token())
  return download('https://www.googleapis.com/drive/v3/files/'+entry['fileId']+'?alt=media',{'Authorization':'Bearer '+token[0]})
 name,entry=resolve_member('manifest.json',idx);raw=member(name,entry);check_entry(raw,entry);identity=source_identity(success,raw,expected);manifest=json.loads(raw)
 members={resolve_member(n,idx)[0]:resolve_member(n,idx)[1] for n in required(manifest,year)}
 if sum(int(e['size']) for e in members.values())>2*1024**3:raise ValueError('Source package exceeds bounded 2GiB preparation budget')
 folder=runtime/'source-packages';folder.mkdir(parents=True,exist_ok=True);stage=folder/('preparing-'+uuid.uuid4().hex);stage.mkdir();(stage/'oracle-dashboard').mkdir();proofs={}
 try:
  for name,entry in sorted(members.items()):
   body=raw if name=='manifest.json' else member(name,entry);check_entry(body,entry);path=stage/'oracle-dashboard'/relative(name);path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body);proofs[name]={'sha256':sha(body),'size':len(body)};report('Verified source member '+name)
  if index_reader()!=indexraw or (integrated/'integrated-dashboard-last-success.json').read_bytes()!=successraw:raise ValueError('Publication generation changed during source preparation')
  descriptor={'format':FORMAT,'datasetYear':year,'source':identity,'files':proofs,'publishedIndexSha256':sha(indexraw),'publicationSuccessSha256':sha(successraw)};gid=sha(canonical(descriptor));(stage/'published-index.json').write_bytes(indexraw);(stage/'publication-success.json').write_bytes(successraw);atomic(stage/'source-package.json',{**descriptor,'generationId':gid});target=folder/gid
  if target.exists():
   validate(target,expected,year);shutil.rmtree(stage)
  else:os.replace(stage,target)
  result=validate(target,expected,year)
  if pin.exists():
   if from_pin(pin,expected,year)!=result:raise ValueError('An analysis pin cannot change generations')
  else:atomic(pin,result)
  return result
 except BaseException:
  # Preserve partial preparation for diagnosis; never publish a pin to it.
  raise
