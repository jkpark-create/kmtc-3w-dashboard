"""Future 3W preflight: wait before holding the output/publication file gate."""
import argparse
import ctypes
import datetime
import hashlib
import json
import time
import os
import sys
from pathlib import Path


def publication_gate(root):
    """Probe the existing normal gate; close only our own handle before waiting."""
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.CreateFileW.argtypes=[ctypes.c_wchar_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p]
    kernel.CreateFileW.restype=ctypes.c_void_p
    kernel.CloseHandle.argtypes=[ctypes.c_void_p]
    handle=kernel.CreateFileW(str(Path(root)/'locks/refresh-deploy.lock'),0x80000000,0,None,3,0x80,None)
    error=ctypes.get_last_error()
    if handle==ctypes.c_void_p(-1).value:
        return {'ready':False,'reason':'publication_gate_busy' if error in (32,33) else 'publication_gate_unavailable',
                'winError':error,'terminal':error not in (2,3,32,33)}
    kernel.CloseHandle(handle)
    return None

def inspect_publication(root, expected, *, gate_probe=None):
    if os.environ.get('KMTC_THREEW_SOURCE_PIN'):
        from published_source_package import pinned
        try:return {'ready':True,'reason':'published_immutable_source_ready',**pinned(expected)}
        except (OSError,ValueError) as error:return {'ready':False,'reason':str(error)}
    root=Path(root)
    probe=publication_gate if gate_probe is None else gate_probe
    gate=probe(root)
    if gate:return gate
    try:
        raw=(root/'oracle-dashboard/manifest.json').read_bytes()
        meta=json.loads(raw)['sourceMeta']
        success=json.loads((root/'integrated-dashboard-last-success.json').read_text(encoding='utf-8-sig'))
        if meta.get('sourceAsOfDate')!=expected:
            return {'ready':False,'reason':'source_date_pending','actualSourceDate':meta.get('sourceAsOfDate')}
        if success.get('status')!='success' or success.get('productionVerified') is not True or success.get('outcome') not in {'updated','already_current'}:
            return {'ready':False,'reason':'publication_success_pending'}
        if success.get('sourceAsOfDate')!=expected or not meta.get('generatedAt') or success.get('generatedAt')!=meta['generatedAt']:
            return {'ready':False,'reason':'generation_identity_pending'}
        if success.get('manifestSha256')!=hashlib.sha256(raw).hexdigest():
            return {'ready':False,'reason':'published_hash_pending'}
        if meta.get('refreshState') in {'partial','pending'} or meta.get('pendingSources') or meta.get('sourceUsable') is False:
            return {'ready':False,'reason':'complete_source_pending'}
        latest_path=root/'integrated-dashboard-last-run.json'
        if latest_path.exists():
            latest=json.loads(latest_path.read_text(encoding='utf-8-sig'))
            if latest.get('status')=='failed' and latest.get('completedAt','')>=success.get('completedAt',''):
                return {'ready':False,'reason':'newer_integrated_failure_pending'}
        gate=probe(root)
        if gate:return gate
        return {'ready':True,'reason':'published_source_ready','sourceDate':expected}
    except PermissionError as error:
        return {'ready':False,'reason':'publication_receipt_access_denied','terminal':True,'winError':getattr(error,'winerror',None) or 5}
    except (OSError,KeyError,ValueError,TypeError):
        return {'ready':False,'reason':'publication_receipt_pending'}

def wait_for_publication(inspect, wait_seconds, poll_seconds, *, clock=time.monotonic,
                         sleep=time.sleep, report=print):
    """Retry readiness without holding a producer or consumer output lock."""
    end = clock() + wait_seconds
    while True:
        result = inspect()
        report(json.dumps(result), flush=True)
        if result['ready']:
            return 0
        if result.get('terminal'):
            return 77 if result.get('winError') == 5 else 78
        remaining = end - clock()
        if remaining <= 0:
            return 3
        sleep(min(poll_seconds, remaining))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    parser.add_argument('--consumer-runtime')
    parser.add_argument('--source-date',required=True)
    parser.add_argument('--wait-seconds',type=int,default=43200)
    parser.add_argument('--poll-seconds',type=int,default=60)
    args=parser.parse_args()
    datetime.datetime.strptime(args.source_date,'%Y%m%d')
    if args.wait_seconds<0 or args.poll_seconds<=0:raise ValueError('Invalid wait bounds')
    if args.consumer_runtime:
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
        from published_source_package import prepare
        def inspect():
            try:
                result=prepare(args.root,args.consumer_runtime,args.source_date,os.environ['KMTC_THREEW_SOURCE_PIN'])
                return {'ready':True,'reason':'published_immutable_source_ready',**result}
            except PermissionError as error:
                return {'ready':False,'reason':str(error),'terminal':True,'winError':getattr(error,'winerror',None) or 5}
            except (OSError,ValueError,KeyError,RuntimeError) as error:
                return {'ready':False,'reason':str(error),'expectedSourceDate':args.source_date,
                        'writerLockBypassed':False}
    else:
        inspect=lambda:inspect_publication(args.root,args.source_date)
    return wait_for_publication(inspect,args.wait_seconds,args.poll_seconds)
if __name__=='__main__':raise SystemExit(main())
