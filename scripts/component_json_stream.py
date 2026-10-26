"""Small read-only JSON framing helper; bounded element decoding."""
import hashlib,json,pathlib

def file_digest(path):
 with pathlib.Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()

def read_contract(path):
 """Keep only independent-source contract fields; stream unrelated arrays.

 The full-file SHA remains part of the proof. The 4MiB bound applies to the
 retained contract, rather than to unrelated ROB/inventory payload bytes.
 """
 path=pathlib.Path(path);before=file_digest(path);value={};retained=0
 with path.open('r',encoding='utf-8-sig') as reader:
  stream=Stream(reader)
  for key in stream.members():
   if key not in ('sourceMeta','monthFiles','kpiByMonth','detailFiles'):stream.skip();continue
   if key in value:raise ValueError('Duplicate independent contract field: '+key)
   item=stream.value()
   if not isinstance(item,dict):raise ValueError('Invalid independent contract field: '+key)
   retained+=len(json.dumps(item,ensure_ascii=False,separators=(',',':')).encode('utf-8'))
   if retained>4*1024**2:raise ValueError('Retained independent contract exceeds 4MiB budget')
   value[key]=item
  stream.finish()
 if not set(('sourceMeta','monthFiles','kpiByMonth'))<=set(value):raise ValueError('Independent manifest contract is incomplete')
 if file_digest(path)!=before:raise ValueError('Manifest changed during independent contract read')
 return value,before

class Stream:
 def __init__(self,reader,chunk=65536):self.reader=reader;self.chunk=chunk;self.buf='';self.pos=0;self.end=False;self.decoder=json.JSONDecoder()
 def fill(self):
  if self.pos:self.buf=self.buf[self.pos:];self.pos=0
  text=self.reader.read(self.chunk)
  if not text:self.end=True
  self.buf+=text
 def peek(self):
  while True:
   while self.pos<len(self.buf) and self.buf[self.pos].isspace():self.pos+=1
   if self.pos<len(self.buf):return self.buf[self.pos]
   if self.end:raise ValueError('Unexpected JSON EOF')
   self.fill()
 def take(self,char):
  if self.peek()!=char:raise ValueError('Unexpected JSON delimiter')
  self.pos+=1
 def value(self):
  self.peek()
  while True:
   try:
    value,end=self.decoder.raw_decode(self.buf,self.pos)
    if not self.end and (end==len(self.buf) or self.buf[end] not in ' \t\r\n,:]}'):self.fill();continue
    self.pos=end;return value
   except json.JSONDecodeError:
    if self.end:raise
    self.fill()
    if len(self.buf)>16*1024**2:raise ValueError('Single JSON element exceeds read-validation budget')
 def finish(self):
  while True:
   if self.buf[self.pos:].strip():raise ValueError('Trailing data after JSON document')
   if self.end:return
   self.pos=len(self.buf);self.fill()
 def skip(self,depth=0):
  if depth>256:raise ValueError('JSON nesting exceeds read-validation budget')
  first=self.peek()
  if first=='{':
   for key in self.members():self.skip(depth+1)
  elif first=='[':
   self.take('[')
   if self.peek()==']':self.pos+=1;return
   while True:
    self.skip(depth+1)
    c=self.peek();self.pos+=1
    if c==']':return
    if c!=',':raise ValueError('Unexpected array separator')
  else:self.value()
 def members(self):
  self.take('{')
  if self.peek()=='}':self.pos+=1;return
  while True:
   key=self.value()
   if not isinstance(key,str):raise ValueError('JSON object key is not text')
   self.take(':');yield key
   c=self.peek();self.pos+=1
   if c=='}':return
   if c!=',':raise ValueError('Unexpected object separator')
 def items(self):
  self.take('[')
  if self.peek()==']':self.pos+=1;return
  while True:
   yield self.value()
   c=self.peek();self.pos+=1
   if c==']':return
   if c!=',':raise ValueError('Unexpected array separator')
