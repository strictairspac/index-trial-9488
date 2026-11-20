import time,logging,redis
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.identity import User,Audit
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.models.experiment import Experiment
from src.models.telemetry import Telemetry
from src.services import index_algorithm,comparison
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
CHANNEL='index-trials'

def _read(run_id):
 with Session() as s:
  row=s.get(Experiment,run_id)
  if not row:return None
  return {'id':row.id,'kind':row.kind or 'index','status':row.status,'attempt':row.attempt or 1,
   'snapshot':row.snapshot,'evidence':row.evidence or {},'result':row.result or {}}

def _save(run_id,evidence=None,status=None,result=None,error=None):
 with Session() as s:
  row=s.get(Experiment,run_id)
  if not row:return
  if evidence is not None:row.evidence=evidence
  if status is not None:row.status=status
  if result is not None:row.result=result
  if error is not None:row.error=error[:1000] if isinstance(error,str) else str(error)[:1000]
  s.commit()

def _run_comparison(run_id):
 state=_read(run_id)
 attempt=state['attempt'];snapshot=state['snapshot'];evidence=state['evidence']
 saved=evidence.get('snapshot') or {}
 checkpoints=list(evidence.get('checkpoints') or [])
 superseded=list(state['result'].get('superseded_attempts') or evidence.get('superseded_attempts') or [])
 resume=saved if saved.get('attempt')==attempt and saved.get('kind')=='comparison' and saved.get('pre') else None
 if not resume and saved:
  superseded.append({'attempt':saved.get('attempt'),'reason':'前一份证据无法核验，整对重测'})
 def checkpoint(stage,payload):
  checkpoints.append({'stage':stage,'at':int(time.time()),'attempt':attempt})
  _save(run_id,evidence={'snapshot':payload,'checkpoints':checkpoints,'superseded_attempts':superseded,'attempt':attempt})
  log('worker','experiment.checkpoint',f'{run_id}:{attempt}:{stage}')
 try:
  result=comparison.run(snapshot,run_id,resume=resume,attempt=attempt,checkpoint=checkpoint)
 except comparison.UnverifiableResume as exc:
  logging.warning('resume unverifiable %s: %s',run_id,exc)
  comparison.discard(run_id)
  superseded.append({'attempt':attempt,'reason':'前一份证据无法核验，整对重测','detail':str(exc)})
  checkpoints.append({'stage':'retest_pair','at':int(time.time()),'attempt':attempt})
  _save(run_id,evidence={'snapshot':None,'checkpoints':checkpoints,'superseded_attempts':superseded,'attempt':attempt})
  result=comparison.run(snapshot,run_id,resume=None,attempt=attempt,checkpoint=checkpoint)
  log('worker','experiment.retest_pair',f'{run_id}:{attempt}')
 result['superseded_attempts']=superseded
 result['attempt']=attempt
 checkpoints.append({'stage':'completed' if result.get('complete') else 'incomplete','at':int(time.time()),'attempt':attempt})
 return result,checkpoints

def _execute(run_id):
 state=_read(run_id)
 if not state or state['status']!='queued':return
 _save(run_id,status='running')
 try:
  if state['kind']=='comparison':
   result,checkpoints=_run_comparison(run_id)
   if result.get('complete'):comparison.discard(run_id)
   _save(run_id,evidence={'snapshot':result,'checkpoints':checkpoints,'superseded_attempts':result.get('superseded_attempts') or [],'attempt':state['attempt']},status='completed',result=result,error='')
  else:
   result=index_algorithm.run(state['snapshot'])
   _save(run_id,status='completed',result=result,error='')
  log('worker','experiment.completed',f'{run_id}:attempt{state["attempt"]}')
 except Exception as exc:
  logging.exception('trial failed')
  detail=str(exc)[:1000]
  title='对照结论未达成：' if isinstance(exc,comparison.UnverifiableResume) else ''
  evidence={'snapshot':None,'checkpoints':[],'attempt':state['attempt']}
  with Session() as s:
   row=s.get(Experiment,run_id)
   if row:
    evidence={'snapshot':(row.evidence or {}).get('snapshot'),'checkpoints':(row.evidence or {}).get('checkpoints') or [],'attempt':state['attempt']}
  _save(run_id,evidence=evidence,status='failed',error=title+detail)

def main():
 logging.basicConfig(level=logging.INFO)
 while True:
  Path('/tmp/worker-heartbeat').touch()
  try:
   item=q.blpop(CHANNEL,timeout=2)
   if item:_execute(int(item[1]))
  except Exception:logging.exception('worker loop');time.sleep(2)

if __name__=='__main__':main()
