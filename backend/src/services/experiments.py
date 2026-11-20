import redis
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.models.experiment import Experiment
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
CHANNEL='index-trials'

def _detail(row):
 data=record(row)
 result=row.result or {}
 evidence=row.evidence or {}
 data['kind']=row.kind or 'index'
 data['attempt']=row.attempt or 1
 data['conclusion']=result.get('conclusion')
 data['conclusion_ready']=result.get('conclusion_ready')
 data['verdict']=result.get('verdict')
 data['reasons']=result.get('reasons') or []
 data['superseded_attempts']=result.get('superseded_attempts') or evidence.get('superseded_attempts') or []
 data['checkpoints']=evidence.get('checkpoints') or []
 data['has_partial_evidence']=bool(evidence.get('snapshot'))
 return data

def list_items():
 with Session() as s:return [_detail(r) for r in s.query(Experiment).order_by(Experiment.id.desc()).limit(100).all()]

def detail(item_id):
 with Session() as s:
  row=s.get(Experiment,item_id)
  if not row:raise HTTPException(404,'未找到试验')
  return _detail(row)

def _enqueue(item_id):
 try:queue.rpush(CHANNEL,item_id)
 except redis.RedisError:
  with Session() as s:
   r=s.get(Experiment,item_id)
   if r:r.status='failed';r.error='队列不可用，请稍后重试或续跑';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试或续跑')

def create(data):
 with Session() as s:
  c=s.get(Candidate,data.candidate_id)
  if not c:raise HTTPException(404,'未找到候选')
  w=s.get(Workload,c.workload_id)
  snapshot={'workload':record(w),'candidate':record(c)}
  if data.kind=='comparison':
   snapshot['comparison']={'skew_rows':data.comparison.skew_rows,'focus_sensor_ids':data.comparison.focus_sensor_ids}
  row=Experiment(name=data.name,candidate_id=c.id,workload_id=w.id,kind=data.kind,attempt=1,status='queued',snapshot=snapshot)
  s.add(row);s.commit();s.refresh(row);result_id=row.id
 _enqueue(result_id)
 with Session() as s:return _detail(s.get(Experiment,result_id))

def resume(item_id):
 """人工续跑：把中断/失败的对照试验重新入队，沿用同一次尝试的已保存证据。
 worker 先核验旧证据；核验不过（例如重新采样已生效但成绩未保存）就整对重测。"""
 with Session() as s:
  row=s.get(Experiment,item_id)
  if not row:raise HTTPException(404,'未找到试验')
  if (row.kind or 'index')!='comparison':raise HTTPException(400,'只有对照试验支持续跑')
  if row.status not in ('running','failed'):raise HTTPException(409,'当前状态不可续跑')
  row.status='queued';row.error=None;s.commit()
 _enqueue(item_id)
 with Session() as s:return _detail(s.get(Experiment,item_id))
