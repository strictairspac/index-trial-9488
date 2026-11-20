"""对照试验的回归检查，运行在真实 MySQL 上。

用法（在 backend/ 下，已设置 DATABASE_URL / REDIS_URL / JWT_SECRET）：
  python3 -m test-artifacts.gb-4102.test_comparison

覆盖：确定性样本、样本必须落在负载条件内、两侧索引一致、对账门禁、
断点续跑，以及「重采样已生效但成绩未保存 → 整对重测」。
"""
import sys,json
from sqlalchemy import text
from src.config.database import Session,engine
from src.models.experiment import Experiment
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.services import comparison
from src.utils.serialization import record

FAILS=[]
TOTAL=[]

def check(name,cond,detail=''):
 TOTAL.append(name)
 print(('PASS ' if cond else 'FAIL ')+name+(' :: '+str(detail) if detail else ''))
 if not cond:FAILS.append(name)

def new_experiment(name,workload,candidate,skew=1500):
 snapshot={'workload':record(workload),'candidate':record(candidate),
  'comparison':{'skew_rows':skew,'focus_sensor_ids':[workload.sensor_id]}}
 with Session() as s:
  row=Experiment(name=name,workload_id=workload.id,candidate_id=candidate.id,kind='comparison',attempt=1,status='running',snapshot=snapshot)
  s.add(row);s.commit();return row.id,snapshot

def main():
 with Session() as s:
  stale=[r.id for r in s.query(Experiment).filter(Experiment.name.like('reg-%')).all()]
  for row in s.query(Experiment).filter(Experiment.name.like('reg-%')).all():s.delete(row)
  s.commit()
 for eid in stale:comparison.discard(eid)
 with Session() as s:
  workload=s.query(Workload).filter_by(name='泵站17温升排查').one()
  candidate=s.query(Candidate).filter_by(workload_id=workload.id).first()
  wid,wed=workload.id,record(workload);cid,ced=candidate.id,record(candidate)

 # 1) 确定性样本：同参数两次生成完全一致
 a,_=comparison.skew_sample(wed,200,[17],1)
 b,_=comparison.skew_sample(wed,200,[17],1)
 check('样本确定性',comparison.canonical(a)==comparison.canonical(b))

 # 2) 样本必须全部落在负载条件内；聚焦到不属于负载的传感器要被拒绝
 eid,snap=new_experiment('reg-badfocus',workload,candidate,skew=100)
 snap['comparison']['focus_sensor_ids']=[99]
 with Session() as s:
  s.get(Experiment,eid).snapshot=snap;s.commit()
 try:
  comparison.run(snap,eid,attempt=1);check('非法聚焦传感器被拒绝',False)
 except ValueError as exc:
  check('非法聚焦传感器被拒绝','负载条件' in str(exc))
 finally:comparison.discard(eid)

 # 3) 正常对照：对账一致、索引一致、统计隔离、重采样被观察到、能出结论
 eid,snap=new_experiment('reg-basic',workload,candidate)
 try:
  r=comparison.run(snap,eid,attempt=1)
  check('对照完成',r['complete'] is True)
  check('两侧结果对账一致',r['reconciled'] is True)
  check('两侧索引结构一致',r['verification']['same_index'] is True)
  check('自动采样已隔离',r['verification']['auto_recalc_isolated'] is True)
  check('重采样被观察到',r['verification']['resample_observed'] is True)
  check('重采样前查到新增遥测',r['verification']['new_telemetry_visible'] is True)
  check('结论门禁通过',r['conclusion_ready'] is True and bool(r['conclusion']))
  check('结论描述两侧访问方式','重新采样前' in r['conclusion'] and '重新采样后' in r['conclusion'])
 finally:comparison.discard(eid)

 # 4) 断点续跑：在 pre_measured 后中断，留存证据可核验，续跑完成
 eid,snap=new_experiment('reg-resume',workload,candidate)
 captured={}
 def cp(stage,payload):
  captured[stage]=payload
  if stage=='pre_measured':raise KeyboardInterrupt()
 try:comparison.run(snap,eid,attempt=1,checkpoint=cp)
 except KeyboardInterrupt:pass
 partial=captured.get('pre_measured')
 try:
  r=comparison.run(snap,eid,resume=partial,attempt=1)
  check('断点续跑完成',r['complete'] is True and r['verification']['resumed'] is True)
  check('续跑后对账一致',r['reconciled'] is True)
 finally:comparison.discard(eid)

 # 5) 重新采样已生效但成绩未保存：续跑核验失败，整对重测
 eid,snap=new_experiment('reg-retest',workload,candidate)
 captured={}
 def cp2(stage,payload):
  captured[stage]=payload
  if stage=='pre_measured':raise KeyboardInterrupt()
 try:comparison.run(snap,eid,attempt=1,checkpoint=cp2)
 except KeyboardInterrupt:pass
 partial=captured.get('pre_measured')
 with engine.connect() as conn:
  conn.execute(text('ANALYZE TABLE '+comparison.table_name(eid)));conn.commit()
 try:
  comparison.run(snap,eid,resume=partial,attempt=1)
  check('重采样已生效时拒绝续跑',False)
 except comparison.UnverifiableResume:
  check('重采样已生效时拒绝续跑',True)
 comparison.discard(eid)
 r=comparison.run(snap,eid,resume=None,attempt=1)
 try:
  check('整对重测得到完整结论',r['complete'] is True and r['reconciled'] is True)
  check('重测不是续跑',r['verification']['resumed'] is False)
 finally:comparison.discard(eid)

 print('\n%d checks, %d failed'%(len(TOTAL),len(FAILS)))
 return 1 if FAILS else 0

if __name__=='__main__':sys.exit(main())
