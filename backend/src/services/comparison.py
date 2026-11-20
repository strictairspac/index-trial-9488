"""统计重采样对照试验。

在按试验 ID 命名的独立副本表上完成：固定候选索引 → 以未偏斜样本建立统计基线 →
写入一批确定性偏斜遥测（提交后立即可查）→ 测量「重新采样前」→ 显式 ANALYZE 重采样 →
测量「重新采样后」。表级 STATS_AUTO_RECALC=0，且要求全局 innodb_stats_on_metadata=0，
避免后台自动采样混入对照，并用统计指纹核对这一点。只在两侧查询结果对账一致、
索引结构一致、且重采样 / 新数据可见等前置条件都成立时给出结论。

副本表只在成功时删除；失败或中断时保留，供下一次尝试核验并续跑。核验不通过
（例如重新采样已生效但成绩尚未保存）就丢弃前一份证据，整对重测。
"""
import hashlib,json,time,statistics
from datetime import datetime,timedelta
from sqlalchemy import text
from src.config.database import engine
from src.constants.enums import COLUMNS

INDEX='trial_cmp_candidate'
LOCK='index_trial_cmp_serial'
GENERATOR='deterministic-v1'
MAX_BASE_COPY=20000
MAX_SKEW=20000

class UnverifiableResume(Exception):
 """中断后保存的前一份证据无法核验，必须整对重测。"""

def table_name(experiment_id):
 try:value=int(experiment_id)
 except (TypeError,ValueError):raise ValueError('试验 ID 无效')
 if value<1:raise ValueError('试验 ID 无效')
 return 'telemetry_cmp_%d'%value

def canonical(records):
 return hashlib.sha256(json.dumps(records,sort_keys=True,default=str).encode()).hexdigest()

def _timed(conn,query,params):
 samples=[];rows=[]
 for _ in range(5):
  start=time.perf_counter();rows=[dict(r) for r in conn.execute(text(query),params).mappings().all()];samples.append((time.perf_counter()-start)*1000)
 return rows,round(statistics.median(samples),4),[round(x,4) for x in samples]

def _handlers(conn):
 return {r['Variable_name']:int(r['Value']) for r in conn.execute(text("SHOW SESSION STATUS LIKE 'Handler_read%'")).mappings().all()}

def _index_columns(conn,table):
 rows=conn.execute(text('SHOW INDEX FROM '+table)).mappings().all()
 return [[r['Key_name'],int(r['Seq_in_index']),r['Column_name']] for r in rows if r['Key_name']==INDEX]

def _fingerprint(conn,table):
 """只读缓存的统计指纹；调用前须确认 innodb_stats_on_metadata=0。"""
 status=conn.execute(text("SHOW TABLE STATUS LIKE '"+table+"'")).mappings().first()
 if status is None:raise UnverifiableResume('对照副本不存在')
 index=conn.execute(text('SHOW INDEX FROM '+table)).mappings().all()
 return {'rows_estimate':int(status['Rows'] or 0),
  'index':[[r['Key_name'],int(r['Seq_in_index']),int(r['Cardinality'] or 0)] for r in index]}

def _flush(conn,table):
 """重载磁盘上的持久统计，丢弃插入带来的内存行数漂移。

 这就是「重采样前」的统计状态：优化器仍使用上一次采样结果，新增遥测已经可查。
 """
 try:
  conn.exec_driver_sql('FLUSH TABLE '+table);conn.commit()
 except Exception as exc:
  raise RuntimeError('无法重载表统计（需要 FLUSH_TABLES 权限），因而不能还原重新采样前的状态；请检查数据库账号权限') from exc

def _measure(conn,query,params):
 plan=[dict(r) for r in conn.execute(text('EXPLAIN '+query),params).mappings().all()]
 before=_handlers(conn)
 rows=[dict(r) for r in conn.execute(text(query),params).mappings().all()]
 after=_handlers(conn)
 delta={k:after.get(k,0)-before.get(k,0) for k in set(before)|set(after)}
 examined=sum(delta.get(k,0) for k in ('Handler_read_first','Handler_read_next','Handler_read_rnd_next'))
 try:analyze=conn.execute(text('EXPLAIN ANALYZE '+query),params).scalar()
 except Exception:analyze=None
 timed,median,samples=_timed(conn,query,params)
 head=plan[0] if plan else {}
 return {'plan':plan,'plan_type':head.get('type'),'plan_key':head.get('key'),'plan_extra':head.get('Extra'),
  'estimate_rows':None if head.get('rows') is None else int(head['rows']),
  'filtered':None if head.get('filtered') is None else float(head['filtered']),
  'returned':len(rows),'checksum':canonical(rows),'handler':delta,'rows_examined':examined,
  'explain_analyze':analyze,'median_ms':median,'samples_ms':samples,
  'timed_result_matches':canonical(timed)==canonical(rows)}

def _query(workload,table):
 params={'sensor':workload['sensor_id'],'start':workload['start_at'],'end':workload['end_at']}
 query='SELECT id,sensor_id,recorded_at,device_type,value FROM '+table+' WHERE sensor_id=:sensor AND recorded_at>=:start AND recorded_at<=:end'
 if workload.get('device_type'):
  query+=' AND device_type=:device';params['device']=workload['device_type']
 return query+' ORDER BY id',params

def _match_params(workload,start_id):
 params={'first':start_id,'sensor':workload['sensor_id'],'start':workload['start_at'],'end':workload['end_at']}
 if workload.get('device_type'):params['device']=workload['device_type']
 return params

def _match_where(workload):
 where=' id>=:first AND sensor_id=:sensor AND recorded_at>=:start AND recorded_at<=:end'
 if workload.get('device_type'):where+=' AND device_type=:device'
 return where

def skew_sample(workload,count,focus,start_id):
 """确定性偏斜样本：同一输入永远生成同一批行，用于两侧共用与断点核验。

 行必须落在负载的传感器 / 时间窗口 / 设备类型条件内，否则它们既不会出现在
 「重新采样前」结果中，也不会影响优化器对流量的判断，对照就失去意义。
 """
 if not count or int(count)>MAX_SKEW:raise ValueError('偏斜行数必须在 1..%d 之间'%MAX_SKEW)
 focus=[int(x) for x in (focus or [workload['sensor_id']])]
 if not focus or any(x<1 or x>1000 for x in focus):raise ValueError('聚焦传感器编号无效')
 start=datetime.fromisoformat(workload['start_at']);end=datetime.fromisoformat(workload['end_at'])
 window=max(1,int((end-start).total_seconds())-1)
 fixed_device=(workload.get('device_type') or '').strip()
 rows=[]
 for n in range(int(count)):
  recorded=(start+timedelta(seconds=(n*7919)%window)).isoformat(timespec='seconds')
  device=fixed_device or ('pump','motor','valve')[n%3]
  rows.append({'id':start_id+n,'sensor_id':focus[n%len(focus)],'recorded_at':recorded,
   'device_type':device,'value':round(40+(n*37%500)/10,2)})
 return rows,focus

def _describe(arm):
 names={'ALL':'全表扫描','index':'全索引扫描','range':'索引范围扫描','ref':'索引等值查找','const':'常量查找','eq_ref':'唯一索引查找','system':'系统表'}
 kind=names.get(arm.get('plan_type'),arm.get('plan_type') or '未知访问方式')
 return '%s（type=%s，key=%s）'%(kind,arm.get('plan_type'),arm.get('plan_key') or '无')

def decide(pre,post,reconciled,auto_isolated,resample_observed,new_visible,same_index):
 """结论门禁：对账一致且对照条件成立才给结论。"""
 reasons=[]
 if not reconciled:reasons.append('两侧查询结果对账不一致')
 if not auto_isolated:reasons.append('检测到自动统计采样混入对照')
 if not resample_observed:reasons.append('重新采样未观察到统计变化')
 if not new_visible:reasons.append('重新采样前未查到新增遥测')
 if not same_index:reasons.append('两侧索引结构不一致')
 if reasons:return {'ready':False,'verdict':'not_comparable','conclusion':None,'reasons':reasons}
 changed=(pre.get('plan_type'),pre.get('plan_key'))!=(post.get('plan_type'),post.get('plan_key'))
 verdict='plan_changed' if changed else 'plan_unchanged'
 summary='重新采样前%s；重新采样后%s。%s估计扫描行数 %s → %s，实查读取行数 %s → %s，两侧均返回 %s 行且结果一致。'%(
  _describe(pre),_describe(post),'访问方式发生变化。' if changed else '访问方式保持一致。',
  pre.get('estimate_rows'),post.get('estimate_rows'),pre.get('rows_examined'),post.get('rows_examined'),pre.get('returned'))
 return {'ready':True,'verdict':verdict,'conclusion':summary,'reasons':[]}

def _skew_rows(conn,table,start_id):
 return [dict(r) for r in conn.execute(text('SELECT id,sensor_id,recorded_at,device_type,value FROM '+table+' WHERE id>=:first ORDER BY id'),{'first':start_id}).mappings().all()]

def _verify_resume(resume,attempt,table,conn,query,params,columns):
 if not isinstance(resume,dict) or resume.get('kind')!='comparison' or not resume.get('pre'):
  raise UnverifiableResume('没有可续跑的对照证据')
 if resume.get('attempt')!=attempt:raise UnverifiableResume('尝试序号不一致')
 stored=resume.get('sample') or {}
 if _fingerprint(conn,table)!=resume.get('pre_stats'):raise UnverifiableResume('重新采样前统计状态已改变')
 expected=[['trial_cmp_candidate',i+1,c] for i,c in enumerate(columns)]
 if _index_columns(conn,table)!=expected:raise UnverifiableResume('候选索引与快照不一致')
 rows=_skew_rows(conn,table,int(stored.get('skew_start_id') or 0))
 if canonical(rows)!=stored.get('skew_checksum') or len(rows)!=stored.get('skew_rows'):
  raise UnverifiableResume('副本中的偏斜样本不一致')
 current=[dict(r) for r in conn.execute(text(query),params).mappings().all()]
 pre=resume.get('pre') or {}
 if canonical(current)!=pre.get('checksum') or len(current)!=pre.get('returned'):
  raise UnverifiableResume('重新采样前查询结果无法复现')
 return pre

def _prepare(conn,workload,columns,count,focus,table):
 conn.execute(text('DROP TABLE IF EXISTS '+table))
 conn.execute(text('CREATE TABLE '+table+'(id BIGINT NOT NULL PRIMARY KEY,sensor_id INT NOT NULL,recorded_at VARCHAR(30) NOT NULL,device_type VARCHAR(40) NOT NULL,value DOUBLE NOT NULL) ENGINE=InnoDB STATS_AUTO_RECALC=0'))
 base_rows=conn.execute(text('INSERT INTO '+table+'(id,sensor_id,recorded_at,device_type,value) SELECT id,sensor_id,recorded_at,device_type,value FROM telemetry')).rowcount
 if base_rows<=0 or base_rows>MAX_BASE_COPY:raise ValueError('遥测样本行数异常，无法建立对照副本')
 conn.execute(text('CREATE INDEX '+INDEX+' ON '+table+' ('+','.join('`'+c+'`' for c in columns)+')'))
 conn.execute(text('ANALYZE TABLE '+table)).all();conn.commit()
 baseline=_fingerprint(conn,table)
 start_id=int(conn.execute(text('SELECT COALESCE(MAX(id),0) FROM '+table)).scalar())+1
 rows,focus=skew_sample(workload,count,focus,start_id)
 conn.execute(text('INSERT INTO '+table+'(id,sensor_id,recorded_at,device_type,value) VALUES (:id,:sensor_id,:recorded_at,:device_type,:value)'),rows)
 conn.commit()
 _flush(conn,table)
 return int(base_rows),baseline,start_id,rows,focus

def run(snapshot,experiment_id,resume=None,attempt=1,checkpoint=None):
 workload=snapshot['workload'];columns=snapshot['candidate']['columns'];cfg=snapshot.get('comparison') or {}
 if not columns or any(c not in COLUMNS for c in columns) or len(set(columns))!=len(columns):raise ValueError('索引列无效')
 table=table_name(experiment_id)
 count=int(cfg.get('skew_rows') or 2000)
 query,params=_query(workload,table)
 with engine.connect() as conn:
  if conn.execute(text("SELECT GET_LOCK('"+LOCK+"',30)")).scalar()!=1:raise RuntimeError('另一对照试验正在执行')
  try:
   metadata_sampling=int(conn.execute(text('SELECT @@innodb_stats_on_metadata')).scalar() or 0)
   if metadata_sampling:raise RuntimeError('innodb_stats_on_metadata 已开启，元数据读取会触发重新采样；请保持为 0')
   resuming=bool(resume)
   if resuming:
    exists=conn.execute(text('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=:t'),{'t':table}).scalar()
    if not exists:raise UnverifiableResume('中断后副本已不存在')
    _flush(conn,table)
    pre=_verify_resume(resume,attempt,table,conn,query,params,columns)
    sample=resume['sample'];baseline=resume['baseline_stats'];pre_stats=resume['pre_stats']
   else:
    base_rows,baseline,start_id,rows,focus=_prepare(conn,workload,columns,count,cfg.get('focus_sensor_ids'),table)
    sample={'generator':GENERATOR,'base_rows':base_rows,'skew_rows':len(rows),
     'focus_sensor_ids':focus,'skew_start_id':start_id,'skew_checksum':canonical(rows)}
    sample['skew_visible_rows']=int(conn.execute(text('SELECT COUNT(*) FROM '+table+' WHERE id>=:first'),{'first':start_id}).scalar() or 0)
    if sample['skew_visible_rows']!=len(rows):raise RuntimeError('新增遥测在重采样前不可见')
    sample['skew_matched_rows']=int(conn.execute(text('SELECT COUNT(*) FROM '+table+' WHERE'+_match_where(workload)),_match_params(workload,start_id)).scalar() or 0)
    if sample['skew_matched_rows']<len(rows):raise ValueError('偏斜样本未全部落在负载条件内，请检查聚焦传感器与设备类型')
    pre=_measure(conn,query,params)
    pre_stats=_fingerprint(conn,table)
   visible_in_result=sample.get('skew_matched_rows')>0 and pre.get('returned',0)>=sample.get('skew_matched_rows',0)
   expected=[['trial_cmp_candidate',i+1,c] for i,c in enumerate(columns)]
   index_structure=_index_columns(conn,table)
   index_consistent=index_structure==expected
   auto_isolated=pre_stats==baseline and metadata_sampling==0
   new_visible=bool(visible_in_result)
   partial={'kind':'comparison','attempt':attempt,'generator':GENERATOR,'table':table,'query':query,'parameters':params,
    'sample':sample,'baseline_stats':baseline,'pre_stats':pre_stats,'pre':pre,'post':None,'post_stats':None,'reconciled':None,
    'index_structure':index_structure,
    'verification':{'auto_recalc_isolated':auto_isolated,'new_telemetry_visible':new_visible,'same_index':index_consistent,'resumed':resuming},
    'conclusion_ready':False,'verdict':None,'conclusion':None,'reasons':[],'complete':False}
   if checkpoint:checkpoint('pre_measured',partial)
   conn.execute(text('ANALYZE TABLE '+table)).all();conn.commit()
   post_stats=_fingerprint(conn,table)
   if checkpoint:checkpoint('resampled',dict(partial,post_stats=post_stats))
   resample_observed=post_stats!=pre_stats
   index_consistent=index_consistent and _index_columns(conn,table)==expected
   post=_measure(conn,query,params)
   reconciled=(pre['checksum']==post['checksum'] and pre['returned']==post['returned']
    and bool(pre.get('timed_result_matches')) and bool(post.get('timed_result_matches')))
   decision=decide(pre,post,reconciled,auto_isolated,resample_observed,new_visible,index_consistent)
   return {'kind':'comparison','attempt':attempt,'generator':GENERATOR,'table':table,'query':query,'parameters':params,
    'sample':sample,'baseline_stats':baseline,'pre_stats':pre_stats,'post_stats':post_stats,
    'pre':pre,'post':post,'reconciled':reconciled,'metadata_sampling':metadata_sampling,
    'index_structure':index_structure,
    'verification':{'auto_recalc_isolated':auto_isolated,'resample_observed':resample_observed,
     'new_telemetry_visible':new_visible,'same_index':index_consistent,'resumed':resuming},
    'conclusion_ready':decision['ready'],'verdict':decision['verdict'],'conclusion':decision['conclusion'],
    'reasons':decision['reasons'],'complete':True,
    'limitations':['独立遥测副本，两侧使用同一批确定性偏斜样本与同一候选索引','统计指纹用于核对自动采样是否混入；结论只在对账一致后生成','耗时包含驱动往返与缓存波动，不构成生产性能承诺']}
  finally:
   conn.rollback()
   conn.execute(text("SELECT RELEASE_LOCK('"+LOCK+"')"));conn.commit()

def discard(experiment_id):
 """丢弃副本（成功后清理或重测前清场）；拿不到锁就保留，避免破坏并行对照。"""
 with engine.connect() as conn:
  try:
   if conn.execute(text("SELECT GET_LOCK('"+LOCK+"',30)")).scalar()==1:
    conn.execute(text('DROP TABLE IF EXISTS '+table_name(experiment_id)));conn.commit()
    return True
   return False
  finally:
   conn.execute(text("SELECT RELEASE_LOCK('"+LOCK+"')"));conn.commit()
