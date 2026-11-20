"""幂等的元数据补列，供已有数据卷升级；新库由 metadata.create_all 建全。"""
import logging
from sqlalchemy import text
from src.config.database import engine

COLUMNS={
 'experiments':{
  'kind':"ADD COLUMN kind VARCHAR(20) NOT NULL DEFAULT 'index'",
  'attempt':"ADD COLUMN attempt INT NOT NULL DEFAULT 1",
  'evidence':"ADD COLUMN evidence JSON NULL",
 },
}

def upgrade():
 with engine.connect() as conn:
  for table,additions in COLUMNS.items():
   present=conn.execute(text('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=:t'),{'t':table}).scalar()
   if not present:continue
   existing={r['Field'] for r in conn.execute(text('SHOW COLUMNS FROM `'+table+'`')).mappings().all()}
   for name,ddl in additions.items():
    if name in existing:continue
    conn.execute(text('ALTER TABLE `'+table+'` '+ddl));conn.commit()
    logging.getLogger(__name__).info('schema: added %s.%s',table,name)

def ensure_flush_privilege():
 """老数据卷不会重跑 init 脚本；对照试验需要 FLUSH TABLE 重载持久统计。"""
 try:
  with engine.connect() as conn:
   conn.execute(text('SELECT 1'))
   try:
    conn.exec_driver_sql('FLUSH TABLE workloads')
    return True
   except Exception:
    logging.getLogger(__name__).warning('FLUSH_TABLES privilege missing; rebuild the db volume or grant it')
    return False
 except Exception:
  logging.getLogger(__name__).exception('could not check FLUSH_TABLES privilege')
  return False
