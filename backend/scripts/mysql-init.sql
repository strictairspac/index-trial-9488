-- 仅在数据卷为空时由 MySQL 官方镜像执行。
-- 对照试验需要 FLUSH TABLE 重载持久统计，才能在「重新采样前」还原上一次采样的统计。
GRANT FLUSH_TABLES ON *.* TO 'trial'@'%';
FLUSH PRIVILEGES;
