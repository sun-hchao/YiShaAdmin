using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YiSha.Data;
using YiSha.Data.EF;
using YiSha.Data.Repository;
using YiSha.Entity;
using YiSha.Entity.OrganizationManage;
using YiSha.Entity.SystemManage;
using YiSha.Model.Result.SystemManage;
using YiSha.Util;
using YiSha.Util.Model;

namespace YiSha.Service.SystemManage
{
    public class DatabaseTablePostgreSQLService : RepositoryFactory, IDatabaseTableService
    {
        #region 获取数据
        public async Task<List<TableInfo>> GetTableList(string tableName)
        {
            StringBuilder strSql = new StringBuilder();
            strSql.Append(@"SELECT table_name TableName FROM information_schema.tables WHERE table_schema='" + GetDatabase() + "' AND table_type='BASE TABLE'");
            IEnumerable<TableInfo> list = await this.BaseRepository().FindList<TableInfo>(strSql.ToString());
            if (!string.IsNullOrEmpty(tableName))
            {
                list = list.Where(p => p.TableName.Contains(tableName));
            }
            await SetTableDetail(list);
            return list.ToList();
        }

        public async Task<List<TableInfo>> GetTablePageList(string tableName, Pagination pagination)
        {
            StringBuilder strSql = new StringBuilder();
            var parameter = new List<DbParameter>();
            strSql.Append(@"SELECT table_name TableName FROM information_schema.tables where table_schema='" + GetDatabase() + "' and table_type='BASE TABLE'");

            if (!string.IsNullOrEmpty(tableName))
            {
                strSql.Append(" AND table_name like @TableName ");
                parameter.Add(DbParameterExtension.CreateDbParameter("@TableName", '%' + tableName + '%'));
            }

            IEnumerable<TableInfo> list = await this.BaseRepository().FindList<TableInfo>(strSql.ToString(), parameter.ToArray(), pagination);
            await SetTableDetail(list);
            return list.ToList();
        }

        public async Task<List<TableFieldInfo>> GetTableFieldList(string tableName)
        {
            StringBuilder strSql = new StringBuilder();
            //strSql.Append(@"SELECT COLUMN_NAME TableColumn, 
            //                 DATA_TYPE Datatype,
            //                 (CASE COLUMN_KEY WHEN 'PRI' THEN COLUMN_NAME ELSE '' END) TableIdentity,
            //                 REPLACE(REPLACE(SUBSTRING(COLUMN_TYPE,LOCATE('(',COLUMN_TYPE)),'(',''),')','') FieldLength,
            //                    (CASE IS_NULLABLE WHEN 'NO' THEN 'N' ELSE 'Y' END) IsNullable,
            //                       IFNULL(COLUMN_DEFAULT,'') FieldDefault,
            //                       COLUMN_COMMENT Remark
            //                 FROM information_schema.columns WHERE table_schema='" + GetDatabase() + "' AND table_name=@TableName");

            strSql.Append(@"select column_name as TableColumn,data_type as Datatype,
                                case when position('nextval' in column_default)> 0 then column_name else '' end as TableIdentity,
                                coalesce(character_maximum_length, numeric_precision, -1) as FieldLength,
                                case is_nullable when 'NO' then 'N' else 'Y' end as IsNullable,
                                column_default as FieldDefault,
                                c.DeText as Remark
                                from information_schema.columns
                                left join (
                                    select pg_attr.attname as colname,pg_constraint.conname as pk_name from pg_constraint
                                    inner join pg_class on pg_constraint.conrelid = pg_class.oid
                                    inner join pg_attribute pg_attr on pg_attr.attrelid = pg_class.oid and  pg_attr.attnum = pg_constraint.conkey[1]
                                    inner join pg_type on pg_type.oid = pg_attr.atttypid
                                    where pg_class.relname = @TableName and pg_constraint.contype = 'p'
                                ) b on b.colname = information_schema.columns.column_name
                                left join(
                                    select attname, description as DeText from pg_class
                                     left join pg_attribute pg_attr on pg_attr.attrelid = pg_class.oid
                                     left join pg_description pg_desc on pg_desc.objoid = pg_attr.attrelid and pg_desc.objsubid = pg_attr.attnum
                                    where pg_attr.attnum > 0 and pg_attr.attrelid = pg_class.oid and pg_class.relname = @TableName
                                )c on c.attname = information_schema.columns.column_name
                                where table_schema = '" + GetDatabase() + "' and table_name = @TableName order by ordinal_position asc");

            var parameter = new List<DbParameter>();
            parameter.Add(DbParameterExtension.CreateDbParameter("@TableName", tableName));
            var list = await this.BaseRepository().FindList<TableFieldInfo>(strSql.ToString(), parameter.ToArray());
            return list.ToList();
        }
        #endregion

        #region 公有方法
        public async Task<bool> DatabaseBackup(string database, string backupPath)
        {
            string backupFile = string.Format("{0}\\{1}_{2}.bak", backupPath, database, DateTime.Now.ToString("yyyyMMddHHmmss"));
            string strSql = string.Format(" backup database [{0}] to disk = '{1}'", database, backupFile);
            var result = await this.BaseRepository().ExecuteBySql(strSql);
            return result > 0 ? true : false;
        }

        /// <summary>
        /// 仅用在YiShaAdmin框架里面，同步不同数据库之间的数据，以 MySql 为主库，同步 MySql 的数据到SqlServer和Oracle，保证各个数据库的数据是一样的
        /// </summary>
        /// <returns></returns>
        public async Task SyncDatabase()
        {
            #region 同步SqlServer数据库
            await SyncPostgreSQLTable<AreaEntity>();
            await SyncPostgreSQLTable<AutoJobEntity>();
            await SyncPostgreSQLTable<AutoJobLogEntity>();
            await SyncPostgreSQLTable<DataDictEntity>();
            await SyncPostgreSQLTable<DataDictDetailEntity>();
            await SyncPostgreSQLTable<DepartmentEntity>();
            await SyncPostgreSQLTable<LogLoginEntity>();
            await SyncPostgreSQLTable<MenuEntity>();
            await SyncPostgreSQLTable<MenuAuthorizeEntity>();
            await SyncPostgreSQLTable<NewsEntity>();
            await SyncPostgreSQLTable<PositionEntity>();
            await SyncPostgreSQLTable<RoleEntity>();
            await SyncPostgreSQLTable<UserEntity>();
            await SyncPostgreSQLTable<UserBelongEntity>();
            #endregion
        }
        private async Task SyncPostgreSQLTable<T>() where T : class, new()
        {
            string sqlServerConnectionString = "Server=localhost;Database=YiShaAdmin;User Id=sa;Password=123456;";
            IEnumerable<T> list = await this.BaseRepository().FindList<T>();

            await new PostgreSQLDatabase(sqlServerConnectionString).Delete<T>(p => true);
            await new PostgreSQLDatabase(sqlServerConnectionString).Insert<T>(list);
        }
        #endregion

        #region 私有方法
        /// <summary>
        /// 获取所有表的主键、主键名称、记录数
        /// </summary>
        /// <returns></returns>
        private async Task<List<TableInfo>> GetTableDetailList()
        {
            //string strSql = @"SELECT t1.TABLE_NAME TableName,t1.TABLE_COMMENT Remark,t1.TABLE_ROWS TableCount,t2.CONSTRAINT_NAME TableKeyName,t2.column_name TableKey
            //                         FROM information_schema.TABLES as t1 
            //                      LEFT JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` as t2 on t1.TABLE_NAME = t2.TABLE_NAME
            //                         WHERE t1.TABLE_SCHEMA='" + GetDatabase() + "' AND t2.TABLE_SCHEMA='" + GetDatabase() + "'";
            
            string strSql = @"select pg_class.relname as tablename,cast(obj_description(pg_class.relfilenode, 'pg_class') as varchar) as remark,pg_class.reltuples as tablecount,case pg_constraint.contype when 'p' then 'PRIMARY' end as tablekeyname,pg_attribute.attname as tablekey
                                from pg_constraint
                                inner join pg_class on pg_constraint.conrelid = pg_class.oid
                                inner
                                join pg_attribute on pg_attribute.attrelid = pg_class.oid
                                and pg_attribute.attnum::text in (select regexp_split_to_table(array_to_string(pg_constraint.conkey, ','), ',+'))
                                where pg_constraint.contype = 'p'";

            IEnumerable<TableInfo> list = await this.BaseRepository().FindList<TableInfo>(strSql.ToString());
            return list.ToList();
        }

        /// <summary>
        /// 赋值表的主键、主键名称、记录数
        /// </summary>
        /// <param name="list"></param>
        private async Task SetTableDetail(IEnumerable<TableInfo> list)
        {
            List<TableInfo> detailList = await GetTableDetailList();
            foreach (TableInfo table in list)
            {
                table.TableKey = string.Join(",", detailList.Where(p => p.TableName == table.TableName).Select(p => p.TableKey));
                var tableInfo = detailList.Where(p => p.TableName == table.TableName).FirstOrDefault();
                if (tableInfo != null)
                {
                    table.TableKeyName = tableInfo.TableKeyName;
                    table.TableCount = tableInfo.TableCount;
                    table.Remark = tableInfo.Remark;
                }
            }
        }
        private string GetDatabase()
        {
            string database = HtmlHelper.Resove(GlobalContext.SystemConfig.DBConnectionString, "database=", ";");
            return database;
        }
        #endregion
    }
}
