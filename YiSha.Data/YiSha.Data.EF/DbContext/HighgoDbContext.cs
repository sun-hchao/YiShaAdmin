using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using YiSha.Util;

namespace YiSha.Data.EF
{
    public class HighgoDBContext : DbContext, IDisposable
    {
        private string ConnectionString { get; set; }

        #region 构造函数
        public HighgoDBContext(string connectionString)
        {
            ConnectionString = connectionString;
        }
        #endregion

        #region 重载
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseNhgdb(ConnectionString, p => p.CommandTimeout(GlobalContext.SystemConfig.DBCommandTimeout));
            optionsBuilder.AddInterceptors(new DbCommandCustomInterceptor());
        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            Assembly entityAssembly = Assembly.Load(new AssemblyName("YiSha.Entity"));
            IEnumerable<Type> typesToRegister = entityAssembly.GetTypes().Where(p => !string.IsNullOrEmpty(p.Namespace))
                                                                         .Where(p => !string.IsNullOrEmpty(p.GetCustomAttribute<TableAttribute>()?.Name));
            foreach (Type type in typesToRegister)
            {
                dynamic configurationInstance = Activator.CreateInstance(type);
                modelBuilder.Model.AddEntityType(type);
            }
            
            foreach (var entity in modelBuilder.Model.GetEntityTypes())
            {
                PrimaryKeyConvention.SetPrimaryKey(modelBuilder, entity.Name);
                string currentTableName = modelBuilder.Entity(entity.Name).Metadata.GetTableName();
                modelBuilder.Entity(entity.Name).ToTable(currentTableName.ToLower());
                //20200825-sun_hchao修改
                foreach (var property in entity.GetProperties())
                {
                    modelBuilder.Entity(entity.Name).Property(property.Name).HasColumnName(property.Name.ToLower());
                }

                //var properties = entity.GetProperties();
                //foreach (var property in properties)
                //{
                //    ColumnConvention.SetColumnName(modelBuilder, entity.Name, property.Name);
                //}

            }

            base.OnModelCreating(modelBuilder);
        }
        #endregion
    }
}
