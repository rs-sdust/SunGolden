// ***********************************************************************
// Assembly         : SunGolden.DBUtils
// Author           : RickerYan
// Created          : 04-25-2018
//
// Last Modified By : RickerYan
// Last Modified On : 04-25-2018
// ***********************************************************************
// <copyright file="ProviderFactory.cs" company="SunGolden">
//     Copyright © SunGolden 2018
// </copyright>
// <summary>DbProviderFactory工厂类</summary>
// ***********************************************************************
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace SunGolden.DBUtils
{
    /// <summary>
    /// DbProviderFactory工厂类
    /// </summary>
    public class ProviderFactory
    {
        private static Dictionary<DbProviderType, string> providerInvariantNames = new Dictionary<DbProviderType, string>();
        private static Dictionary<DbProviderType, DbProviderFactory> providerFactoies = new Dictionary<DbProviderType, DbProviderFactory>(20);
        static ProviderFactory()
        {
            //加载已知的数据库访问类的程序集
            providerInvariantNames.Add(DbProviderType.SqlServer, "System.Data.SqlClient");
            providerInvariantNames.Add(DbProviderType.OleDb, "System.Data.OleDb");
            providerInvariantNames.Add(DbProviderType.ODBC, "System.Data.ODBC");
            providerInvariantNames.Add(DbProviderType.Oracle, "Oracle.DataAccess.Client");
            providerInvariantNames.Add(DbProviderType.MySql, "MySql.Data.MySqlClient");
            providerInvariantNames.Add(DbProviderType.SQLite, "System.Data.SQLite");
            providerInvariantNames.Add(DbProviderType.Firebird, "FirebirdSql.Data.Firebird");
            providerInvariantNames.Add(DbProviderType.PostgreSql, "Npgsql");
            providerInvariantNames.Add(DbProviderType.DB2, "IBM.Data.DB2.iSeries");
            providerInvariantNames.Add(DbProviderType.Informix, "IBM.Data.Informix");
            providerInvariantNames.Add(DbProviderType.SqlServerCe, "System.Data.SqlServerCe");
        }
        /// <summary>
        /// 获取指定数据库类型对应的程序集名称
        /// </summary>
        /// <param name="providerType">数据库类型枚举</param>
        /// <returns></returns>
        public static string GetProviderInvariantName(DbProviderType providerType)
        {
            return providerInvariantNames[providerType];
        }
        /// <summary>
        /// 获取指定类型的数据库对应的DbProviderFactory
        /// </summary>
        /// <param name="providerType">数据库类型枚举</param>
        /// <returns></returns>
        public static DbProviderFactory GetDbProviderFactory(DbProviderType providerType)
        {
            //如果还没有加载，则加载该DbProviderFactory
            if (!providerFactoies.ContainsKey(providerType))
            {
                providerFactoies.Add(providerType, ImportDbProviderFactory(providerType));
            }
            return providerFactoies[providerType];
        }
        /// <summary>
        /// 加载指定数据库类型的DbProviderFactory
        /// </summary>
        /// <param name="providerType">数据库类型枚举</param>
        /// <returns></returns>
        private static DbProviderFactory ImportDbProviderFactory(DbProviderType providerType)
        {
            string providerName = providerInvariantNames[providerType];
            DbProviderFactory factory = null;
            try
            {
                //从全局程序集中查找
                factory = DbProviderFactories.GetFactory(providerName);
            }
            catch (ArgumentException e)
            {
                factory = null;
            }
            return factory;
        }
    }

    /// <summary>
    /// 数据库类型枚举
    /// </summary>
    public enum DbProviderType : byte
    {
        SqlServer,
        MySql,
        SQLite,
        Oracle,
        ODBC,
        OleDb,
        Firebird,
        PostgreSql,
        DB2,
        Informix,
        SqlServerCe
    }
}
