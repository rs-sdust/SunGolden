// ***********************************************************************
// Assembly         : SunGolden.DBUtils
// Author           : RickerYan
// Created          : 04-23-2018
//------------------------------------------------------------------------
// Modified By : RickerYan
// Modified On : 04-24-2018
// 升级NPGSQL版本到3.1.00,
// 添加数据库事务
// 添加返回指定model类型列表函数
//------------------------------------------------------------------------
// Modified By : RickerYan
// Modified On : 04-24-2018
// 添加返回DbDataReader类型函数ExecuteReaderQuery
// 添加返回泛型数据函数ExecuteTQuery、ExecuteTListQuery
//------------------------------------------------------------------------
// Modified By : RickerYan
// Modified On : 05-15-2018
// 修复创建事务时的An operation is already in progress错误。
// 错误原因为使用的datareader未及时释放。
// ***********************************************************************
// <copyright file="PostgreSQL.cs" company="SunGolden">
//     Copyright © SunGolden 2018
// </copyright>
// <summary>PostgreSQL 访问类</summary>
// ***********************************************************************
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace SunGolden.DBUtils
{
    //public delegate T ObjectRelationMapper<T>(IDataReader reader);
    public class PostgreSQL
    {
        #region Props
        public static string connectionString = PubConstant.ConnectionString;
        public static NpgsqlConnection Connection { get; set; }
        private static bool IsDatabaseInitialized { get; set; }
        #endregion

        #region CheckDatabase
        /// <summary>
        /// 检查数据库连接
        /// </summary>
        /// <exception cref="System.Exception">Database system is not yet initialized</exception>
        protected static void CheckDatabase()
        {
            if (!IsDatabaseInitialized)
            {
                throw new Exception("Database system is not yet initialized");
            }
        }
        #endregion

        #region InitializeDatabase
        /// <summary>
        /// 打开数据库连接
        /// </summary>
        public static void OpenCon()
        {
            Connection = new NpgsqlConnection(connectionString);

            try
            {
                Connection.Open();
                IsDatabaseInitialized = true;
            }
            catch (Exception e)
            {
                IsDatabaseInitialized = false;
                throw e;
            }
        }
        /// <summary>
        /// 关闭数据库连接
        /// </summary>
        public static void CloseCon()
        {
            if (Connection == null)
            {
                IsDatabaseInitialized = false;
                return;
            }
            try
            {
                Connection.Close();
                IsDatabaseInitialized = false;
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        #endregion

        #region Transaction
        /// <summary>
        /// 开始事务
        /// </summary>
        /// <returns>NpgsqlTransaction.</returns>
        public static NpgsqlTransaction BeginTransaction()
        {
            CheckDatabase();
            return Connection.BeginTransaction();
        }

        /// <summary>
        /// 创建事务还原点
        /// </summary>
        /// <param name="savespoint">The savespoint.</param>
        /// <param name="transation">The transation.</param>
        public static void CreateSavePoint(string savespoint, NpgsqlTransaction transation)
        {
            NpgsqlCommand command = Connection.CreateCommand();
            command.CommandText = string.Format("SAVEPOINT {0}", savespoint);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// 回滚到指定还原点
        /// </summary>
        /// <param name="savespoint">The savespoint.</param>
        /// <param name="transation">The transation.</param>
        public static void RollbackToSavePoint(string savespoint, NpgsqlTransaction transation)
        {
            NpgsqlCommand command = Connection.CreateCommand();
            command.CommandText = string.Format("ROLLBACK TO SAVEPOINT  {0}", savespoint);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// 释放还原点.
        /// </summary>
        /// <param name="savespoint">The savespoint.</param>
        /// <param name="transation">The transation.</param>
        public static void ReleaseSavePoint(string savespoint, NpgsqlTransaction transation)
        {
            NpgsqlCommand command = Connection.CreateCommand();
            command.CommandText = string.Format("RELEASE SAVEPOINT {0}", savespoint);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// 回滚事务
        /// </summary>
        /// <param name="transaction">事务对象</param>
        public static void RollbackTransaction(NpgsqlTransaction transaction)
        {
            transaction.Rollback();
        }

        /// <summary>
        /// 提交事务
        /// </summary>
        /// <param name="transaction">The transaction.</param>
        public static void CommitTransaction(NpgsqlTransaction transaction)
        {
            transaction.Commit();
        }
        #endregion

        #region ExecuteNoneQuery

        /// <summary>
        /// 执行查询返回受影响的行数,适用于INSERT INTO、UPDATE、DELETE等
        /// </summary>
        /// <param name="sqlStatement">SQL查询语句.</param>
        /// <param name="transaction">事务对象，默认值为null.</param>
        /// <param name="parameters">参数列表.</param>
        public static int ExecuteNoneQuery(string sqlStatement, NpgsqlTransaction transaction = null, params DbParameter[] parameters)
        {
            NpgsqlCommand command = new NpgsqlCommand(sqlStatement, Connection);
            if (transaction != null)
            {
                command.Transaction = transaction;
            }
            SetupParameters(command, parameters);
            return command.ExecuteNonQuery();
        }
        #endregion

        #region ExecuteReaderQuery
        /// <summary>
        /// 执行一个查询语句，返回一个关联的DataReader实例   
        /// </summary>
        /// <param name="sqlStatement">The SQL statement.</param>
        /// <param name="transaction">The transaction.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>DbDataReader.</returns>
        public static DbDataReader ExecuteReaderQuery(string sqlStatement, NpgsqlTransaction transaction = null, params DbParameter[] parameters)
        {
            NpgsqlCommand command = new NpgsqlCommand(sqlStatement, Connection);
            if (transaction != null)
            {
                command.Transaction = transaction;
            }
            SetupParameters(command, parameters);
            return command.ExecuteReader();
        }
        #endregion

        #region ExecuteTableQuery
        /// <summary>
        /// 执行查询返回DataSet集合
        /// </summary>
        /// <param name="sqlStatement">SQL查询语句.</param>
        /// <param name="transaction">事务对象，默认值为null.</param>
        /// <param name="parameters">SQL参数列表.</param>
        /// <returns>DataSet.</returns>
        public static DataSet ExecuteDataSetQuery(string sqlStatement, NpgsqlTransaction transaction = null, params DbParameter[] parameters)
        {
            using (NpgsqlCommand command = new NpgsqlCommand(sqlStatement, Connection))
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }
                SetupParameters(command, parameters);
                using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(command))
                {
                    DataSet ds = new DataSet();
                    da.Fill(ds, "DataSet");
                    command.Parameters.Clear();
                    return ds;
                }
            }
        }
        /// <summary>
        /// 执行查询返回DataTable集合
        /// </summary>
        /// <param name="sqlStatement">SQL查询语句.</param>
        /// <param name="transaction">事务对象，默认值为null.</param>
        /// <param name="parameters">SQL参数列表.</param>
        /// <returns>DataSet.</returns>
        public static DataTable ExecuteTableQuery(string sqlStatement, NpgsqlTransaction transaction = null, params DbParameter[] parameters)
        {
            using (NpgsqlCommand command = new NpgsqlCommand(sqlStatement, Connection))
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }
                SetupParameters(command, parameters);
                using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(command))
                {
                    DataTable dt = new DataTable();
                    da.Fill(dt);
                    command.Parameters.Clear();
                    return dt;
                }
            }
        }
        #endregion

        #region ExecuteObjectQuery
        /// <summary>
        /// 执行查询返回指定model类型实例
        /// </summary>
        /// <param name="sqlStatement">SQL查询语句.</param>
        /// <param name="modelType">指定的model类型.</param>
        /// <param name="transaction">事务对象，默认值为null.</param>
        /// <param name="parameters">SQL参数列表.</param>
        /// <returns>List&lt;T&gt;.</returns>
        public static object ExecuteObjectQuery(string sqlStatement, Type modelType, NpgsqlTransaction transaction = null, params DbParameter[] parameters)
        {
            var objList=ExecuteObjectListQuery(sqlStatement, modelType, transaction, parameters);
            if (objList == null|| objList.Count==0)
            {
                return null;
            }
            else
            {
                return objList[0];
            }
        }
        /// <summary>
        /// 执行查询返回指定model类型集合
        /// </summary>
        /// <param name="sqlStatement">SQL查询语句.</param>
        /// <param name="modelType">指定的model类型.</param>
        /// <param name="transaction">事务对象，默认值为null.</param>
        /// <param name="parameters">SQL参数列表.</param>
        /// <returns>List&lt;T&gt;.</returns>
        public static List<object> ExecuteObjectListQuery(string sqlStatement, Type modelType, NpgsqlTransaction transaction = null, params DbParameter[] parameters)
        {
            using (NpgsqlCommand command = new NpgsqlCommand(sqlStatement, Connection))
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }
                SetupParameters(command, parameters);
                using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(command))
                {
                    DataSet ds = new DataSet();
                    da.Fill(ds, modelType.ToString());
                    command.Parameters.Clear();
                    if (ds == null)
                        return null;
                    return DataTableToList(ds.Tables[0], modelType);
                }
            }
        }

        #endregion

        #region ExecuteTQuery

        /// <summary>
        /// 执行一个查询语句，返回一个指定类型的实例
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlStatement">The SQL statement.</param>
        /// <param name="transaction">The transaction.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>T.</returns>
        public static T ExecuteTQuery<T>(string sqlStatement, NpgsqlTransaction transaction = null, params DbParameter[] parameters) where T : new()
        {
            var tList = ExecuteTListQuery<T>(sqlStatement, transaction , parameters);
            if (tList == null || tList.Count == 0)
                return default(T);
            else
                return tList[0];
        }

        /// <summary>
        /// 执行一个查询语句，返回指定类型的实例列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlStatement">The SQL statement.</param>
        /// <param name="transaction">The transaction.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>List&lt;T&gt;.</returns>
        public static List<T> ExecuteTListQuery<T>(string sqlStatement, NpgsqlTransaction transaction = null, params DbParameter[] parameters) where T : new()
        {
            using (DbDataReader reader = ExecuteReaderQuery(sqlStatement, transaction, parameters))
            {
                return EntityReader.GetEntities<T>(reader);
            }
        }
        #endregion

        #region ExecuteScalarQuery
        /// <summary>
        /// 执行查询返回第一行第一列数据，多用于获取记录条数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlStatement">SQL查询语句.</param>
        /// <param name="transaction">事务对象，默认值为null.</param>
        /// <param name="parameters">参数列表.</param>
        /// <returns>T.</returns>
        public static T ExecuteScalarQuery<T>(string sqlStatement, NpgsqlTransaction transaction=null, params DbParameter[] parameters)
        {
            return (T)ExecuteScalarQuery(sqlStatement, transaction, parameters);
        }

        /// <summary>
        /// 执行查询返回第一行第一列数据
        /// </summary>
        /// <param name="sqlStatement">SQL查询语句.</param>
        /// <param name="transaction">事务对象，默认值为null.</param>
        /// <param name="parameters">参数列表.</param>
        /// <returns>System.Object.</returns>
        public static object ExecuteScalarQuery(string sqlStatement, NpgsqlTransaction transaction=null, params DbParameter[] parameters)
        {
            NpgsqlCommand command = new NpgsqlCommand(sqlStatement, Connection);
            if (transaction != null)
            {
                command.Transaction = transaction;
            }
            SetupParameters(command, parameters);
            return command.ExecuteScalar();
        }
        #endregion

        #region SetupParameters
        private static void SetupParameters(DbCommand command, DbParameter[] parameters)
        {
            foreach (DbParameter param in parameters)
                command.Parameters.Add(param);
        }
        #endregion

        #region Convert
        //public static T Convert<T>(object data, object default_if_null)
        //{
        //    if (data == null || data == DBNull.Value)
        //        return (T)default_if_null;
        //    else
        //        return (T)data;
        //}
        #endregion

        #region NewParameter
        public static DbParameter NewParameter(string name, object value)
        {
            return new NpgsqlParameter(name, value);
        }
        #endregion

        private static List<object> DataTableToList(DataTable dt, Type modelType)
        {
            if (dt == null)
                return null;
            List<object> list = new List<object>();
            //遍历DataTable中所有的数据行
            foreach (DataRow dr in dt.Rows)
            {
                var model = Activator.CreateInstance(modelType);
                PropertyInfo[] propertys = modelType.GetProperties();
                foreach (PropertyInfo pro in propertys)
                {
                    //检查DataTable是否包含对象属性名  
                    if (dt.Columns.Contains(pro.Name))
                    {
                        object value = dr[pro.Name];

                        Type tmpType = Nullable.GetUnderlyingType(pro.PropertyType) ?? pro.PropertyType;
                        object safeValue = (value == null) ? null : System.Convert.ChangeType(value, tmpType);

                        //如果非空，则赋给对象的属性  PropertyInfo
                        if (safeValue != DBNull.Value)
                        {
                            pro.SetValue(model, safeValue, null);
                        }
                    }
                }
                //对象添加到集合
                list.Add(model);
            }
            return list;
        }
    }
}
