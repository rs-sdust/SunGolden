// ***********************************************************************
// Assembly         : SunGolden.DBUtils
// Author           : Richer Yan
// Created          : 03-23-2018
//
// Last Modified By : Richer Yan
// Last Modified On : 03-23-2018
// ***********************************************************************
// <copyright file="PubConstant.cs" company="SunGolden">
//     Copyright (c) SunGolden. All rights reserved.
// </copyright>
// <summary>从web.config获取数据库连接配置</summary>
// ***********************************************************************
using System.Configuration;
/// <summary>
/// The DBUtils namespace.
/// </summary>
namespace SunGolden.DBUtils
{

    /// <summary>
    /// Class PubConstant.
    /// </summary>
    public class PubConstant
    {
        /// <summary>
        /// 获取连接字符串
        /// </summary>
        /// <value>The connection string.</value>
        public static string ConnectionString
        {           
            get 
            {
                string _connectionString = ConfigurationManager.AppSettings["ConnectionString"];       
                string ConStringEncrypt = ConfigurationManager.AppSettings["ConStringEncrypt"];
                if (ConStringEncrypt == "true")
                {
                    _connectionString = Encryption.DEncrypt.Decrypt(_connectionString);
                }
                return _connectionString; 
            }
        }

        /// <summary>
        /// 得到web.config里配置项的数据库连接字符串。
        /// </summary>
        /// <param name="configName">Name of the configuration.</param>
        /// <returns>System.String.</returns>
        public static string GetConnectionString(string configName)
        {
            string connectionString = ConfigurationManager.AppSettings[configName];
            string ConStringEncrypt = ConfigurationManager.AppSettings["ConStringEncrypt"];
            if (ConStringEncrypt == "true")
            {
                connectionString = Encryption.DEncrypt.Decrypt(connectionString);
            }
            return connectionString;
        }

        /// <summary>
        /// 获取SQLServer连接字符串
        /// </summary>
        /// <value>The SQL server connection string.</value>
        public static string SQLServerConnectionString
        {
            get
            {
                string _connectionString = ConfigurationManager.AppSettings["SQLServerConnectionString"];
                string ConStringEncrypt = ConfigurationManager.AppSettings["ConStringEncrypt"];
                if (ConStringEncrypt == "true")
                {
                    _connectionString = Encryption.DEncrypt.Decrypt(_connectionString);
                }
                return _connectionString;
            }
        }



    }
}
