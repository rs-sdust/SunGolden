// ***********************************************************************
// Assembly         : SunGolden.DBUtils
// Author           : Richer Yan
// Created          : 03-23-2018
//
// Last Modified By : Richer Yan
// Last Modified On : 03-23-2018
// ***********************************************************************
// <copyright file="CommandInfo.cs" company="SunGolden">
//     Copyright (c) SunGolden. All rights reserved.
// </copyright>
// <summary></summary>
// ***********************************************************************
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
/// <summary>
/// The DBUtils namespace.
/// </summary>
namespace SunGolden.DBUtils
{
    /// <summary>
    /// Enum EffentNextType
    /// </summary>
    public enum EffentNextType
    {
        /// <summary>
        /// 对其他语句无任何影响
        /// </summary>
        None,
        /// <summary>
        /// 当前语句必须为"select count(1) from .."格式，如果存在则继续执行，不存在回滚事务
        /// </summary>
        WhenHaveContine,
        /// <summary>
        /// 当前语句必须为"select count(1) from .."格式，如果不存在则继续执行，存在回滚事务
        /// </summary>
        WhenNoHaveContine,
        /// <summary>
        /// 当前语句影响到的行数必须大于0，否则回滚事务
        /// </summary>
        ExcuteEffectRows,
        /// <summary>
        /// 引发事件-当前语句必须为"select count(1) from .."格式，如果不存在则继续执行，存在回滚事务
        /// </summary>
        SolicitationEvent
    }
    /// <summary>
    /// Class CommandInfo.
    /// </summary>
    public class CommandInfo
    {
        /// <summary>
        /// The share object
        /// </summary>
        public object ShareObject = null;
        /// <summary>
        /// The original data
        /// </summary>
        public object OriginalData = null;
        /// <summary>
        /// Occurs when [_solicitation event].
        /// </summary>
        event EventHandler _solicitationEvent;
        /// <summary>
        /// Occurs when [solicitation event].
        /// </summary>
        public event EventHandler SolicitationEvent
        {
            add
            {
                _solicitationEvent += value;
            }
            remove
            {
                _solicitationEvent -= value;
            }
        }
        /// <summary>
        /// Called when [solicitation event].
        /// </summary>
        public void OnSolicitationEvent()
        {
            if (_solicitationEvent != null)
            {
                _solicitationEvent(this,new EventArgs());
            }
        }
        /// <summary>
        /// The command text
        /// </summary>
        public string CommandText;
        /// <summary>
        /// The parameters
        /// </summary>
        public System.Data.Common.DbParameter[] Parameters;
        /// <summary>
        /// The effent next type
        /// </summary>
        public EffentNextType EffentNextType = EffentNextType.None;
        /// <summary>
        /// Initializes a new instance of the <see cref="CommandInfo" /> class.
        /// </summary>
        public CommandInfo()
        {

        }
        /// <summary>
        /// Initializes a new instance of the <see cref="CommandInfo" /> class.
        /// </summary>
        /// <param name="sqlText">The SQL text.</param>
        /// <param name="para">The para.</param>
        public CommandInfo(string sqlText, SqlParameter[] para)
        {
            this.CommandText = sqlText;
            this.Parameters = para;
        }
        /// <summary>
        /// Initializes a new instance of the <see cref="CommandInfo" /> class.
        /// </summary>
        /// <param name="sqlText">The SQL text.</param>
        /// <param name="para">The para.</param>
        /// <param name="type">The type.</param>
        public CommandInfo(string sqlText, SqlParameter[] para, EffentNextType type)
        {
            this.CommandText = sqlText;
            this.Parameters = para;
            this.EffentNextType = type;
        }
    }
}
