// ***********************************************************************
// Assembly         : SunGolden.DBUtils
// Author           : Richer Yan
// Created          : 03-23-2018
//
// Last Modified By : Richer Yan
// Last Modified On : 03-23-2018
// ***********************************************************************
// <copyright file="DESEncrypt.cs" company="SunGolden">
//     Copyright (c) SunGolden. All rights reserved.
// </copyright>
// <summary>DES加解密</summary>
// ***********************************************************************
using System;
using System.Security.Cryptography;  
using System.Text;
/// <summary>
/// The DBUtils namespace.
/// </summary>
namespace SunGolden.Encryption
{
    /// <summary>
    /// DES加密/解密类。
    /// </summary>
	public class DESEncrypt
	{
        /// <summary>
        /// Initializes a new instance of the <see cref="DESEncrypt"/> class.
        /// </summary>
		public DESEncrypt()
		{			
		}

		#region ========加密======== 
 
        /// <summary>
        /// 加密
        /// </summary>
        /// <param name="Text">The text.</param>
        /// <returns>System.String.</returns>
		public static string Encrypt(string Text) 
		{
            return Encrypt(Text, "SunGolden");
		}
        /// <summary>
        /// 加密数据
        /// </summary>
        /// <param name="Text">The text.</param>
        /// <param name="sKey">The s key.</param>
        /// <returns>System.String.</returns>
		public static string Encrypt(string Text,string sKey) 
		{ 
			DESCryptoServiceProvider des = new DESCryptoServiceProvider(); 
			byte[] inputByteArray; 
			inputByteArray=Encoding.Default.GetBytes(Text); 
			des.Key = ASCIIEncoding.ASCII.GetBytes(System.Web.Security.FormsAuthentication.HashPasswordForStoringInConfigFile(sKey, "md5").Substring(0, 8)); 
			des.IV = ASCIIEncoding.ASCII.GetBytes(System.Web.Security.FormsAuthentication.HashPasswordForStoringInConfigFile(sKey, "md5").Substring(0, 8)); 
			System.IO.MemoryStream ms=new System.IO.MemoryStream(); 
			CryptoStream cs=new CryptoStream(ms,des.CreateEncryptor(),CryptoStreamMode.Write); 
			cs.Write(inputByteArray,0,inputByteArray.Length); 
			cs.FlushFinalBlock(); 
			StringBuilder ret=new StringBuilder(); 
			foreach( byte b in ms.ToArray()) 
			{ 
				ret.AppendFormat("{0:X2}",b); 
			} 
			return ret.ToString(); 
		} 

		#endregion
		
		#region ========解密======== 
   
 
        /// <summary>
        /// 解密
        /// </summary>
        /// <param name="Text">The text.</param>
        /// <returns>System.String.</returns>
		public static string Decrypt(string Text) 
		{
            return Decrypt(Text, "SunGolden");
		}
        /// <summary>
        /// 解密数据
        /// </summary>
        /// <param name="Text">The text.</param>
        /// <param name="sKey">The s key.</param>
        /// <returns>System.String.</returns>
		public static string Decrypt(string Text,string sKey) 
		{ 
			DESCryptoServiceProvider des = new DESCryptoServiceProvider(); 
			int len; 
			len=Text.Length/2; 
			byte[] inputByteArray = new byte[len]; 
			int x,i; 
			for(x=0;x<len;x++) 
			{ 
				i = Convert.ToInt32(Text.Substring(x * 2, 2), 16); 
				inputByteArray[x]=(byte)i; 
			} 
			des.Key = ASCIIEncoding.ASCII.GetBytes(System.Web.Security.FormsAuthentication.HashPasswordForStoringInConfigFile(sKey, "md5").Substring(0, 8)); 
			des.IV = ASCIIEncoding.ASCII.GetBytes(System.Web.Security.FormsAuthentication.HashPasswordForStoringInConfigFile(sKey, "md5").Substring(0, 8)); 
			System.IO.MemoryStream ms=new System.IO.MemoryStream(); 
			CryptoStream cs=new CryptoStream(ms,des.CreateDecryptor(),CryptoStreamMode.Write); 
			cs.Write(inputByteArray,0,inputByteArray.Length); 
			cs.FlushFinalBlock(); 
			return Encoding.Default.GetString(ms.ToArray()); 
		} 
 
		#endregion 


	}
}
