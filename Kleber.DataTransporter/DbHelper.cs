using System;
using System.Data;
using System.Data.Common;

namespace Kleber.DataTransporter
{
	public abstract class DbHelper
	{
		protected DbHelper()
		{
		}

		public abstract DbConnection CreateConnection();
		public abstract DbConnection CreateConnection(string connStr);
		public abstract DbDataAdapter CreateDataAdapter();
		public abstract DbCommand CreateCommand();
		public abstract DbParameter CreateParameter(string paramName);
		public abstract DbParameter CreateParameter(string paramName,
			DbType type, byte? size = null);

		public void AddParam(DbCommand cmd, string paramName)
		{
			cmd.Parameters.Add(this.CreateParameter(paramName));
		}

		public void AddParam(DbCommand cmd, string paramName,
			DbType type, byte? size = null)
		{
			cmd.Parameters.Add(this.CreateParameter(paramName, type, size));
		}

		public virtual void SetParamValue(DbCommand cmd, string paramName, object value)
		{
			cmd.Parameters[paramName].Value = value;
		}

		public void SetParamValues(DbCommand cmd, params object[] values)
		{
			int count = values.Length;
			count = Math.Min(count, cmd.Parameters.Count);
			for (int i = 0; i <= count - 1; i++)
			{
				cmd.Parameters[i].Value = values[i];
			}
		}

		public static void CloseConnection(DbConnection conn)
		{
			try
			{
				if ((conn != null))
				{
					try
					{
						if ((conn.State != ConnectionState.Closed
							&& conn.State != ConnectionState.Broken))
						{
							conn.Close();
						}
					}
					catch (Exception)
					{
					}
					conn.Dispose();
				}
			}
			catch (Exception)
			{
			}
		}
	}
}
