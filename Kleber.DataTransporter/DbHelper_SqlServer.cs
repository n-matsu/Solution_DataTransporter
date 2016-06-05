using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace Kleber.DataTransporter
{
	public class DbHelper_SqlServer : DbHelper
	{
		public override DbConnection CreateConnection()
		{
			return new SqlConnection();
		}

		public override DbConnection CreateConnection(string connStr)
		{
			return new SqlConnection(connStr);
		}

		public override DbDataAdapter CreateDataAdapter()
		{
			return new SqlDataAdapter();
		}

		public override sealed DbCommand CreateCommand()
		{
			dynamic cmd = new SqlCommand();
			cmd.CommandType = CommandType.Text;
			return cmd;
		}

		public override DbParameter CreateParameter(string paramName)
		{
			DbParameter param = new SqlParameter();
			param.SourceColumn = paramName;
			param.ParameterName = "@" + paramName;
			return param;
		}

		public override DbParameter CreateParameter(string paramName,
			DbType type, byte? size = null)
		{
			DbParameter param = this.CreateParameter(paramName);
			param.DbType = type;
			if (size.HasValue)
			{
				param.Size = size.Value;
			}
			return param;
		}

		public override void SetParamValue(DbCommand cmd, string paramName, object value)
		{
			cmd.Parameters[$"@{paramName}"].Value = value;
		}
	}
}
