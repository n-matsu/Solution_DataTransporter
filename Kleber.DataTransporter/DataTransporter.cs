using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Reactive.Linq;

namespace Kleber.DataTransporter
{
	public interface ITransportContext
	{
	}

	public enum TransportActionType
	{
		Empty,
		Load,
		Move,
		Exec,
	}

	public enum TransportActionStatusCode
	{
		None,
		Processing,
		Done,
		Canceled,
	}

	public enum TransportActionResultCode
	{
		None,
		OK,
		NG,
		Canceled,
	}

	public interface ITransportActionResult
	{
		string ActionId { get; }
		TransportActionResultCode ResultCode { get; }
	}

	public class TransportActionResult
		: ITransportActionResult
	{
		public TransportActionResult(
			string actionId,
			TransportActionResultCode code)
		{
			this.ActionId = actionId;
			this.ResultCode = code;
		}
		public string ActionId { get; private set; }
		public TransportActionResultCode ResultCode { get; private set; }
	}

	public interface ITransportActionStatus
	{
		string ActionId { get; }
		TransportActionStatusCode StatusCode { get; }
	}

	public class TransportActionStatus
		: ITransportActionStatus
	{
		public TransportActionStatus(
			string actionId,
			TransportActionStatusCode code)
		{
			this.ActionId = actionId;
			this.StatusCode = code;
		}
		public string ActionId { get; private set; }
		public TransportActionStatusCode StatusCode { get; private set; }
	}

	public interface ITransportAction<TResult, TStatus>
		where TResult : ITransportActionResult
		where TStatus : ITransportActionStatus
	{
		Task<TResult> Execute(IProgress<TStatus> progress);
	}

	public abstract class TransportActionBase<TResult, TStatus>
		: ITransportAction<TResult, TStatus>
		where TResult : ITransportActionResult
		where TStatus : ITransportActionStatus
	{
		public TransportActionBase(string id, ITransportContext ctx)
		{
			this.Id = id;
			this.Context = ctx;
		}
		public string Id { get; private set; }
		protected ITransportContext Context { get; private set; }
		public abstract Task<TResult> Execute(IProgress<TStatus> progress);
	}

	public interface ITransportSource<TData>
	{
		IObservable<TData> Create();
	}

	public interface ITransportDestination<TData>
	{
		void Write(TData data);
	}

	public interface ITransportHandler<TInData, TOutData>
	{
		bool Setup();
		TOutData Handle(TInData data);
	}

	public class DbTransportSource
		: ITransportSource<DbDataReader>
	{
		public DbTransportSource(DbCommand cmd)
		{
			this.Cmd = cmd;
		}

		public DbCommand Cmd { get; private set; }

		public IObservable<DbDataReader> Create()
		{
			return this.CreateReaderAsEnumerable().ToObservable();
		}

		private IEnumerable<DbDataReader> CreateReaderAsEnumerable()
		{
			using (var reader = this.Cmd.ExecuteReader())
				while (reader.Read())
					yield return reader;
		}
	}

	public abstract class DbDataReaderHandlerBase
		: ITransportHandler<DbDataReader, DbDataReader>
	{
		public DbDataReaderHandlerBase(
			DbCommand cmdSrc,
			DbCommand cmdDst,
			DbHelper helperSrc,
			DbHelper helperDst
			)
		{
			this.CmdSrc = cmdSrc;
			this.CmdDst = cmdDst;
		}

		protected ITransportContext Context { get; private set; }
		protected DbCommand CmdSrc { get; private set; }
		protected DbCommand CmdDst { get; private set; }
		protected DbHelper HelperSrc { get; private set; }
		protected DbHelper HelperDst { get; private set; }

		public virtual bool Setup() => true;

		public abstract DbDataReader Handle(DbDataReader data);
	}

	public class DbParamsSetHandler
		: DbDataReaderHandlerBase
	{
		private IEnumerable<string> ParamNames { get; set; }

		public DbParamsSetHandler(
			DbCommand cmdSrc,
			DbCommand cmdDst,
			DbHelper helperSrc,
			DbHelper helperDst
			)
			: base(cmdSrc, cmdDst, helperSrc, helperDst)
		{
		}

		public override bool Setup()
		{
			return this.CreateParametersAndGetNames();
		}

		public override DbDataReader Handle(DbDataReader data)
		{
			foreach (string name in this.ParamNames)
				this.HelperDst.SetParamValue(this.CmdDst, name, data[name]);
			return data;
		}

		private bool CreateParametersAndGetNames()
		{
			var paramNames = new List<string>();
			using (var reader = this.CmdSrc.ExecuteReader(CommandBehavior.SchemaOnly))
			{
				var tblSchema = reader.GetSchemaTable();
				foreach (DataRow row in tblSchema.Rows)
				{
					var paramName = row.Field<string>("ColumnName");
					paramNames.Add(paramName);
					var param = this.HelperDst.CreateParameter(paramName);
					this.CmdDst.Parameters.Add(param);
				}
			}
			this.ParamNames = paramNames;
			return true;
		}
	}

	public class DbTransportSink
		: ITransportDestination<DbDataReader>
	{
		public DbTransportSink(DbCommand cmd)
		{
			this.Cmd = cmd;
		}

		public DbCommand Cmd { get; private set; }

		public void Write(DbDataReader data)
		{
			this.Cmd.ExecuteNonQuery();
		}
	}

	public class TransportAction
		: TransportActionBase<TransportActionResult, TransportActionStatus>
	{
		private TaskCompletionSource<TransportActionResult> m_tcs;
		private IProgress<TransportActionStatus> m_progress;

		public TransportAction(
			string id,
			ITransportContext ctx,
			DbCommand cmdSrc,
			DbCommand cmdDst
			)
			: base(id, ctx)
		{
			this.CmdSrc = cmdSrc;
			this.CmdDst = cmdDst;
			this.Source = new DbTransportSource(cmdSrc);
			this.Sink = new DbTransportSink(cmdSrc);
			this.Result = new TransportActionResult(id, TransportActionResultCode.None);
			this.Handlers = new List<DbDataReaderHandlerBase>();
		}
		public DbCommand CmdSrc { get; private set; }
		public DbCommand CmdDst { get; private set; }
		public CancellationToken CancelToken { get; private set; }
		public TransportActionStatus Status { get; private set; }
		public ITransportActionResult Result { get; private set; }

		private DbTransportSource Source { get; set; }
		private DbTransportSink Sink { get; set; }
		private List<DbDataReaderHandlerBase> Handlers { get; set; }

		public bool Setup()
		{
			return this.Handlers.TrueForAll(h => h.Setup());
		}

		public override Task<TransportActionResult> Execute(
			IProgress<TransportActionStatus> progress
			)
		{
			m_progress = progress;
			m_tcs = new TaskCompletionSource<TransportActionResult>();
			this.Status = new TransportActionStatus(this.Id, TransportActionStatusCode.None);
			progress.Report(this.Status);
			Task<TransportActionResult>.Run(() => this.DoExecute(m_tcs, m_progress));
			return m_tcs.Task;
		}
		private void DoExecute(
			TaskCompletionSource<TransportActionResult> tcs,
			IProgress<TransportActionStatus> progress
			)
		{
			var disposable =
				this.Source.Create()
				.Select(data =>
				{
					var dataNew = data;
					this.Handlers.ForEach(h => dataNew = h.Handle(dataNew));
					return dataNew;
				})
				.Do(_ => this.CancelToken.ThrowIfCancellationRequested())
				.Subscribe(
					data => this.Sink.Write(data),
					e =>
					{
						if (e is TaskCanceledException)
							this.SetResultAndReport_Canceled();
						else
							this.SetResultAndReport_NG(e);
					},
					() => this.SetResultAndReport_OK()
					);
		}

		private void SetResultAndReport_OK()
		{
			this.SetResultAndReport(
				TransportActionStatusCode.Done,
				TransportActionResultCode.OK);
		}
		private void SetResultAndReport_NG(Exception e)
		{
			this.SetResultAndReport(
				TransportActionStatusCode.Done,
				TransportActionResultCode.NG);
			m_tcs.SetException(e);
		}
		private void SetResultAndReport_Canceled()
		{
			this.SetResultAndReport(
				TransportActionStatusCode.Canceled,
				TransportActionResultCode.Canceled);
			m_tcs.SetCanceled();
		}

		private void SetResultAndReport(
			TransportActionStatusCode statusCode,
			TransportActionResultCode resultCode)
		{
			this.Result = new TransportActionResult(this.Id, resultCode);
			this.Status = new TransportActionStatus(this.Id, statusCode);
			m_progress.Report(this.Status);
		}
	}
}
