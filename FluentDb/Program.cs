using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

// This was trial 2 

namespace FluentDb
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var connBuilder = new FluentDbConnectionBuilder2(args[0])
                .AddCommand("SELECT * FROM dbo.[Schedule] WHERE Id = @Id",
                CommandType.Text,
                (o) =>
                {
                    o.Parameters.AddWithValue("@Id", SqlDbType.Int, 1);

                    o.ReadWith(async reader =>
                    {
                        while (await reader.ReadAsync())
                        {
                            var row = "";
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                row += reader[i].ToString();
                            }
                            Console.WriteLine(row);
                        }
                    });
                });

            await connBuilder.ExecuteAsync();
        }
        
    }

    public class DALOptions
    {
        public string ConnectionString { get; set; }
    }

    public class DALDependency
    {
        private readonly DALOptions _options;

        public DALDependency(DALOptions options)
        {
            _options = options;
        }

        public async Task<object> GetDataAsync()
        {
            object results = null;

            var connBuilder = new FluentDbConnectionBuilder2(_options.ConnectionString)
                .UseTransaction(IsolationLevel.ReadUncommitted)
                .AddCommand("INSERT INTO dbo.[SomeTable] (Col1, Col2) SELECT '@Col1Data', '@Col2Data';",
                    CommandType.Text,
                    options =>
                    {
                        options.Parameters.AddWithValue("@Col1Data", SqlDbType.VarChar, "Test");
                        options.Parameters.AddWithValue("@Col2Data", SqlDbType.VarChar, "Test2");

                        options.Assert(results => results.RecordsAffected == 1);
                    })
                .AddCommand("AddMoreStuff", CommandType.StoredProcedure, options =>
                {
                    options.Parameters.AddWithValue("@Param1", SqlDbType.VarChar, "Test");
                    options.Parameters.AddWithValue("@Param2", SqlDbType.VarChar, "Test2");

                    options.ReadWith(reader =>
                    {
                        results = reader[0];
                    });
                });

            await connBuilder.ExecuteAsync();

            return results;
        }
    }


    // This is working much better
    public class FluentDbCommandBuilder2
    {
        public FluentDbCommandBuilder2(string commandName, CommandType commandType)
        {
            CommandName = commandName;
            CommandType = commandType;
        }

        public virtual string CommandName { get; }
        public virtual CommandType CommandType { get;}

        public virtual FluentDbCommandOptions2 Options { get; private set; }

        public virtual Action<FluentDbCommandOptions2> ConfigureAction { get; private set; }
        

        public virtual Func<SqlConnection, SqlTransaction, SqlCommand> CommandFunc => (connection, transaction) =>
        {
            Command = connection.CreateCommand();

            if (transaction != null)
            {
                Command.Transaction = transaction;
            }

            Command.CommandText = CommandName;
            Command.CommandType = CommandType;

            Options = new FluentDbCommandOptions2();
            ConfigureAction(Options);
            Options.ConfigureCommand(Command);

            return Command;
        };

        public virtual SqlCommand Command { get; private set; }
        public FluentDbCommandResults2 Results { get; } = new FluentDbCommandResults2();

        private FluentDbCommandBuilder2 ConfigureHelper(Action<FluentDbCommandOptions2> configureAction)
        {
            ConfigureAction = configureAction;
            return this;
        }


        public virtual FluentDbCommandBuilder2 Configure(Action<FluentDbCommandOptions2> configureAction) =>
            ConfigureHelper(configureAction);
    }


    public class FluentDbCommandResults2
    {
        public int RecordsAffected { get; set; }

        public bool HasRows { get; set; }

        public Exception Exception { get; set; }
    }

    public class FluentDbCommandParametersBuilder2
    {
        private readonly IList<SqlParameter> _parameters = new List<SqlParameter>();

        public virtual SqlParameter[] AsArray => _parameters.ToArray();

        public virtual FluentDbCommandParametersBuilder2 AddWithValue(string name, SqlDbType type, object value) =>
            AddParameterHelper(name, type, value);

        private FluentDbCommandParametersBuilder2 AddParameterHelper(string name, SqlDbType type, object value)
        {
            _parameters.Add(new SqlParameter(name, type) {Value = value});

            return this;
        }
    }

    public class FluentDbCommandOptions2
    {
        public virtual FluentDbCommandParametersBuilder2 Parameters { get; } = new FluentDbCommandParametersBuilder2();

        public virtual Action<SqlCommand> ConfigureCommand => ConfigureCommandHelper;
        public virtual Action<SqlDataReader> ReaderFunc { get; private set; }
        public virtual Func<FluentDbCommandResults2, bool> AssertionFunc { get; private set; }

        private void ConfigureCommandHelper(SqlCommand obj)
        {
            obj.Parameters.AddRange(Parameters.AsArray);
        }

        private FluentDbCommandOptions2 ReadWithHelper(Action<SqlDataReader> readAction)
        {
            Test = readAction.GetHashCode();

            ReaderFunc = readAction;

            return this;
        }

        public int Test { get; set; }

        public virtual FluentDbCommandOptions2 ReadWith(Action<SqlDataReader> readAction) => ReadWithHelper(readAction);

        public virtual FluentDbCommandOptions2 Assert(Func<FluentDbCommandResults2, bool> assertionFunc) =>
            AssertionHelper(assertionFunc);

        private FluentDbCommandOptions2 AssertionHelper(Func<FluentDbCommandResults2, bool> assertionFunc)
        {
            AssertionFunc = assertionFunc;

            return this;
        }
    }

    public class FluentDbConnectionBuilder2
    {
        public FluentDbConnectionBuilder2(string connectionString)
            => ConnectionString = connectionString;

        public virtual string ConnectionString { get; }

        public virtual SqlConnection Connection { get; private set; }

        public virtual Func<Task<SqlTransaction>> TransactionFunc { get; private set; }

        public virtual Func<SqlConnection> ConnectionFunc => () => Connection = new SqlConnection(ConnectionString);

        public virtual IList<FluentDbCommandBuilder2> FluentDbCommandBuilder2s { get; } =
            new List<FluentDbCommandBuilder2>();

        public virtual FluentDbConnectionBuilder2 UseTransaction(IsolationLevel isolationLevel)
            => UseTransactionHelper(isolationLevel);

        public virtual FluentDbConnectionBuilder2 AddCommand(string commandName, CommandType commandType,
            Action<FluentDbCommandOptions2> configureAction) =>
            AddCommandHelper(commandName, commandType, configureAction);

        private FluentDbConnectionBuilder2 AddCommandHelper(string commandName, CommandType commandType,
            Action<FluentDbCommandOptions2> configureAction)
        {
            FluentDbCommandBuilder2s.Add(new FluentDbCommandBuilder2(commandName, commandType).Configure(configureAction));

            return this;
        }

        private FluentDbConnectionBuilder2 UseTransactionHelper(IsolationLevel isolationLevel)
        {
            TransactionFunc = async () => await Task.Run(() => Connection.BeginTransaction(isolationLevel));

            return this;
        }

        public virtual Task ExecuteAsync() => ExecuteAsyncHelper();

        private async Task ExecuteAsyncHelper()
        {
            ConnectionFunc();
            await using (Connection)
            {
                await Connection.OpenAsync();

                if (TransactionFunc != null)
                {
                    await using var transaction = await TransactionFunc();
                    {
                        foreach (var fluentDbCommandBuilder2 in FluentDbCommandBuilder2s)
                        {
                            try
                            {

                                await using var command = fluentDbCommandBuilder2.CommandFunc(Connection, transaction);

                                if (fluentDbCommandBuilder2.Options.ReaderFunc == null) continue;

                                await using var reader = await command.ExecuteReaderAsync();

                                fluentDbCommandBuilder2.Results.RecordsAffected = reader.RecordsAffected;
                                fluentDbCommandBuilder2.Results.HasRows = reader.HasRows;

                                fluentDbCommandBuilder2.Options.ReaderFunc(reader);
                            }
                            catch (Exception e)
                            {
                                // Independently catch command exceptions
                                fluentDbCommandBuilder2.Results.Exception = e;

                                await transaction.RollbackAsync();
                            }
                        }

                        await transaction.CommitAsync();
                    }
                }
                else
                {
                    foreach (var fluentDbCommandBuilder2 in FluentDbCommandBuilder2s)
                    {
                        try
                        {

                            await using var command = fluentDbCommandBuilder2.CommandFunc(Connection, null);

                            if (fluentDbCommandBuilder2.Options.ReaderFunc == null) continue;

                            await using var reader = await command.ExecuteReaderAsync();

                            fluentDbCommandBuilder2.Results.RecordsAffected = reader.RecordsAffected;
                            fluentDbCommandBuilder2.Results.HasRows = reader.HasRows;

                            fluentDbCommandBuilder2.Options.ReaderFunc(reader);
                        }
                        catch (Exception e)
                        {
                            // Independently catch command exceptions
                            fluentDbCommandBuilder2.Results.Exception = e;
                        }
                    }
                }
            }
        }
    }
}