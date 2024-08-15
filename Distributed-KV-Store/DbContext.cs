using System.Data;
using System.Data.SqlClient;

namespace Distributed_KV_Store
{
    public class DbContext : IDisposable
    {
        private readonly string _connectionString;
        private SqlConnection _connection;
        private SqlTransaction _transaction;

        public DbContext(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection");
            _connection = new SqlConnection(_connectionString);
        }

        public void OpenConnection()
        {
            if (_connection.State == ConnectionState.Closed)
            {
                _connection.Open();
            }
        }

        public void CloseConnection()
        {
            if (_connection.State == ConnectionState.Open)
            {
                _connection.Close();
            }
        }

        public DataTable ExecuteQuery(string query)
        {
            OpenConnection();
            using (var command = new SqlCommand(query, _connection))
            {
                using (var adapter = new SqlDataAdapter(command))
                {
                    var dataTable = new DataTable();
                    adapter.Fill(dataTable);
                    CloseConnection();
                    return dataTable;
                }
            }
        }

        public int ExecuteNonQuery(string query)
        {
            OpenConnection();
            using (var command = new SqlCommand(query, _connection))
            {
                int result = command.ExecuteNonQuery();
                CloseConnection();
                return result;
            }
        }

        public void BeginTransaction()
        {
            if (_connection.State == ConnectionState.Open && _transaction == null)
            {
                _transaction = _connection.BeginTransaction();
            }
        }

        public void CommitTransaction()
        {
            if (_transaction != null)
            {
                _transaction.Commit();
                _transaction.Dispose();
                _transaction = null;
            }
        }

        public void RollbackTransaction()
        {
            if (_transaction != null)
            {
                _transaction.Rollback();
                _transaction.Dispose();
                _transaction = null;
            }
        }

        public void Dispose()
        {
            if (_transaction != null)
            {
                RollbackTransaction();
            }
            CloseConnection();
            _connection.Dispose();
        }

    }
}
