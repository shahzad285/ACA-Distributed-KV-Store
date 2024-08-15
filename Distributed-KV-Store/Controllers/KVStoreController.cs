using Distributed_KV_Store.DTOs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace Distributed_KV_Store.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class KVStoreController : ControllerBase
    {
        private readonly ILogger<KVStoreController> _logger;
        private readonly DbContext _dbContext;
        public KVStoreController(ILogger<KVStoreController> logger, DbContext dbContext)
        {
            _dbContext = dbContext;
            _logger = logger;
        }

        [HttpGet("Get")]
        public KV Get(string key)
        {
            var query = $"select * from KVSTore where Expiry>DATEDIFF(SECOND, '1970-01-01',GetUTCDate()) and ky='{key}'";
            var dt=_dbContext.ExecuteQuery(query);

            KV kv=new KV();

            if (dt.Rows.Count > 0)
            {
                kv.Key = Convert.ToString(dt.Rows[0]["Ky"]);
                kv.Value= Convert.ToString(dt.Rows[0]["Value"]);
            }

            return kv;
        }
        [HttpPost("Put")]
        public string Put(KVAddUpdateDTO kVAddUpdateDTO)
        {
            var expiry = DateTimeOffset.UtcNow.ToUnixTimeSeconds()+kVAddUpdateDTO.ExpiryTimeInSeconds;

            //There is not Upsert command in sql server and merge query alterative is comparitively more complicated and time taking even adter the overhead of transaction etc
            //Meged/Upsert query
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Start();
            var query = $@"MERGE KVStore AS target
                           USING (SELECT '{kVAddUpdateDTO.Key}' AS Ky, '{kVAddUpdateDTO.Value}' AS Value, {expiry} AS Expiry) AS source
                           ON (target.Ky = source.Ky)
                           WHEN MATCHED THEN
                               UPDATE SET 
                                   target.Value = source.Value,
                                   target.Expiry = source.Expiry
                           WHEN NOT MATCHED THEN
                               INSERT (Ky, Value, Expiry)
                            VALUES (source.Ky, source.Value, source.Expiry);";
            int res=_dbContext.ExecuteNonQuery(query);
            stopwatch.Stop();

            Console.WriteLine("Time elapsed in milliseconds  in merge/upsert query "+stopwatch.ElapsedMilliseconds);

            try
            {
                stopwatch = Stopwatch.StartNew();
                stopwatch.Start();
                _dbContext.BeginTransaction();
                //if else insert update query
               
                query = $@"IF EXISTS (SELECT 1 FROM KVStore WHERE Ky = '{kVAddUpdateDTO.Key}')
                         BEGIN
                          -- Update the existing record
                          UPDATE KVStore
                          SET Value = '{kVAddUpdateDTO.Value}', Expiry = {expiry}
                          WHERE Ky = '{kVAddUpdateDTO.Key}';
                       END
                       ELSE
                       BEGIN
                       -- Insert a new record
                       INSERT INTO KVStore (Ky, Value, Expiry)
                       VALUES ('{kVAddUpdateDTO.Key}', '{kVAddUpdateDTO.Value}', {expiry});
                       END";
                res = _dbContext.ExecuteNonQuery(query);
                stopwatch.Stop();               
                _dbContext.CommitTransaction();
                Console.WriteLine("Time elapsed in milliseconds  in if else update or insert query " + stopwatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Rollbacked");
                _dbContext.RollbackTransaction();
            }
            finally { 
            _dbContext.Dispose();
            }

            if (res==-1 || res == 0)
                return "Something went wrong";

            return "KV Store added/Updated";

        }

        [HttpPost("Del")]
        public string Del(DeleteDto del)
        {
            var query = $"Update KVStore set expiry=-1 where key={del.Key} and expiry>DATEDIFF(SECOND, '1970-01-01', GetDate())";
            int res = _dbContext.ExecuteNonQuery(query);
            if (res == 0 || res == 1)
                return "Deleted successfully";
            return "Something went wrong";
        }

    }
}
