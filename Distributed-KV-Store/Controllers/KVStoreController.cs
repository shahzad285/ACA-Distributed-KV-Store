using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Distributed_KV_Store.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class KVStoreController : ControllerBase
    {
        private readonly ILogger<KVStoreController> _logger;
        public KVStoreController(ILogger<KVStoreController> logger)
        {
                _logger = logger;
        }

        [HttpGet(Name = "Get")]
        public string Get()
        {
            return "KV Store";
        }
    }
}
