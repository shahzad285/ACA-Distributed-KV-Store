namespace Distributed_KV_Store.DTOs
{
    public class KVAddUpdateDTO
    {
        public string Key { get; set; }
        public string Value { get; set; }
        public int ExpiryTimeInSeconds { get; set; }
    }
}
