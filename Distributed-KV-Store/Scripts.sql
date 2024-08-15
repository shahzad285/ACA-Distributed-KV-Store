create table KVStore(
Ky nvarchar(256) primary key,
Value text,
Expiry int
);
CREATE INDEX IX_KVStore_Expiry ON KVStore (Expiry);