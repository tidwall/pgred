# pgred

Key value store using the redis protocol with Postgres as a backend.

- Zero configuration
- Requires docker on local machine
- Basic commands. GET, SET, DEL, FLUSHDB, PING
- Serialized transaction commands BEGIN, ROLLBACK, COMMIT
- Ephemeral. Data go bye-bye on shutdown
- Experimental. You've been warned

## Running

```
go run main.go
```

## Using

```
redis-cli -p 6380
```

Good luck 
