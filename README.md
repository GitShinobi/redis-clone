# MiniRedis

A lightweight, Redis‑compatible server written in Go, with a built‑in CLI client.

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)

---

## 📖 Overview

MiniRedis is a Redis‑compatible in‑memory key‑value store built from scratch in Go. It supports a wide range of Redis commands, including strings, lists, sets, hashes, transactions, and optimistic locking (`WATCH`/`UNWATCH`). Persistence is handled via an Append‑Only File (AOF) with automatic rewriting.


---

## ✨ Features

| Feature | Status |
|---------|--------|
| **RESP Protocol** | ✅ Full support |
| **Strings** (`SET`, `GET`, `DEL`, `EXISTS`) | ✅ |
| **Expiry** (`EXPIRE`, `TTL`, `EXPIREAT`) | ✅ |
| **Lists** (`LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`, `RPOP`, `LPUSHX`, `RPUSHX`, `LINDEX`, `LSET`, `LTRIM`) | ✅ |
| **Sets** (`SADD`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SREM`) | ✅ |
| **Hashes** (`HSET`, `HGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`) | ✅ |
| **Transactions** (`MULTI`, `EXEC`, `DISCARD`) | ✅ |
| **Optimistic Locking** (`WATCH`, `UNWATCH`) | ✅ |
| **Keyspace** (`DBSIZE`, `KEYS`, `FLUSHALL`) | ✅ |
| **AOF Persistence** (append + rewrite) | ✅ |
| **Concurrency** (sharded locks) | ✅ |
| **CLI Client** | ✅ |
| **Test Suite** | ✅ |

---

## 🚀 Getting Started

### Prerequisites
- Go 1.21 or higher
- (Optional) `redis-cli` for interactive testing

### Installation & Running

```bash
# Clone the repository
git clone https://github.com/[your-username]/redis.git
cd redis

# Build and run the server
go run main.go
The server will start on localhost:8080.
# Starting the Client
go run client/client.go
You'll see a prompt:
redis:8080>
redis:8080> SET name Alice
+OK
redis:8080> GET name
"Alice" 
redis:8080> LPUSH colors red blue green
:3
redis:8080> LRANGE colors 0 -1
*3
"green" 
"blue" 
"red" 
redis:8080> Q
