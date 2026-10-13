//===----------------------------------------------------------------------===//
// Redis for applications: commands with their replies typed, pipelines and
// transactions in one round trip, and pub/sub. From Garuda's RedisPool.
//===----------------------------------------------------------------------===//

import VegaRedisProtocol

// MARK: - Commands

/// Something commands can be sent to: a pool, or one session taken from it.
public protocol RedisCommandSender: Sendable {
    /// Sends one command and returns its reply. A refusal from the server
    /// throws `RedisClientError.server`.
    func send(_ command: RedisCommand) async throws(RedisClientError) -> RedisValue

    /// Sends several commands and returns a reply for each, in order. A
    /// command the server refused is an `.error` among them rather than a
    /// throw, so the others' replies are not lost with it.
    ///
    /// One write where one will do: a pool writes them all together, and a
    /// cluster writes one batch per slot, since a node answers only for the
    /// slots it owns.
    func pipeline(_ commands: [RedisCommand]) async throws(RedisClientError) -> [RedisValue]
}

/// How `set` treats a key that already exists.
public enum RedisSetCondition: Sendable {
    case always
    /// NX: only if the key does not exist.
    case ifAbsent
    /// XX: only if it does.
    case ifPresent
}

extension RedisCommandSender {
    /// Sends a command by name: `try await redis.send("ZADD", "board", 12, "ada")`.
    @discardableResult
    public func send(_ name: String, _ arguments: any RedisArgument...) async throws(RedisClientError) -> RedisValue {
        try await send(RedisCommand(name, arguments: arguments))
    }

    // Strings

    /// The value at `key` as text, or nil when there is none.
    public func get(_ key: String) async throws(RedisClientError) -> String? {
        try optional(try await send("GET", key)) { $0.string }
    }

    /// The value at `key` as bytes, or nil when there is none.
    public func getBytes(_ key: String) async throws(RedisClientError) -> [UInt8]? {
        try optional(try await send("GET", key)) { $0.bytes }
    }

    /// Stores `value` at `key`. With `expireMilliseconds` it expires; with a
    /// condition it may not be stored, which returns false.
    @discardableResult
    public func set(_ key: String, _ value: any RedisArgument, expireMilliseconds: Int? = nil,
                    condition: RedisSetCondition = .always) async throws(RedisClientError) -> Bool {
        var command = RedisCommand("SET", key, value)
        if let expireMilliseconds {
            command.append("PX")
            command.append(expireMilliseconds)
        }
        switch condition {
        case .always: break
        case .ifAbsent: command.append("NX")
        case .ifPresent: command.append("XX")
        }
        let reply = try await send(command)
        if reply.isNull { return false }
        guard reply.string == "OK" else { throw .unexpectedReply(reply) }
        return true
    }

    /// Deletes keys and returns how many existed.
    @discardableResult
    public func del(_ keys: String...) async throws(RedisClientError) -> Int {
        try integer(try await send(RedisCommand("DEL", arguments: keys)))
    }

    /// How many of `keys` exist, counting a key named twice twice.
    public func exists(_ keys: String...) async throws(RedisClientError) -> Int {
        try integer(try await send(RedisCommand("EXISTS", arguments: keys)))
    }

    /// Sets `key` to expire, and returns whether the key exists.
    @discardableResult
    public func pexpire(_ key: String, milliseconds: Int) async throws(RedisClientError) -> Bool {
        try integer(try await send("PEXPIRE", key, milliseconds)) == 1
    }

    /// Milliseconds until `key` expires: nil when there is no such key, and
    /// -1 when it never does.
    public func pttl(_ key: String) async throws(RedisClientError) -> Int? {
        let ms = try integer(try await send("PTTL", key))
        return ms == -2 ? nil : ms
    }

    /// Adds `amount` to the integer at `key`, from 0, and returns the result.
    @discardableResult
    public func incr(_ key: String, by amount: Int = 1) async throws(RedisClientError) -> Int {
        try integer(try await send("INCRBY", key, amount))
    }

    // Hashes

    /// Sets fields of the hash at `key`, and returns how many were new.
    @discardableResult
    public func hset(_ key: String, _ fields: [(String, any RedisArgument)]) async throws(RedisClientError) -> Int {
        var command = RedisCommand("HSET", key)
        for (field, value) in fields {
            command.append(field)
            command.append(value)
        }
        return try integer(try await send(command))
    }

    public func hget(_ key: String, _ field: String) async throws(RedisClientError) -> String? {
        try optional(try await send("HGET", key, field)) { $0.string }
    }

    /// Every field of the hash at `key`, empty when there is none.
    public func hgetall(_ key: String) async throws(RedisClientError) -> [String: String] {
        let reply = try await send("HGETALL", key)
        guard let pairs = reply.pairs else { throw .unexpectedReply(reply) }
        var out: [String: String] = [:]
        for pair in pairs {
            guard let field = pair.key.string, let value = pair.value.string else { throw .unexpectedReply(reply) }
            out[field] = value
        }
        return out
    }

    @discardableResult
    public func hdel(_ key: String, _ fields: String...) async throws(RedisClientError) -> Int {
        try integer(try await send(RedisCommand("HDEL", arguments: [key] + fields)))
    }

    // Lists

    /// Pushes onto the head of the list at `key`, and returns its length.
    @discardableResult
    public func lpush(_ key: String, _ values: any RedisArgument...) async throws(RedisClientError) -> Int {
        try integer(try await send(RedisCommand("LPUSH", arguments: [key] + values)))
    }

    /// Pushes onto the tail of the list at `key`, and returns its length.
    @discardableResult
    public func rpush(_ key: String, _ values: any RedisArgument...) async throws(RedisClientError) -> Int {
        try integer(try await send(RedisCommand("RPUSH", arguments: [key] + values)))
    }

    public func lpop(_ key: String) async throws(RedisClientError) -> String? {
        try optional(try await send("LPOP", key)) { $0.string }
    }

    public func rpop(_ key: String) async throws(RedisClientError) -> String? {
        try optional(try await send("RPOP", key)) { $0.string }
    }

    /// Elements `start` to `stop` inclusive; negative counts from the end.
    public func lrange(_ key: String, _ start: Int, _ stop: Int) async throws(RedisClientError) -> [String] {
        try strings(try await send("LRANGE", key, start, stop))
    }

    // Sets

    @discardableResult
    public func sadd(_ key: String, _ members: any RedisArgument...) async throws(RedisClientError) -> Int {
        try integer(try await send(RedisCommand("SADD", arguments: [key] + members)))
    }

    @discardableResult
    public func srem(_ key: String, _ members: any RedisArgument...) async throws(RedisClientError) -> Int {
        try integer(try await send(RedisCommand("SREM", arguments: [key] + members)))
    }

    public func smembers(_ key: String) async throws(RedisClientError) -> [String] {
        try strings(try await send("SMEMBERS", key))
    }

    // Pub/sub

    /// Publishes `message` on `channel`, and returns how many subscribers
    /// received it.
    @discardableResult
    public func publish(_ channel: String, _ message: any RedisArgument) async throws(RedisClientError) -> Int {
        try integer(try await send("PUBLISH", channel, message))
    }

    /// Publishes `message` on a sharded `channel`, and returns how many
    /// subscribers received it.
    ///
    /// A sharded channel belongs to a slot, as a key does, so in a cluster it
    /// travels no further than the shard that owns it.
    @discardableResult
    public func spublish(_ channel: String, _ message: any RedisArgument) async throws(RedisClientError) -> Int {
        try integer(try await send("SPUBLISH", channel, message))
    }

    // Replies

    private func integer(_ reply: RedisValue) throws(RedisClientError) -> Int {
        guard case .integer(let value) = reply, let int = Int(exactly: value) else { throw .unexpectedReply(reply) }
        return int
    }

    private func optional<T>(_ reply: RedisValue, _ read: (RedisValue) -> T?) throws(RedisClientError) -> T? {
        if reply.isNull { return nil }
        guard let value = read(reply) else { throw .unexpectedReply(reply) }
        return value
    }

    private func strings(_ reply: RedisValue) throws(RedisClientError) -> [String] {
        guard let elements = reply.array else { throw .unexpectedReply(reply) }
        var out: [String] = []
        out.reserveCapacity(elements.count)
        for element in elements {
            guard let text = element.string else { throw .unexpectedReply(reply) }
            out.append(text)
        }
        return out
    }
}

/// The replies to MULTI, the commands and EXEC: the commands' own, or nil
/// when a watched key changed and none of them ran.
func transactionReplies(_ replies: [RedisValue], commands: Int) throws(RedisClientError) -> [RedisValue]? {
    guard replies.count == commands + 2 else { throw .unexpectedReply(.array(replies)) }
    if case .error(let error) = replies[0] { throw .server(error) }
    let exec = replies[replies.count - 1]
    if case .error(let error) = exec {
        // EXECABORT: a command was refused as it was queued, and nothing ran.
        // Its own error says which and why.
        for queued in replies[1..<replies.count - 1] {
            if case .error(let refused) = queued { throw .server(refused) }
        }
        throw .server(error)
    }
    if exec.isNull { return nil }
    guard let results = exec.array, results.count == commands else { throw .unexpectedReply(exec) }
    return results
}
