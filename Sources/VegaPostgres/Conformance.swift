//===----------------------------------------------------------------------===//
// Asking whether a type conforms to a protocol, once per type.
//
// `value as? any P` and `T.self is any P.Type` both ask the runtime, and the
// runtime charges about half a microsecond for the answer. On a path taken
// once that is nothing; on one taken for every result it is more than the
// work it guards. The answer cannot change while the process runs, so it is
// asked once and remembered.
//
// Every worker reads these on every result, and a new type is written about
// once. So a read takes no lock: it looks in the latest snapshot, which is
// never changed or freed once published. A miss takes the lock, asks, and
// publishes a new snapshot with the answer added. The old one is left alive
// for readers still in it: one snapshot per type ever asked about, a few
// small dictionaries for the life of the process. With a lock on every read,
// four workers reading at once made the pool's database path wait in the
// kernel (a contended Mutex is a futex call each way).
//===----------------------------------------------------------------------===//

import Synchronization

/// Answers about types, each worked out once and then read without a lock.
final class TypeAnswers<Value>: @unchecked Sendable {
    private final class Snapshot {
        let answers: [ObjectIdentifier: Value]
        init(_ answers: [ObjectIdentifier: Value]) { self.answers = answers }
    }

    /// The latest snapshot, retained once and never released.
    private let latest = Atomic<UnsafeRawPointer?>(nil)
    private let writing = Mutex<Void>(())

    /// The answer for `type`, from `ask` the first time and remembered after.
    func answer(for type: Any.Type, _ ask: (Any.Type) -> Value) -> Value {
        let key = ObjectIdentifier(type)
        if let known = lookUp(key) { return known }
        return writing.withLock { _ -> Value in
            if let known = lookUp(key) { return known }
            let answer = ask(type)
            var answers = snapshot.map { $0._withUnsafeGuaranteedRef { $0.answers } } ?? [:]
            answers[key] = answer
            let next = Unmanaged.passRetained(Snapshot(answers))
            latest.store(UnsafeRawPointer(next.toOpaque()), ordering: .releasing)
            return answer
        }
    }

    private var snapshot: Unmanaged<Snapshot>? {
        latest.load(ordering: .acquiring).map { Unmanaged<Snapshot>.fromOpaque($0) }
    }

    @inline(__always)
    private func lookUp(_ key: ObjectIdentifier) -> Value? {
        snapshot?._withUnsafeGuaranteedRef { $0.answers[key] }
    }
}

/// Whether types conform to one protocol, remembered as they are asked about.
final class ConformanceCache: Sendable {
    private let known = TypeAnswers<Bool>()
    private let ask: @Sendable (Any.Type) -> Bool

    /// - Parameter ask: the conformance question, such as
    ///   `{ $0 is any Validated.Type }`. Called at most once for each type.
    init(_ ask: @escaping @Sendable (Any.Type) -> Bool) {
        self.ask = ask
    }

    /// Whether `type` conforms.
    func holds(_ type: Any.Type) -> Bool {
        known.answer(for: type, ask)
    }
}
