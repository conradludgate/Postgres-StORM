import Foundation
import StORM
import PerfectThread

public struct ConnectionPool {
  public static var idleConnections: Int = 10
  //  public static var maxConections: Int = 50

  private static var idleLock: Threading.Lock = Threading.Lock()
  private static var active: Int = 0
  private static var idle: [PostgresConnect] = [] // Stack
  //  private static var connections: [PGConnection:(Lock,Int)] = [:]

  private static func getConnection(maxTimeout: Double, forcePrint: Bool?) -> PostgresConnect? {

    idleLock.lock()
    active += 1

    // Get the first ok connection
    while let conn = idle.popLast() {
      if conn.state == .good {
        idleLock.unlock()
        return conn
      }

      conn.server.close()
    }

    idleLock.unlock()

    // No connections available or all the idle connections were bad
    let conn = PostgresConnect(
      host:    PostgresConnector.host,
      username:  PostgresConnector.username,
      password:  PostgresConnector.password,
      database:  PostgresConnector.database,
      port:    PostgresConnector.port
    )

    var timeout = 0.1
    var waitedFor = 0.0
    // If the connection is bad, that means there's either a problem with auth
    // or a problem with the network. The only thing we can do it wait until it's available
    conn.open(forcePrint: forcePrint)
    while conn.state == .bad {
      PostgresStORM.printInfo("Error connecting to database", "Conn Err", logFile: "./StORMlog.txt", forcePrint: true)

      if waitedFor > maxTimeout {
        return nil
      }

      Threading.sleep(seconds: min(timeout, maxTimeout - waitedFor))
      conn.open(forcePrint: forcePrint)

      waitedFor += timeout
      //      timeout = max(timeout * 2, 60)
      timeout *= 2
    }

    return conn
  }

  private static func returnConnection(_ conn: PostgresConnect) {
    Threading.dispatch {
      idleLock.lock()
      defer { idleLock.unlock() }

      idle.append(conn)

      for i in 0..<(idle.count - idleConnections) {
        idle[i].server.close()
      }

      if idle.count > idleConnections {
        // Remove first since they'll be the oldest connections
        idle.removeFirst(idle.count - idleConnections)
      }

      active -= 1
    }
  }

  public static func dispatch<ReturnType>(maxTimeout: Double = 10, forcePrint: Bool?, closure: @escaping (PostgresConnect) throws -> ReturnType) -> Promise<Result<ReturnType, StORMError>> {
    return Promise {

      // Get a connection
      guard let conn = getConnection(maxTimeout: maxTimeout, forcePrint: forcePrint) else {
        return .failure(StORMError.error("Error establishing connection to database"))
      }
      defer { returnConnection(conn) }

      // Perform computation
      let result: Result<ReturnType, StORMError>
      do {
        result = .success(try closure(conn))
      } catch {
        result = .failure(StORMError.error("\(error)"))
      }

      // Return result
      return result
    }
  }

  // PGConnections are classes so this is CoW and this is will be a reference if used correctly
  public static func dispatch<ReturnType>(maxTimeout: Double = 10, forcePrint: Bool?, closure: @escaping (PostgresConnect) -> Result<ReturnType, StORMError>) -> Promise<Result<ReturnType, StORMError>> {
    return Promise {

      // Get a connection
      guard let conn = getConnection(maxTimeout: maxTimeout, forcePrint: forcePrint) else {
        return .failure(StORMError.error("Error establishing connection to database"))
      }
      defer { returnConnection(conn) }

      // Perform computation
      return closure(conn)
    }
  }
}

extension Result {
  func flatten<T>() -> Result<T, Failure> where Success == Result<T, Failure> {
    return self.flatMap { $0 }.mapError { $0 }
  }
  func unwrap() throws -> Success {
    switch self {
    case .success(let success):
      return success
    case .failure(let failure):
      throw failure
    }
  }
}

extension Promise {
  func waitResult(seconds: Double = Threading.noTimeout) -> Result<ReturnType, StORMError> {
    do {
      guard let result = try self.wait(seconds: seconds) else {
        return .failure(StORMError.error("Return value is not available"))
      }
      return .success(result)
    } catch {
      return .failure(StORMError.error("\(error)"))
    }
  }
}
