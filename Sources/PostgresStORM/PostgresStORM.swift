//
//  PostgreStORM.swift
//  PostgresSTORM
//
//  Created by Jonathan Guthrie on 2016-10-03.
//
//

import StORM
import PerfectPostgreSQL
import PerfectLogger
import Foundation
import PerfectLib

/// PostgresConnector sets the connection parameters for the PostgreSQL Server access
/// Usage:
/// PostgresConnector.host = "XXXXXX"
/// PostgresConnector.username = "XXXXXX"
/// PostgresConnector.password = "XXXXXX"
/// PostgresConnector.port = 5432
public struct PostgresConnector {

	public static var host: String		= ""
	public static var username: String	= ""
	public static var password: String	= ""
	public static var database: String	= ""
	public static var port: Int			= 5432

	public static var quiet: Bool		= false

	private init(){}

}

protocol CodableArray {}
extension Array : CodableArray where Element: Codable {}
protocol CodableOptional {}
extension Optional : CodableOptional where Wrapped: Codable {}
protocol CodableArrayOptional {}
extension Optional : CodableArrayOptional where Wrapped: CodableArray {}

public protocol PostgresStringRepresentable {
  var rawValue: String { get }
}
protocol PostgresStringRepresentableArray {}
protocol PostgresStringRepresentableOptional {}
protocol PostgresStringRepresentableArrayOptional {}
extension Array : PostgresStringRepresentableArray where Element: PostgresStringRepresentable {}
extension Optional : PostgresStringRepresentableOptional where Wrapped: PostgresStringRepresentable {}
extension Optional : PostgresStringRepresentableArrayOptional where Wrapped: PostgresStringRepresentableArray {}

/// SuperClass that inherits from the foundation "StORM" class.
/// Provides PosgreSQL-specific ORM functionality to child classes
open class PostgresStORM: StORM, StORMProtocol {
  var table_name: String? = nil

	/// Table that the child object relates to in the database.
	/// Defined as "open" as it is meant to be overridden by the child class.
  open func table() -> String {
    guard let table_name = self.table_name else {
      let m = "\(Mirror(reflecting: self).subjectType)"

      let pattern = "([a-z0-9])([A-Z])"

      let regex = try! NSRegularExpression(pattern: pattern, options: [])
      let range = NSRange(location: 0, length: m.count)
      self.table_name = regex.stringByReplacingMatches(in: m, options: [], range: range, withTemplate: "$1_$2").lowercased() + "s"

      return self.table_name!
    }

    return table_name
	}

	/// Empty initializer
	override public init() {
		super.init()
	}

  private static func printDebug(_ statement: String, _ type: String, _ params: [String] = [], forcePrint: Bool?) {
    let output: Bool
    if let forcePrint = forcePrint { output = forcePrint }
    else { output = StORMdebug }

		if output {
      let ending: String
      if params.count == 0 {
        ending = ""
      } else {
        ending = " | \(params.joined(separator: ", "))"
      }
      LogFile.debug("\(type): \(statement)\(ending)", logFile: "./StORMlog.txt")
    }
	}

  public static func printInfo(_ statement: String, _ type: String, logFile: String, forcePrint: Bool?) {
    let output: Bool
    if let forcePrint = forcePrint { output = forcePrint }
    else { output = StORMdebug }

    if output {
      LogFile.info("\(type): \(statement)", logFile: "./StORMlog.txt")
    }
  }

	// Internal function which executes statements, with parameter binding
	// Returns raw result
	@discardableResult
	func exec(_ statement: String, params: [String], forcePrint: Bool?) throws -> PGResult {
		return try ConnectionPool.dispatch(forcePrint: forcePrint, closure: { conn in
      conn.statement = statement

      PostgresStORM.printDebug(statement, "Execute", forcePrint: forcePrint)
      let result = conn.server.exec(statement: statement, params: params)

      // set exec message
      self.errorMsg = conn.server.errorMessage().trimmingCharacters(in: .whitespacesAndNewlines)
      if self.isError() {
        PostgresStORM.printInfo(self.errorMsg, "Error msg", logFile: "./StORMlog.txt", forcePrint: forcePrint)
        throw StORMError.error(self.errorMsg)
      }

      return result
    }).waitResult().flatten().unwrap()
	}

  override open func modifyValue(_ v: Any, forKey k: String) -> Any {
    return v
  }

	// Internal function which executes statements, with parameter binding
	// Returns a processed row set
	@discardableResult
	func execRows(_ statement: String, params: [String], forcePrint: Bool?) throws -> [StORMRow] {
    return try ConnectionPool.dispatch(forcePrint: forcePrint, closure: { conn in
      conn.statement = statement

      PostgresStORM.printDebug(statement, "Request Rows", params, forcePrint: forcePrint)
      let result = conn.server.exec(statement: statement, params: params)

      // set exec message
      self.errorMsg = conn.server.errorMessage().trimmingCharacters(in: .whitespacesAndNewlines)
      if self.isError() {
        PostgresStORM.printInfo(self.errorMsg, "Error msg", logFile: "./StORMlog.txt", forcePrint: forcePrint)
        throw StORMError.error(self.errorMsg)
      }

      let resultRows = self.parseRows(result)
      PostgresStORM.printDebug(resultRows.map{ "\($0.data)" }.joined(separator: ","), "Response", forcePrint: forcePrint)
      return resultRows
    }).waitResult().flatten().unwrap()
	}


	func isError() -> Bool {
		if errorMsg.contains(string: "ERROR"), !PostgresConnector.quiet {
			print(errorMsg)
			return true
		}
		return false
	}


	/// Generic "to" function
	/// Defined as "open" as it is meant to be overridden by the child class.
	///
	/// Sample usage:
	///		id				= this.data["id"] as? Int ?? 0
	///		firstname		= this.data["firstname"] as? String ?? ""
	///		lastname		= this.data["lastname"] as? String ?? ""
	///		email			= this.data["email"] as? String ?? ""
	open func to(_ this: StORMRow) {
	}

	/// Generic "makeRow" function
	/// Defined as "open" as it is meant to be overridden by the child class.
	open func makeRow() {
		guard self.results.rows.count > 0 else {
			return
		}
		self.to(self.results.rows[0])
	}

	/// Standard "Save" function.
	/// Designed as "open" so it can be overriden and customized.
	/// If an ID has been defined, save() will perform an updae, otherwise a new document is created.
	/// On error can throw a StORMError error.

	open func save(forcePrint: Bool?) throws {
		do {
			if keyIsEmpty() {
				try insert(asData(1), forcePrint: forcePrint)
			} else {
				let (idname, idval) = firstAsKey()
				try update(data: asData(1), idName: idname, idValue: idval, forcePrint: forcePrint)
			}
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

	/// Alternate "Save" function.
	/// This save method will use the supplied "set" to assign or otherwise process the returned id.
	/// Designed as "open" so it can be overriden and customized.
	/// If an ID has been defined, save() will perform an updae, otherwise a new document is created.
	/// On error can throw a StORMError error.

	open func save(forcePrint: Bool?, set: (_ id: Any)->Void) throws {
    // LogFile.debug("\(keyIsEmpty())", logFile: "./StORMlog.txt")

    do {
			if keyIsEmpty() {
				let setId = try insert(asData(1), forcePrint: forcePrint)
				set(setId)
			} else {
				let (idname, idval) = firstAsKey()
				try update(data: asData(1), idName: idname, idValue: idval, forcePrint: forcePrint)
			}
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

	/// Unlike the save() methods, create() mandates the addition of a new document, regardless of whether an ID has been set or specified.

  open func create(forcePrint: Bool?) throws {
		do {
			try insert(asData(), forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}


	/// Table Creation (alias for setup)

  public static func determineType(_ t: Any.Type, at: Int = 1) -> String {
    if (t == Int.self || t == Int?.self) && at == 0 {
      return "serial"
    } else if t == Int.self || t == Int?.self {
      return "int"
    } else if t == Bool.self || t == Bool?.self {
      return "bool"
    } else if t == [String].self || t == [String]?.self ||
      t is PostgresStringRepresentableArray.Type || t is PostgresStringRepresentableArrayOptional.Type {
      return "text[]";
    } else if t == [[String:Any]].self || t == [[String:Any]]?.self {
      return "jsonb[]"
    } else if t == [Int].self || t == [Int]?.self {
      return "int[]"
    } else if t == Double.self || t == Double?.self {
      return "float8"
    } else if t == [Double].self || t == [Double]?.self {
      return "float8[]"
    } else if t == UInt.self || t == UInt8.self || t == UInt16.self || t == UInt32.self || t == UInt64.self ||
      t == UInt?.self || t == UInt8?.self || t == UInt16?.self || t == UInt32?.self || t == UInt64?.self{
      return "bytea"
    } else if t == [String:Any].self || t == [String:Any]?.self {
      return "jsonb"
    } else if t == String.self || t == String?.self ||
      t is PostgresStringRepresentable .Type ||
      t is PostgresStringRepresentableOptional.Type {
      return "text"
    } else if t is CodableArray.Type || t is CodableArrayOptional.Type {
      return "jsonb[]"
    } else if t is Codable.Type || t is CodableOptional.Type {
      return "jsonb"
    } else {
      return "text"
    }
  }

  public static func convertInto(_ val: Any, _ i: inout Int, insert: Bool = false) -> ([String], String) {
    let v: Any
    if let ds = Mirror(reflecting: val).displayStyle {
      if case .optional = ds {
        if let val = Mirror(reflecting: val).children.first {
          v = val.value
        } else {
          return ([], "")
        }
      } else {
        v = val
      }
    } else {
      v = val
    }

    let t = type(of: v).self
    let type1 = determineType(t)

    switch type1 {
    case "jsonb":
      let param: String?
      if t == [String:Any].self || t == [String:Any]?.self {
        param = try? (v as? [String:Any] ?? [:]).jsonEncodedString()
      } else {
        param = (v as? Encodable).flatMap { try? $0.string() }
      }

      i += 1

      return (param.map { [$0] } ?? [ "{}" ], "$\(i)::jsonb")
    case "jsonb[]":
      var params: [String] = []
      var substs: [String] = []

      if t == [[String:Any]].self || t == [[String:Any]]?.self {
        (v as? [[String:Any]]).map{ $0.forEach { json in
          if let jsonString = try? json.jsonEncodedString() {
            params.append(jsonString)
            i += 1
            substs.append("$\(i)::jsonb")
          }}
        }
      } else {
        let encoder = JSONEncoder()
        (v as? [Encodable]).map { $0.forEach { json in
          if let jsonString = try? json.string(using: encoder) {
            params.append(jsonString)
            i += 1
            substs.append("$\(i)::jsonb")
          }}
        }
      }

      return (params, "ARRAY[\(substs.joined(separator: ","))]::jsonb[]")
    case "text[]", "int[]", "bytea[]", "float8[]":
      let subType = type1[type1.startIndex..<type1.index(type1.endIndex, offsetBy: -2)]
      var params: [String] = []
      var substs: [String] = []

      if let v = v as? [PostgresStringRepresentable] {
        v.forEach{
          params.append($0.rawValue)
          i += 1
          substs.append("$\(i)::\(subType)")
        }
      } else {
        (v as! [Any]).forEach {
          params.append(String(describing: $0))
          i += 1
          substs.append("$\(i)::\(subType)")
        }
      }

      return (params, "ARRAY[\(substs.joined(separator: ","))]::\(type1)")

    default:
      i += 1

      if let ds = Mirror(reflecting: v).displayStyle {
        if case .optional = ds {
          if let v = Mirror(reflecting: v).children.first {
            return ([String(describing: v)], "$\(i)::\(type1)")
          }
        }
      }

      if let v = v as? PostgresStringRepresentable {
        return ([v.rawValue], "$\(i)::\(type1)")
      } else {
        return ([String(describing: v)], "$\(i)::\(type1)")
      }
    }
  }

  open func setupTable(_ str: String = "", forcePrint: Bool? = nil) throws {
    try setup(str, forcePrint: forcePrint)
	}

  open func setup(_ str: String = "", forcePrint: Bool? = nil) throws {
		LogFile.info("Running setup: \(table())", logFile: "./StORMlog.txt")
		var createStatement = str
		if str.count == 0 {
			var opt = [String]()
			var keyName = ""
			for child in Mirror(reflecting: self).children {
				guard let key = child.label else {
					continue
				}
				var verbage = ""
				if !key.hasPrefix("internal_") && !key.hasPrefix("_") {
					verbage = "\"\(key)\" "

          verbage += PostgresStORM.determineType(type(of: child.value).self, at: opt.count)

					if opt.count == 0 {
						verbage += " NOT NULL"
						keyName = key
					}
					opt.append(verbage)
				}
			}
			let keyComponent = ", CONSTRAINT \(table())_key PRIMARY KEY (\(keyName)) NOT DEFERRABLE INITIALLY IMMEDIATE"

			createStatement = "CREATE TABLE IF NOT EXISTS \(table()) (\(opt.joined(separator: ", "))\(keyComponent));"
      PostgresStORM.printDebug(createStatement, "Create Statement", forcePrint: forcePrint)

		}
		do {
      try sql(createStatement, params: [], forcePrint: forcePrint)
		} catch {
			LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}
}


