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

/// SuperClass that inherits from the foundation "StORM" class.
/// Provides PosgreSQL-specific ORM functionality to child classes
open class PostgresStORM: StORM, StORMProtocol {

	/// Table that the child object relates to in the database.
	/// Defined as "open" as it is meant to be overridden by the child class.
	open func table() -> String {
		let m = Mirror(reflecting: self)
		return ("\(m.subjectType)").lowercased()
	}

	/// Empty initializer
	override public init() {
		super.init()
	}

	private func printDebug(_ statement: String, _ params: [String]) {
		if StORMdebug { LogFile.debug("StORM Debug: \(statement) : \(params.joined(separator: ", "))", logFile: "./StORMlog.txt") }
	}

	// Internal function which executes statements, with parameter binding
	// Returns raw result
	@discardableResult
	func exec(_ statement: String, params: [String]) throws -> PGResult {
		let thisConnection = PostgresConnect(
			host:		PostgresConnector.host,
			username:	PostgresConnector.username,
			password:	PostgresConnector.password,
			database:	PostgresConnector.database,
			port:		PostgresConnector.port
		)

		thisConnection.open()
		if thisConnection.state == .bad {
			error = .connectionError
			throw StORMError.error("Connection Error")
		}
		thisConnection.statement = statement

		printDebug(statement, params)
		let result = thisConnection.server.exec(statement: statement, params: params)

		// set exec message
		errorMsg = thisConnection.server.errorMessage().trimmingCharacters(in: .whitespacesAndNewlines)
		if isError() {
      if StORMdebug { LogFile.info("Error msg: \(errorMsg)", logFile: "./StORMlog.txt") }
			thisConnection.server.close()
			throw StORMError.error(errorMsg)
		}
		thisConnection.server.close()
		return result
	}

  override open func modifyValue(_ v: Any, forKey k: String) -> Any {
//    if v is [String:Any] {
//
//      let jsonData = try? JSONSerialization.data(withJSONObject: v, options: [])
//
//      return String(data: jsonData!, encoding: .utf8)!
//    } else if v is [[String:Any]] {
//      let arrayVals: [String] = (v as! [[String:Any]]).map {
//        let jsonData = try? JSONSerialization.data(withJSONObject: $0, options: [])
//
//        return "\(String(data: jsonData!, encoding: .utf8)!)::jsonb"
//      }
//
//      return "ARRAY[\(arrayVals.joined(separator: ","))]"
//
//    } else if v is [Any] {
//      let arrayVals: [String] = (v as! [Any]).map {"\(String(describing: $0))"}
//
//      return "ARRAY[\(arrayVals.joined(separator: ","))]"
//    } else {
//      return String(describing: v)
//    }
    return v
  }

	// Internal function which executes statements, with parameter binding
	// Returns a processed row set
	@discardableResult
	func execRows(_ statement: String, params: [String]) throws -> [StORMRow] {
		let thisConnection = PostgresConnect(
			host:		PostgresConnector.host,
			username:	PostgresConnector.username,
			password:	PostgresConnector.password,
			database:	PostgresConnector.database,
			port:		PostgresConnector.port
		)

		thisConnection.open()
		if thisConnection.state == .bad {
			error = .connectionError
			throw StORMError.error("Connection Error")
		}
		thisConnection.statement = statement

		printDebug(statement, params)
		let result = thisConnection.server.exec(statement: statement, params: params)
//    LogFile.debug("\(result)", logFile: "./StORMlog.txt")

		// set exec message
		errorMsg = thisConnection.server.errorMessage().trimmingCharacters(in: .whitespacesAndNewlines)
		if isError() {
      if StORMdebug { LogFile.info("Error msg: \(errorMsg)", logFile: "./StORMlog.txt") }
			thisConnection.server.close()
			throw StORMError.error(errorMsg)
		}

		let resultRows = parseRows(result)

    LogFile.debug("Response: \(resultRows.map{ "\($0.data)" }.joined(separator: ","))", logFile: "./StORMlog.txt")
		//		result.clear()
		thisConnection.server.close()
		return resultRows
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

	open func save() throws {
		do {
			if keyIsEmpty() {
				try insert(asData(1))
			} else {
				let (idname, idval) = firstAsKey()
				try update(data: asData(1), idName: idname, idValue: idval)
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

	open func save(set: (_ id: Any)->Void) throws {
    LogFile.debug("\(keyIsEmpty())", logFile: "./StORMlog.txt")

    do {
			if keyIsEmpty() {
				let setId = try insert(asData(1))
				set(setId)
			} else {
				let (idname, idval) = firstAsKey()
				try update(data: asData(1), idName: idname, idValue: idval)
			}
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

	/// Unlike the save() methods, create() mandates the addition of a new document, regardless of whether an ID has been set or specified.

	override open func create() throws {
		do {
			try insert(asData())
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}


	/// Table Creation (alias for setup)

	open func setupTable(_ str: String = "") throws {
		try setup(str)
	}

	/// Table Creation
	/// Requires the connection to be configured, as well as a valid "table" property to have been set in the class

//  func getType(_ value: Any) -> String {
//    let mirror = Mirror(reflecting: value)
//
//    guard let style = mirror.displayStyle else {
//      if value is Int {
//        return "int"
//      } else if value is Bool {
//        return "bool"
//      }else if value is String {
//        return "text"
//      } else if value is [String:Any] {
//        return "jsonb"
//      } else {
//        return "text"
//      }
//    }
//
//    switch style {
//    case .struct,
//         .class:
//      print("class")
//      var children: [String] = []
//      for child in mirror.children {
//        children.append(getType(child.value))
//      }
//      return "{\(children.joined(separator: ", "))}"
//    case .collection:
//      print("\(mirror.displayStyle!)")
//      if value is [Int] {
//        return "int[]"
//      } else if value is [Bool] {
//        return "bool[]"
//      } else if value is [String] {
//        return "text[]"
//      } else if value is [[String:Any]] {
//        return "jsonb[]"
//      } else {
//        return "text[]"
//      }
//    case .dictionary:
//      if value is [String:Any] {
//        return "jsonb"
//      } else {
//        return "text"
//      }
//    default:
//      return ""
//    }
//  }

  open func setup(_ str: String = "") throws {
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
					verbage = "\(key.lowercased()) "

          let t = type(of: child.value).self

					if t == Int.self && opt.count == 0 {
						verbage += "serial"
					} else if t == Int.self || t == Int?.self {
						verbage += "int8"
					} else if t == Bool.self || t == Bool?.self {
						verbage += "bool"
          } else if t == [String].self {
            verbage += "text[]";
          } else if t == [[String:Any]].self {
            verbage += "jsonb[]"
          } else if t == [Int].self {
            verbage += "int[]"
          } else if t == Double.self || t == Double?.self {
						verbage += "float8"
					} else if t == UInt.self || t == UInt8.self || t == UInt16.self || t == UInt32.self || t == UInt64.self ||
              t == UInt?.self || t == UInt8?.self || t == UInt16?.self || t == UInt32?.self || t == UInt64?.self{
						verbage += "bytea"
          } else if t == [String:Any].self || t == [String:Any]?.self {
            verbage += "jsonb"
          } else if t == String.self || t == String?.self {
            verbage += "text"
          } else if t is CodableArray.Type {
            verbage += "jsonb[]"
          } else if t is Codable.Type {
            verbage += "jsonb"
          } else {
						verbage += "text"
					}
					if opt.count == 0 {
						verbage += " NOT NULL"
						keyName = key
					}
					opt.append(verbage)
				}
			}
			let keyComponent = ", CONSTRAINT \(table())_key PRIMARY KEY (\(keyName)) NOT DEFERRABLE INITIALLY IMMEDIATE"

			createStatement = "CREATE TABLE IF NOT EXISTS \(table()) (\(opt.joined(separator: ", "))\(keyComponent));"
			if StORMdebug { LogFile.info("createStatement: \(createStatement)", logFile: "./StORMlog.txt") }

		}
		do {
			try sql(createStatement, params: [])
		} catch {
			LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}
}


