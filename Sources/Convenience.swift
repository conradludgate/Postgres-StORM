//
//  Convenience.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2016-10-04.
//
//


import StORM
import PerfectLogger
import Foundation

extension Encodable {
  func data(using encoder: JSONEncoder = JSONEncoder()) throws -> Data {
    return try encoder.encode(self)
  }

  func string(using encoder: JSONEncoder = JSONEncoder()) throws -> String {
    return try String(data: encoder.encode(self), encoding: .utf8)!
  }
}

extension Decodable {
  static func fromJson(_ json: Any?, using decoder: JSONDecoder = JSONDecoder()) -> Self? {
    guard let jsonString = try? (json as? [String:Any] ?? [:]).jsonEncodedString() else {
      return nil
    }
    let jsonData = jsonString.data(using: .utf8)!
    return try? decoder.decode(Self.self, from: jsonData)
  }

  static func fromJsonArray(_ jsonarray: Any?, using decoder: JSONDecoder = JSONDecoder()) -> [Self] {
    return (jsonarray  as? [[String:Any]] ?? []).compactMap { Self.fromJson($0, using: decoder) }
  }
}

/// Convenience methods extending the main class.
extension PostgresStORM {

	/// Deletes one row, with an id.
	/// Presumes first property in class is the id.
	public func delete() throws {
		let (idname, idval) = firstAsKey()
		do {
			try exec(deleteSQL(self.table(), idName: idname.lowercased()), params: [String(describing: idval)])
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}
	}

	/// Deletes one row, with the id as set.
	public func delete(_ id: Any) throws {
		let (idname, _) = firstAsKey()
		do {
			try exec(deleteSQL(self.table(), idName: idname.lowercased()), params: [String(describing: id)])
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}
	}

	/// Retrieves a single row with the supplied ID.
	public func get(_ id: Any) throws {
		let (idname, _) = firstAsKey()
		do {
			try select(whereclause: "\"\(idname.lowercased())\" = $1", params: [id], orderby: [])
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw error
		}
	}

	/// Retrieves a single row with the ID as set.
	public func get() throws {
		let (idname, idval) = firstAsKey()
		do {
			try select(whereclause: "\"\(idname.lowercased())\" = $1", params: ["\(idval)"], orderby: [])
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw error
		}
	}

	/// Performs a find on matching column name/value pairs.
	/// An optional `cursor:StORMCursor` object can be supplied to determine pagination through a larger result set.
	/// For example, `try find([("username","joe")])` will find all rows that have a username equal to "joe"
	public func find(_ data: [(String, Any)], cursor: StORMCursor = StORMCursor()) throws {
		let (idname, _) = firstAsKey()

		var paramsString = [String]()
		var set = [String]()
    var i = 0
		data.forEach { (key, val) in
      let (params, subst) = convertInto(val, &i)

			paramsString += params

      if params.count > 1 {
        set.append("\(key.lowercased()) IN (\(subst[6..<subst.lastIndex(of: "]::")]))")
      } else {
        set.append("\(key.lowercased()) = \(subst)")
      }
		}

		do {
			try select(whereclause: set.joined(separator: " AND "), params: paramsString, orderby: [idname], cursor: cursor)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw error
		}

	}


	/// Performs a find on mathing column name/value pairs.
	/// An optional `cursor:StORMCursor` object can be supplied to determine pagination through a larger result set.
	/// For example, `try find(["username": "joe"])` will find all rows that have a username equal to "joe"
	public func find(_ data: [String: Any], cursor: StORMCursor = StORMCursor()) throws {
		let (idname, _) = firstAsKey()

		var paramsString = [String]()
		var set = [String]()
    var i = 0
    data.forEach { (key, val) in
      let (params, subst) = convertInto(val, &i)

      paramsString += params

      if params.count > 1 {
        set.append("\(key.lowercased()) IN (select(unnest(\(subst)))")
      } else {
        set.append("\(key.lowercased()) = \(subst)")
      }
    }

		do {
			try select(whereclause: set.joined(separator: " AND "), params: paramsString, orderby: [idname], cursor: cursor)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw error
		}

	}

}
