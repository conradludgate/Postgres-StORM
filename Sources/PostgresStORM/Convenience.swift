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
  public func data(using encoder: JSONEncoder = JSONEncoder()) throws -> Data {
    return try encoder.encode(self)
  }

  public func string(using encoder: JSONEncoder = JSONEncoder()) throws -> String {
    return try String(data: encoder.encode(self), encoding: .utf8)!
  }
}

extension Decodable {
  public static func fromJson(_ json: Any?, using decoder: JSONDecoder = JSONDecoder()) -> Self? {

    guard let dict = (json as? [String:Any]), let jsonString = try? dict.jsonEncodedString() else {
      return nil
    }

    let jsonData = jsonString.data(using: .utf8)!
    return try? decoder.decode(Self.self, from: jsonData)
  }

  public static func fromJsonArray(_ jsonarray: Any?, using decoder: JSONDecoder = JSONDecoder()) -> [Self] {
    return (jsonarray  as? [[String:Any]] ?? []).compactMap { Self.fromJson($0, using: decoder) }
  }
}

/// Convenience methods extending the main class.
extension PostgresStORM {

	/// Deletes one row, with an id.
	/// Presumes first property in class is the id.
	public func delete(forcePrint: Bool?) throws {
		let (idname, idval) = firstAsKey()
		do {
			try exec(deleteSQL(self.table(), idName: idname), params: [String(describing: idval)], forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}
	}

	/// Deletes one row, with the id as set.
	public func delete(_ id: Any, forcePrint: Bool?) throws {
		let (idname, _) = firstAsKey()
		do {
			try exec(deleteSQL(self.table(), idName: idname), params: [String(describing: id)], forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}
	}

	/// Retrieves a single row with the supplied ID.
	public func get(_ id: Any, forcePrint: Bool?) throws {
		let (idname, _) = firstAsKey()
		do {
			try select(whereclause: "\"\(idname)\" = $1", params: [id], orderby: [], forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw error
		}
	}

	/// Retrieves a single row with the ID as set.
	public func get(forcePrint: Bool?) throws {
		let (idname, idval) = firstAsKey()
		do {
			try select(whereclause: "\"\(idname)\" = $1", params: ["\(idval)"], orderby: [], forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw error
		}
	}

	/// Performs a find on matching column name/value pairs.
	/// An optional `cursor:StORMCursor` object can be supplied to determine pagination through a larger result set.
	/// For example, `try find([("username","joe")])` will find all rows that have a username equal to "joe"
  public func find(_ data: [(String, Any)], cursor: StORMCursor = StORMCursor(), forcePrint: Bool?) throws {
		let (idname, _) = firstAsKey()

		var paramsString = [String]()
		var set = [String]()
    var i = 0
		data.forEach { (key, val) in
      let (params, subst) = PostgresStORM.convertInto(val, &i)

			paramsString += params

      if params.count > 1 {
        let unroll = subst.lastIndex(of: ":").map { end in subst[subst.index(subst.startIndex, offsetBy: 6)..<subst.index(end, offsetBy: -2)] } ?? "-1"
        set.append("\(key) IN (\(unroll))")
      } else if params.count == 1 {
        set.append("\(key) = \(subst)")
      }
		}

		do {
			try select(whereclause: set.joined(separator: " AND "), params: paramsString, orderby: [idname], cursor: cursor, forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw error
		}

	}

  public func findCount(_ data: [(String, Any)], forcePrint: Bool?) throws -> Int {
    var paramsString = [String]()
    var set = [String]()
    var i = 0
    data.forEach { (key, val) in
      let (params, subst) = PostgresStORM.convertInto(val, &i)

      paramsString += params

      if params.count > 1 {
        let unroll = subst.lastIndex(of: ":").map { end in subst[subst.index(subst.startIndex, offsetBy: 6)..<subst.index(end, offsetBy: -2)] } ?? "-1"
        set.append("\(key) IN (\(unroll))")
      } else if params.count == 1 {
        set.append("\(key) = \(subst)")
      }
    }

    do {
      return try count(whereclause: set.joined(separator: " AND "), params: paramsString, forcePrint: forcePrint)
    } catch {
      LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
      throw error
    }
  }


	/// Performs a find on mathing column name/value pairs.
	/// An optional `cursor:StORMCursor` object can be supplied to determine pagination through a larger result set.
	/// For example, `try find(["username": "joe"])` will find all rows that have a username equal to "joe"
  public func find(_ data: [String: Any], cursor: StORMCursor = StORMCursor(), forcePrint: Bool?) throws {
		let (idname, _) = firstAsKey()

		var paramsString = [String]()
		var set = [String]()
    var i = 0
    data.forEach { (key, val) in
      let (params, subst) = PostgresStORM.convertInto(val, &i)

      paramsString += params

      if params.count > 1 {
        let unroll = subst.lastIndex(of: ":").map { end in subst[subst.index(subst.startIndex, offsetBy: 6)..<subst.index(end, offsetBy: -2)] } ?? "-1"
        set.append("\(key) IN (\(unroll))")
      } else if params.count == 1 {
        set.append("\(key) = \(subst)")
      }
    }

		do {
      try select(whereclause: set.joined(separator: " AND "), params: paramsString, orderby: [idname], cursor: cursor, forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw error
		}

	}

  public func findCount(_ data: [String:Any], forcePrint: Bool?) throws -> Int {
    var paramsString = [String]()
    var set = [String]()
    var i = 0
    data.forEach { (key, val) in
      let (params, subst) = PostgresStORM.convertInto(val, &i)

      paramsString += params

      if params.count > 1 {
        let unroll = subst.lastIndex(of: ":").map { end in subst[subst.index(subst.startIndex, offsetBy: 6)..<subst.index(end, offsetBy: -2)] } ?? "-1"
        set.append("\(key) IN (\(unroll))")
      } else if params.count == 1 {
        set.append("\(key) = \(subst)")
      }
    }

    do {
      return try count(whereclause: set.joined(separator: " AND "), params: paramsString, forcePrint: forcePrint)
    } catch {
      LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
      throw error
    }
  }

}
