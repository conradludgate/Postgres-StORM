//
//  Insert.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2016-09-24.
//
//

import Foundation
import StORM
import PerfectLogger
import PerfectLib

protocol EncodableArray {}
extension Array : EncodableArray where Element: Encodable {}
protocol OptionalEncodable {}
extension Optional : OptionalEncodable where Wrapped: Encodable {}

/// Performs insert functions as an extension to the main class.
extension PostgresStORM {

	/// Insert function where the suppled data is in [(String, Any)] format.
	@discardableResult
	public func insert(_ data: [(String, Any)]) throws -> Any {

		var keys = [String]()
		var vals = [Any]()
		for i in data {
			keys.append(i.0)
			vals.append(i.1)
		}
		do {
			return try insert(cols: keys, params: vals)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

	/// Insert function where the suppled data is in [String: Any] format.
	public func insert(_ data: [String: Any]) throws -> Any {

		var keys = [String]()
		var vals = [Any]()
		for i in data.keys {
			keys.append(i)
			vals.append(data[i]!)
		}

		do {
			return try insert(cols: keys, params: vals)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}
	

	/// Insert function where the suppled data is in matching arrays of columns and parameter values.
	public func insert(cols: [String], params: [Any]) throws -> Any {
		let (idname, _) = firstAsKey()
		do {
			return try insert(cols: cols, params: params, idcolumn: idname)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

  func encode<T: Codable> (_ t: T, with encoder: JSONEncoder) -> String? {
    return (try? encoder.encode(t)).map{ String(data: $0, encoding: .utf8)! }
  }

	/// Insert function where the suppled data is in matching arrays of columns and parameter values, as well as specifying the name of the id column.
	public func insert(cols: [String], params: [Any], idcolumn: String) throws -> Any {

    var paramString = [String]()
    var substString = [String]()

    var newcols: [String] = []

    var i = 0
    params.enumerated().forEach { (index, param) in
      let (params, subst) = PostgresStORM.convertInto(param, &i)
      if params.count > 0 {
        paramString += params
        substString.append(subst)
        newcols.append(cols[index])
      }
    }

		//"\"" + columns.joined(separator: "\",\"") + "\""

		let colsjoined = "\"" + newcols.joined(separator: "\",\"") + "\""
		let str = "INSERT INTO \(self.table()) (\(colsjoined)) VALUES(\(substString.joined(separator: ","))) RETURNING \"\(idcolumn)\""

		do {
			let response = try exec(str, params: paramString)
			return parseRows(response)[0].data[idcolumn]!
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}

	}


}
