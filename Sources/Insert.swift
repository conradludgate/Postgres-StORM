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
			keys.append(i.lowercased())
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

    var i = 1
    params.forEach { param in
//      LogFile.info("\(type(of: param).self) = \(param)", logFile: "./StORMlog.txt")

      let t = type(of: param)
//      print(t.self is EncodableArray.Type, t.self is OptionalEncodableArray.Type)

      if t.self == [String:Any].self || t.self == [String:Any]?.self {

        let jsonData = try? (param as? [String:Any] ?? [:]).jsonEncodedString()

        paramString.append(jsonData!)
        substString.append("$\(i)::jsonb")
        i += 1

      } else if t.self == [[String:Any]].self {
        var arrayVals: [String] = []

        (param as! [[String:Any]]).forEach { json in
          let jsonData = try? json.jsonEncodedString()

          paramString.append(jsonData!)
          arrayVals.append("$\(i)::jsonb")
          i += 1
        }

        substString.append("ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")

      } else if let array = param as? [Any] {
        var arrayVals: [String] = []

        let type: String
        if t.self == [String].self {
          type = "text[]"
        } else if t.self == [Int].self {
          type = "int[]"
        } else if t.self is EncodableArray.Type {

          let encodablearray = array as! [Encodable]

          guard encodablearray.count > 0 else {
            substString.append("ARRAY[]::jsonb[]")
            return
          }

          let encoder = JSONEncoder()
          encodablearray.forEach { encodable in
            
            paramString.append((try? encodable.string(using: encoder)) ?? "{}")
            arrayVals.append("$\(i)::jsonb")
            i += 1
          }

          substString.append("ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")
          return
        } else if t.self == [[String:Any]].self {

          let jsonarray = array as! [[String:Any]]

          guard jsonarray.count > 0 else {
            substString.append("ARRAY[]::jsonb[]")
            return
          }

          jsonarray.forEach { json in

            paramString.append((try? json.jsonEncodedString()) ?? "{}")
            arrayVals.append("$\(i)::jsonb")
            i += 1
          }

          substString.append("ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")
          return
        } else {
          type = "text[]"
        }

        array.forEach { elem in
          paramString.append(String(describing: elem))
          arrayVals.append("$\(i)")
          i += 1
        }

        substString.append("ARRAY[\(arrayVals.joined(separator: ","))]::\(type)")
      } else if t.self == String.self || t.self == String?.self {
        paramString.append((param as? String) ?? "")
        substString.append("$\(i)::text")
        i += 1

      } else if t.self == Int.self || t.self == Int?.self {
        paramString.append((param as? Int).map{ String(describing: $0) } ?? "")
        substString.append("$\(i)::int")
        i += 1

      } else if t.self is Encodable.Type || t.self is OptionalEncodable.Type {
        paramString.append((param as? Encodable).map { (try? $0.string()) ?? "{}" } ?? "{}")
        substString.append("$\(i)::jsonb")
        i += 1
      } else {
        paramString.append(String(describing: param))
        substString.append("$\(i)")
        i += 1
      }
    }

		//"\"" + columns.joined(separator: "\",\"") + "\""

		let colsjoined = "\"" + cols.joined(separator: "\",\"") + "\""
		let str = "INSERT INTO \(self.table()) (\(colsjoined.lowercased())) VALUES(\(substString.joined(separator: ","))) RETURNING \"\(idcolumn.lowercased())\""

		do {
			let response = try exec(str, params: paramString)
			return parseRows(response)[0].data[idcolumn.lowercased()]!
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}

	}


}
