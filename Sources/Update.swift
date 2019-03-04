//
//  Update.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2016-09-24.
//
//

import StORM
import PerfectLogger
import Foundation

/// Extends the main class with update functions.
extension PostgresStORM {

	/// Updates the row with the specified data.
	/// This is an alternative to the save() function.
	/// Specify matching arrays of columns and parameters, as well as the id name and value.
	@discardableResult
	public func update(cols: [String], params: [Any], idName: String, idValue: Any) throws -> Bool {
    var paramString = [String]()
    var substString = [String]()

    var i = 1
    params.enumerated().forEach { (index, param) in
//      LogFile.info("\(cols[index].lowercased()): \(type(of: param).self) = \(param)", logFile: "./StORMlog.txt")

      let t = type(of: param)

      if t.self == [String:Any].self || t.self == [String:Any]?.self {

        let jsonData = try? (param as? [String:Any] ?? [:]).jsonEncodedString()

        paramString.append(jsonData!)
        substString.append("\"\(cols[index].lowercased())\" = $\(i)::jsonb")
        i += 1

      } else if t.self == [[String:Any]].self {
        var arrayVals: [String] = []

        (param as! [[String:Any]]).forEach { json in
          let jsonData = try? json.jsonEncodedString()

          paramString.append(jsonData!)
          arrayVals.append("$\(i)::jsonb")
          i += 1
        }

        substString.append("\"\(cols[index].lowercased())\" = ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")

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
            substString.append("\"\(cols[index].lowercased())\" = ARRAY[]::jsonb[]")
            return
          }

          let encoder = JSONEncoder()
          encodablearray.forEach { encodable in

            paramString.append((try? encodable.string(using: encoder)) ?? "{}")
            arrayVals.append("$\(i)::jsonb")
            i += 1
          }

          substString.append("\"\(cols[index].lowercased())\" = ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")
          return
        } else if t.self == [[String:Any]].self {

          let jsonarray = array as! [[String:Any]]

          guard jsonarray.count > 0 else {
            substString.append("\"\(cols[index].lowercased())\" = ARRAY[]::jsonb[]")
            return
          }

          jsonarray.forEach { json in

            paramString.append((try? json.jsonEncodedString()) ?? "{}")
            arrayVals.append("$\(i)::jsonb")
            i += 1
          }

          substString.append("\"\(cols[index].lowercased())\" = ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")
          return
        } else {
          type = "text[]"
        }

        array.forEach { elem in
          paramString.append(String(describing: elem))
          arrayVals.append("$\(i)")
          i += 1
        }

        substString.append("\"\(cols[index].lowercased())\" = ARRAY[\(arrayVals.joined(separator: ","))]::\(type)")
      } else if t.self == String.self || t.self == String?.self {
        paramString.append((param as? String) ?? "")
        substString.append("\"\(cols[index].lowercased())\" = $\(i)::text")
        i += 1

      } else if t.self == Int.self || t.self == Int?.self {
        paramString.append((param as? Int).map{ String(describing: $0) } ?? "")
        substString.append("\"\(cols[index].lowercased())\" = $\(i)::int")
        i += 1

      } else if t.self is Encodable.Type || t.self is OptionalEncodable.Type {
        paramString.append((param as? Encodable).map { (try? $0.string()) ?? "{}" } ?? "{}")
        substString.append("\"\(cols[index].lowercased())\" = $\(i)::jsonb")
        i += 1
      } else {
        paramString.append(String(describing: param))
        substString.append("\"\(cols[index].lowercased())\" = $\(i)")
        i += 1
      }
    }
//
//
//      if type(of: param).self == [String:Any].self {
//
//        let jsonData = try? JSONSerialization.data(withJSONObject: param, options: [])
//
//        paramString.append(String(data: jsonData!, encoding: .utf8)!)
//        substString.append("\"\(cols[index].lowercased())\" = $\(i)::jsonb")
//        i += 1
//
////        print("json: \"\(cols[index].lowercased())\" = $\(i)::jsonb")
//      } else if type(of: param).self == [[String:Any]].self {
//        var arrayVals: [String] = []
//
//        (param as! [[String:Any]]).forEach { json in
//
//          let jsonData = try? JSONSerialization.data(withJSONObject: json, options: [])
//
//          paramString.append(String(data: jsonData!, encoding: .utf8)!)
//          arrayVals.append("$\(i)::jsonb")
//          i += 1
//        }
//
//        substString.append("\"\(cols[index].lowercased())\" = ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")
//
//      } else if let array = param as? [Any] {
//        var arrayVals: [String] = []
//
//        print(t.self)
//        print([Codable.Type].self)
//        let type: String
//        if t.self == [String].self {
//          type = "text[]"
//        } else if t.self == [Int].self {
//          type = "int[]"
//        } else if let jsonarray = array as? [Encodable] {
//          print(t)
//
//          guard jsonarray.count > 0 else {
//            substString.append("ARRAY[]::jsonb[]")
//            return
//          }
//
//          let encoder = JSONEncoder()
//          jsonarray.forEach { json in
//            print(json)
//
//            paramString.append((try? json.string(using: encoder)) ?? "{}")
//            arrayVals.append("$\(i)::jsonb")
//            i += 1
//          }
//
//          substString.append("ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")
//          return
//        } else {
//          type = "text[]"
//        }
//
//        param.forEach { elem in
//          paramString.append(String(describing: elem))
//          arrayVals.append("$\(i)")
//          i += 1
//        }
//
//        substString.append("\"\(cols[index].lowercased())\" = ARRAY[\(arrayVals.joined(separator: ","))]::\(type)")
//      } else {
//        paramString.append(String(describing: param))
//        substString.append("\"\(cols[index].lowercased())\" = $\(i)")
//        i += 1
//      }
//    }

    paramString.append(String(describing: idValue))

    let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) WHERE \"\(idName.lowercased())\" = $\(i)"

    LogFile.debug("Params: \(paramString.joined(separator: ", "))", logFile: "./StORMlog.txt")
    LogFile.debug("Substs: \(substString.joined(separator: ", "))", logFile: "./StORMlog.txt")

		do {
			try exec(str, params: paramString)
		} catch {
			LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}

		return true
	}

	/// Updates the row with the specified data.
	/// This is an alternative to the save() function.
	/// Specify a [(String, Any)] of columns and parameters, as well as the id name and value.
	@discardableResult
	public func update(data: [(String, Any)], idName: String = "id", idValue: Any) throws -> Bool {

		var keys = [String]()
		var vals = [Any]()
		for i in 0..<data.count {
			keys.append(data[i].0.lowercased())
			vals.append(data[i].1)
		}
		do {
			return try update(cols: keys, params: vals, idName: idName, idValue: idValue)
		} catch {
			LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

}
