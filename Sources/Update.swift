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
//      LogFile.info("\(index): \(type(of: param).self) = \(param)", logFile: "./StORMlog.txt")

      if type(of: param).self == [String:Any].self {

        let jsonData = try? JSONSerialization.data(withJSONObject: param, options: [])

        paramString.append(String(data: jsonData!, encoding: .utf8)!)
        substString.append("\"\(cols[index].lowercased())\" = $\(i)::jsonb")
        i += 1
      } else if type(of: param).self == [[String:Any]].self {
        var arrayVals: [String] = []

        (param as! [[String:Any]]).forEach { json in

          let jsonData = try? JSONSerialization.data(withJSONObject: json, options: [])

          paramString.append(String(data: jsonData!, encoding: .utf8)!)
          arrayVals.append("$\(i)::jsonb")
          i += 1
        }

        substString.append("\"\(cols[index].lowercased())\" = ARRAY[\(arrayVals.joined(separator: ","))]::jsonb[]")

      } else if param is [Any] {
        var arrayVals: [String] = []

        (param as! [Any]).forEach { elem in
          paramString.append(String(describing: elem))
          arrayVals.append("$\(i)")
          i += 1
        }

        let type: String
        if type(of: param).self == [String].self {
          type = "text[]"
        } else if type(of: param).self == [Int].self {
          type = "int[]"
        } else {
          type = "text[]"
        }

        substString.append("\"\(cols[index].lowercased())\" = ARRAY[\(arrayVals.joined(separator: ","))]::\(type)")
      } else {
        paramString.append(String(describing: param))
        substString.append("\"\(cols[index].lowercased())\" = $\(i)")
        i += 1
      }
    }

    paramString.append(String(describing: idValue))

		let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) WHERE \"\(idName.lowercased())\" = $\(i)"

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
