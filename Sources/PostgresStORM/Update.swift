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

    var i = 0
    params.enumerated().forEach { (index, param) in
      let (params, subst) = PostgresStORM.convertInto(param, &i)
      if params.count > 0 {
        paramString += params
        substString.append("\"\(cols[index])\" = \(subst)")
      }
    }

    paramString.append(String(describing: idValue))

    let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) WHERE \"\(idName)\" = $\(i + 1)"

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
			keys.append(data[i].0)
			vals.append(data[i].1)
		}
		do {
			return try update(cols: keys, params: vals, idName: idName, idValue: idValue)
		} catch {
			LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

  @discardableResult
  public func push(cols: [String], params: [Any], idName: String, idValue: Any) throws -> Bool {
    var paramString = [String]()
    var substString = [String]()

    var i = 0
    params.enumerated().forEach { (index, param) in
      let (params, subst) = PostgresStORM.convertInto(param, &i)

      paramString += params
      if params.count > 1 {
        substString.append("\"\(cols[index])\" = array_cat(\"\(cols[index])\", \(subst))")
      } else if params.count == 1 {
        substString.append("\"\(cols[index])\" = array_append(\"\(cols[index])\", \(subst))")
      }
    }

    paramString.append(String(describing: idValue))

    let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) WHERE \"\(idName)\" = $\(i + 1)"

    do {
      try exec(str, params: paramString)
    } catch {
      LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
      self.error = StORMError.error("\(error)")
      throw error
    }

    return true
  }

  @discardableResult
  public func push(data: [(String, Any)], idName: String = "id", idValue: Any) throws -> Bool {

    var keys = [String]()
    var vals = [Any]()
    for i in 0..<data.count {
      keys.append(data[i].0)
      vals.append(data[i].1)
    }
    do {
      return try push(cols: keys, params: vals, idName: idName, idValue: idValue)
    } catch {
      LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
      throw StORMError.error("\(error)")
    }
  }


  @discardableResult
  public func pull(cols: [String], params: [Any], idName: String, idValue: Any) throws -> Bool {
    var paramString = [String]()
    var substString = [String]()

    var i = 0
    params.enumerated().forEach { (index, param) in
      let (params, subst) = PostgresStORM.convertInto(param, &i)
      paramString += params

      if params.count > 1 {
        substString.append("\"\(cols[index])\" = (select array_agg(elements) from (select unnest(\"\(cols[index])\") except select unnest(\(subst))) t (elements))")
      } else if params.count == 1 {
        let type = subst.lastIndex(of: ":").map { index in subst[subst.index(index, offsetBy: -1)..<subst.endIndex] } ?? "::text"
        substString.append("\"\(cols[index])\" = (select array_agg(elements) from (select unnest(\"\(cols[index])\") except select unnest(ARRAY[\(subst)]\(type)[])) t (elements))")
      }
    }

    paramString.append(String(describing: idValue))

    let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) WHERE \"\(idName)\" = $\(i + 1)"

    do {
      try exec(str, params: paramString)
    } catch {
      LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
      self.error = StORMError.error("\(error)")
      throw error
    }

    return true
  }

  @discardableResult
  public func pull(data: [(String, Any)], idName: String = "id", idValue: Any) throws -> Bool {

    var keys = [String]()
    var vals = [Any]()
    for i in 0..<data.count {
      keys.append(data[i].0)
      vals.append(data[i].1)
    }
    do {
      return try push(cols: keys, params: vals, idName: idName, idValue: idValue)
    } catch {
      LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
      throw StORMError.error("\(error)")
    }
  }

//  @discardableResult
//  public func update(_ query: [String:Any], set: [String:Any], pull: [String:Any], push: [String:Any], addToSet: [String:Any], returning: [String] = ["*"]) throws -> Bool {
//    var paramString = [String]()
//    var substString = [String]()
//
//    var i = 0
//
//    set.forEach { (col, param) in
//      let (params, subst) = PostgresStORM.convertInto(param, &i)
//      paramString += params
//      substString.append("\"\(col)\" = \(subst)")
//    }
//
//    pull.forEach { (col, param) in
//      let (params, subst) = PostgresStORM.convertInto(param, &i)
//      paramString += params
//
//      if params.count > 1 {
//        substString.append("\"\(col)\" = (select array_agg(elements) from (select unnest(\"\(col)\") except select unnest(\(subst))) t (elements))")
//      } else {
//        substString.append("\"\(col)\" = (select array_agg(elements) from (select unnest(\"\(col)\") except select unnest(ARRAY[\(subst)]\(subst[subst.lastIndex(of: "::")..<subst.count])[])) t (elements))")
//      }
//    }
//
//    push.forEach { (col, param) in
//      let (params, subst) = PostgresStORM.convertInto(param, &i)
//      paramString += params
//
//      if params.count > 1 {
//        substString.append("\"\(col)\" = array_cat(\"\(col)\", \(subst))")
//      } else {
//        substString.append("\"\(col)\" = array_append(\"\(col)\", \(subst))")
//      }
//    }
//
//    addToSet.forEach { (col, param) in
//      let (params, subst) = PostgresStORM.convertInto(param, &i)
//      paramString += params
//
//      substString.append("\"\(col)\" = (SELECT ARRAY_AGG(DISTINCT e) FROM UNNEST(\"\(col)\" || \(subst)) e)")
//    }
//
//    var querySubst: [String] = []
//    query.forEach { (col, param) in
//      let (params, subst) = PostgresStORM.convertInto(param, &i)
//      paramString += params
//
//      querySubst.append("\"\(col)\" = \(subst)")
//    }
//
//    let whereclause: String
//    if querySubst.count > 0 {
//      whereclause = "WHERE \(querySubst.joined(separator: " AND "))"
//    } else {
//      whereclause = ""
//    }
//
//    let returningclause: String
//    if querySubst.count > 0 {
//      returningclause = "RETURNING \(returning.joined(separator: ", "))"
//    } else {
//      returningclause = ""
//    }
//
//    let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) \(whereclause) \(returningclause)"
//
//    do {
//      results.rows = try execRows(str, params: paramString)
//      if results.rows.count == 1 { makeRow() }
//    } catch {
//      LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
//      self.error = StORMError.error("\(error)")
//      throw error
//    }
//
//    return true
//  }
}
