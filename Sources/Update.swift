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
      let (params, subst) = convertInto(param, &i)
      paramString += params
      substString.append("\"\(cols[index].lowercased())\" = \(subst)")
    }

    paramString.append(String(describing: idValue))

    let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) WHERE \"\(idName.lowercased())\" = $\(i + 1)"

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

  @discardableResult
  public func push(cols: [String], params: [Any], idName: String, idValue: Any) throws -> Bool {
    var paramString = [String]()
    var substString = [String]()

    var i = 0
    params.enumerated().forEach { (index, param) in
      let (params, subst) = convertInto(param, &i)
      print(param, params, subst)
      paramString += params
      substString.append("\"\(cols[index].lowercased())\" = array_cat(\"\(cols[index].lowercased())\", \(subst))")
    }

    paramString.append(String(describing: idValue))

    let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) WHERE \"\(idName.lowercased())\" = $\(i + 1)"

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

  @discardableResult
  public func push(data: [(String, Any)], idName: String = "id", idValue: Any) throws -> Bool {

    var keys = [String]()
    var vals = [Any]()
    for i in 0..<data.count {
      keys.append(data[i].0.lowercased())
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
      let (params, subst) = convertInto(param, &i)
      paramString += params
      substString.append("\"\(cols[index].lowercased())\" = (select array_agg(elements) from (select unnest(\"\(cols[index].lowercased())\") except select unnest(\(subst))) t (elements))")
    }

    paramString.append(String(describing: idValue))

    let str = "UPDATE \(self.table()) SET \(substString.joined(separator: ", ")) WHERE \"\(idName.lowercased())\" = $\(i + 1)"

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

  @discardableResult
  public func pull(data: [(String, Any)], idName: String = "id", idValue: Any) throws -> Bool {


    var keys = [String]()
    var vals = [Any]()
    for i in 0..<data.count {
      keys.append(data[i].0.lowercased())
      vals.append(data[i].1)
    }
    do {
      return try push(cols: keys, params: vals, idName: idName, idValue: idValue)
    } catch {
      LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
      throw StORMError.error("\(error)")
    }
  }
}
