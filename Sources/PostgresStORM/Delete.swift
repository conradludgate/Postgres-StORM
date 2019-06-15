//
//  Delete.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2016-09-24.
//
//

import PerfectLib
import StORM
import PerfectLogger

/// Performs delete-specific functions as an extension
extension PostgresStORM {

	func deleteSQL(_ table: String, idName: String = "id") -> String {
		return "DELETE FROM \(table) WHERE \"\(idName)\" = $1"
	}

	/// Deletes one row, with an id as an integer
	@discardableResult
  public func delete(_ id: Int, idName: String = "id", forcePrint: Bool? = nil) throws -> Bool {
		do {
			try exec(deleteSQL(self.table(), idName: idName), params: [String(id)], forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}
		return true
	}

	/// Deletes one row, with an id as a String
	@discardableResult
	public func delete(_ id: String, idName: String = "id", forcePrint: Bool? = nil) throws -> Bool {
		do {
			try exec(deleteSQL(self.table(), idName: idName), params: [id], forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}
		return true
	}

	/// Deletes one row, with an id as a UUID
	@discardableResult
	public func delete(_ id: UUID, idName: String = "id", forcePrint: Bool? = nil) throws -> Bool {
		do {
			try exec(deleteSQL(self.table(), idName: idName), params: [id.string], forcePrint: forcePrint)
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}
		return true
	}

}
