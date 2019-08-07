//
//  SQL.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2016-09-24.
//
//

import StORM
import PerfectPostgreSQL
import PerfectLogger

/// An extension to the main class providing SQL statement functions
extension PostgresStORM {

	/// Execute Raw SQL (with parameter binding)
	/// Returns PGResult (discardable)
	@discardableResult
  public func sql(_ statement: String, params: [String], forcePrint: Bool?) throws -> PGResult {
		do {
			return try exec(statement, params: params, forcePrint: forcePrint)
		} catch {
			if !PostgresConnector.quiet {
				LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
				self.error = StORMError.error("\(error)")
			}
			throw error
		}
	}

	/// Execute Raw SQL (with parameter binding)
	/// Returns [StORMRow] (discardable)
	@discardableResult
	public func sqlRows(_ statement: String, params: [String], forcePrint: Bool?) throws -> [StORMRow] {
		do {
			return try execRows(statement, params: params, forcePrint: forcePrint)
		} catch {
			if !PostgresConnector.quiet {
				LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
				self.error = StORMError.error("\(error)")
			}
			throw error
		}
	}
}

