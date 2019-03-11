//
//  DataConverters.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2017-08-03.
//
//

import Foundation

extension PostgresStORM {

	fileprivate func trim(_ str: String) -> String {
		return str.trimmingCharacters(in: .whitespacesAndNewlines)
	}

	public func toArrayString(_ input: Any) -> [String] {
    var text = (input as? String ?? "{}")
    if text.count < 2 {
      return []
    }

    text.removeFirst()
    text.removeLast()
		return text.split(separator: ",").map{ trim(String($0)) }
	}
	public func toArrayInt(_ input: Any) -> [Int] {
    var text = (input as? String ?? "{}")
    if text.count < 2 {
      return []
    }

    text.removeFirst()
    text.removeLast()

    return text.split(separator: ",").compactMap{ trim(String($0)).toInt() }
	}
  public func toArrayFloat(_ input: Any) -> [Float] {
    var text = (input as? String ?? "{}")
    if text.count < 2 {
      return []
    }

    text.removeFirst()
    text.removeLast()

    return text.split(separator: ",").compactMap{ Float(trim(String($0))) }
  }
  public func toArrayDouble(_ input: Any) -> [Double] {
    var text = (input as? String ?? "{}")
    if text.count < 2 {
      return []
    }

    text.removeFirst()
    text.removeLast()

    return text.split(separator: ",").compactMap{ Double(trim(String($0)))}
  }
}
