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

  // Basic string parser
  public func toArrayString(_ text: String) -> [String] {
    // Early termination. Not necessary but avoids wasted calculations
    if text.count < 2 {
      return []
    }

    var sections: [String] = []
    var builder: String = ""

    var iterIndex = text.index(after: text.startIndex)

    var control = false
    var open = false

    while iterIndex < text.endIndex {
      let char = text[iterIndex]
      if !control && char == "\"" {
        open = !open
      } else if open && !control && char == "\\" {
        control = true
      } else if !open && (char == "," || char == "}") {
        sections.append(builder)
        builder = ""
      } else if control && open {
        builder.append(char)
        control = false
      } else if open || char != " " {
        builder.append(char)
      }

      iterIndex = text.index(after: iterIndex)
    }

    return sections
  }
	public func toArrayInt(_ input: Any) -> [Int] {
    var text = (input as? String ?? "{}")
    if text.count < 2 {
      return []
    }

    text.removeFirst()
    text.removeLast()

    return text.split(separator: ",").compactMap{ Int(trim(String($0))) }
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
