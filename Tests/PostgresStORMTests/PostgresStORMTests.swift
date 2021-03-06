import XCTest
import PerfectLib
import Foundation
import StORM
@testable import PostgresStORM

struct Data: Equatable, Codable {
  let foo: String
  let bar: Int
}

class UserTest: PostgresStORM {
	// NOTE: First param in class should be the ID.
	var id				: Int = 0
	var firstname		: String = ""
	var lastName		: String? = ""
	var email			: String = ""
  var optional: Double?
  var stringArray: [String]!
  var intarray: [Int] = []
  var Json: [String:Any]?
  var jsonArray: [[String:Any]]!
  var struct_: Data?
  var structarray: [Data]!

	override func to(_ this: StORMRow) {
    let decoder = JSONDecoder()

    id				= this.data["id"] as? Int ?? 0
		firstname		= this.data["firstname"] as? String ?? ""
		lastName		= this.data["lastName"] as? String ?? ""
		email			= this.data["email"] as? String ?? ""
    optional = this.data["optional"] as? Double
		stringArray		= this.data["stringArray"] as? [String] ?? []
    Json = this.data["Json"] as? [String:Any] ?? [:]
    jsonArray = this.data["jsonArray"] as? [[String:Any]] ?? []
    intarray = this.data["intarray"] as? [Int] ?? []
    struct_ = Data.fromJson(this.data["struct_"], using: decoder) ?? Data(foo: "", bar: 0)
    structarray = Data.fromJsonArray(this.data["structarray"], using: decoder)
	}

	func rows() -> [UserTest] {
		var rows = [UserTest]()
		for i in 0..<self.results.rows.count {
			let row = UserTest()
			row.to(self.results.rows[i])
			rows.append(row)
		}
		return rows
	}
}


class PostgresStORMTests: XCTestCase {

	override func setUp() {
		super.setUp()
		#if os(Linux)

			PostgresConnector.host		= ProcessInfo.processInfo.environment["HOST"]!
			PostgresConnector.username	= ProcessInfo.processInfo.environment["USER"]!
			PostgresConnector.password	= ProcessInfo.processInfo.environment["PASS"]!
			PostgresConnector.database	= ProcessInfo.processInfo.environment["DB"]!
			PostgresConnector.port		= Int(ProcessInfo.processInfo.environment["PORT"]!)!

		#else
			PostgresConnector.host		= "localhost"
			PostgresConnector.username	= "postgres"
			PostgresConnector.password	= "foo"
			PostgresConnector.database	= "postgres"
			PostgresConnector.port		= 5432
		#endif
		let obj = UserTest()
		try! obj.setup()
		StORMdebug = true
	}

	/* =============================================================================================
	Save - New
	============================================================================================= */
	func testSaveNew() {

		let obj = UserTest()
		obj.firstname = "X"
		obj.lastName = "Y"

		do {
			try obj.save { id in obj.id = id as! Int }
		} catch {
			XCTFail(String(describing: error))
		}
		XCTAssert(obj.id > 0, "Object not saved (new)")
	}

	/* =============================================================================================
	Save - Update
	============================================================================================= */
	func testSaveUpdate() {
		let obj = UserTest()
		obj.firstname = "X"
		obj.lastName = "Y"

		do {
			try obj.save { id in obj.id = id as! Int }
		} catch {
			XCTFail(String(describing: error))
		}

		obj.firstname = "A"
		obj.lastName = "B"
		do {
			try obj.save()
		} catch {
			XCTFail(String(describing: error))
		}
		print(obj.errorMsg)
		XCTAssert(obj.id > 0, "Object not saved (update)")
	}

	/* =============================================================================================
	Save - Create
	============================================================================================= */
	func testSaveCreate() {
		// first clean up!
		let deleting = UserTest()
		do {
			deleting.id			= 10001
			try deleting.delete()
		} catch {
			XCTFail(String(describing: error))
		}

		let obj = UserTest()

		do {
			obj.id			= 10001
			obj.firstname	= "Mister"
			obj.lastName	= "PotatoHead"
			obj.email		= "potato@example.com"
			try obj.create()
		} catch {
			XCTFail(String(describing: error))
		}
		XCTAssert(obj.id == 10001, "Object not saved (create)")
	}

	/* =============================================================================================
	Get (with id)
	============================================================================================= */
	func testGetByPassingID() {
		let obj = UserTest()
		obj.firstname = "X"
		obj.lastName = "Y"

		do {
			try obj.save { id in obj.id = id as! Int }
		} catch {
			XCTFail(String(describing: error))
		}

		let obj2 = UserTest()

		do {
			try obj2.get(obj.id)
		} catch {
			XCTFail(String(describing: error))
		}
		XCTAssert(obj.id == obj2.id, "Object not the same (id)")
		XCTAssert(obj.firstname == obj2.firstname, "Object not the same (firstname)")
		XCTAssert(obj.lastName == obj2.lastName, "Object not the same (lastname)")
	}


	/* =============================================================================================
	Get (by id set)
	============================================================================================= */
	func testGetByID() {
		let obj = UserTest()
		obj.firstname = "X"
		obj.lastName = "Y"

		do {
			try obj.save { id in obj.id = id as! Int }
		} catch {
			XCTFail(String(describing: error))
		}

		let obj2 = UserTest()
		obj2.id = obj.id
		
		do {
			try obj2.get()
		} catch {
			XCTFail(String(describing: error))
		}
		XCTAssert(obj.id == obj2.id, "Object not the same (id)")
		XCTAssert(obj.firstname == obj2.firstname, "Object not the same (firstname)")
		XCTAssert(obj.lastName == obj2.lastName, "Object not the same (lastname)")
	}

	/* =============================================================================================
	Get (with id) - integer too large
	============================================================================================= */
	func testGetByPassingIDtooLarge() {
		let obj = UserTest()

		do {
			try obj.get(874682634789)
			XCTFail("Should have failed (integer too large)")
		} catch {
			print("^ Ignore this error, that is expected and should show 'ERROR:  value \"874682634789\" is out of range for type integer'")
			// test passes - should have a failure!
		}
	}
	
	/* =============================================================================================
	Get (with id) - no record
	// test get where id does not exist (id)
	============================================================================================= */
	func testGetByPassingIDnoRecord() {
		let obj = UserTest()

		do {
			try obj.get(1111111)
			XCTAssert(obj.results.cursorData.totalRecords == 0, "Object should have found no rows")
		} catch {
			XCTFail(error as! String)
		}
	}




	// test get where id does not exist ()
	/* =============================================================================================
	Get (preset id) - no record
	// test get where id does not exist (id)
	============================================================================================= */
	func testGetBySettingIDnoRecord() {
		let obj = UserTest()
		obj.id = 1111111
		do {
			try obj.get()
			XCTAssert(obj.results.cursorData.totalRecords == 0, "Object should have found no rows")
		} catch {
			XCTFail(error as! String)
		}
	}


	/* =============================================================================================
	Returning DELETE statement to verify correct form
	// deleteSQL
	============================================================================================= */
	func testCheckDeleteSQL() {
		let obj = UserTest()
		XCTAssert(obj.deleteSQL("test", idName: "testid") == "DELETE FROM test WHERE \"testid\" = $1", "DeleteSQL statement is not correct")

	}



	/* =============================================================================================
	Find
	============================================================================================= */
	func testFind() {
		// Ensure table is empty
		do {
			let obj = UserTest()
			let tableName = obj.table()
			_ = try? obj.sql("DELETE FROM \(tableName)", params: [])
		}

		// Doing a `find` with an empty table should yield zero results
		do {
			let obj = UserTest()
			do {
				try obj.find([("\"lastName\"", "Ashpool")])
				XCTAssertEqual(obj.results.rows.count, 0)
				XCTAssertEqual(obj.results.cursorData.totalRecords, 0)
			} catch {
				XCTFail("Find error: \(obj.error.string())")
			}
		}

		// Insert more rows than the StORMCursor().limit
		for i in 0..<200 {
			let obj = UserTest()
			obj.firstname = "Tessier\(i)"
			obj.lastName = "Ashpool"
			do {
				try obj.save { id in obj.id = id as! Int }
			} catch {
				XCTFail(String(describing: error))
			}
		}
		for i in 0..<10 {
			let obj = UserTest()
			obj.firstname = "Molly\(i)"
			do {
				try obj.save { id in obj.id = id as! Int }
			} catch {
				XCTFail(String(describing: error))
			}
		}

    do {
      let obj = UserTest()
      do {
        let count = try obj.findCount([("\"lastName\"", "Ashpool")])
        XCTAssertEqual(count, 200, "Object should have found the all the rows just inserted")
      } catch {
        XCTFail("Find error: \(obj.error.string())")
      }
    }

		// Doing the same `find` should now return rows
		do {
			let obj = UserTest()
			do {
				try obj.find([("\"lastName\"", "Ashpool")])
				let cursorLimit: Int = StORMCursor().limit
				XCTAssertEqual(obj.results.rows.count, cursorLimit, "Object should have found the all the rows just inserted. Limited by the default cursor limit.")
				XCTAssertEqual(obj.results.cursorData.totalRecords, 200, "Object should have found the all the rows just inserted")
			} catch {
				XCTFail("Find error: \(obj.error.string())")
			}
		}

		// Doing the same `find` should now return rows limited by the provided cursor limit
		do {
			let obj = UserTest()
			do {
				let cursor = StORMCursor(limit: 150, offset: 0)
				try obj.find(["\"lastName\"": "Ashpool"], cursor: cursor)
				XCTAssertEqual(obj.results.rows.count, cursor.limit, "Object should have found the all the rows just inserted. Limited by the provided cursor limit.")
				XCTAssertEqual(obj.results.cursorData.totalRecords, 200, "Object should have found the all the rows just inserted")
			} catch {
				XCTFail("Find error: \(obj.error.string())")
			}
		}
	}

  func testFindIn() {
    // Ensure table is empty
    do {
      let obj = UserTest()
      let tableName = obj.table()
      _ = try? obj.sql("DELETE FROM \(tableName)", params: [])
    }

    let data = (195..<205).map { "Tessier\($0)" } + (5..<15).map { "Molly\($0)" }

    // Doing a `find` with an empty table should yield zero results
    do {
      let obj = UserTest()
      do {
        try obj.find([("firstname", data)])
        XCTAssertEqual(obj.results.rows.count, 0)
        XCTAssertEqual(obj.results.cursorData.totalRecords, 0)
      } catch {
        XCTFail("Find error: \(obj.error.string())")
      }
    }

    // Insert more rows than the StORMCursor().limit
    for i in 0..<200 {
      let obj = UserTest()
      obj.firstname = "Tessier\(i)"
      obj.lastName = "Ashpool"
      do {
        try obj.save { id in obj.id = id as! Int }
      } catch {
        XCTFail(String(describing: error))
      }
    }
    for i in 0..<10 {
      let obj = UserTest()
      obj.firstname = "Molly\(i)"
      do {
        try obj.save { id in obj.id = id as! Int }
      } catch {
        XCTFail(String(describing: error))
      }
    }

    // Doing the same `find` should now return rows
    do {
      let obj = UserTest()
      do {
        try obj.find([("firstname", data)])
        XCTAssertEqual(obj.results.rows.count, 10, "Object should have found the all the rows just inserted. Limited by the default cursor limit.")
      } catch {
        XCTFail("Find error: \(obj.error.string())")
      }
    }
  }
	
	/* =============================================================================================
	FindAll
	============================================================================================= */
	func testFindAll() {
		// Ensure table is empty
		do {
			let obj = UserTest()
			let tableName = obj.table()
			_ = try? obj.sql("DELETE FROM \(tableName)", params: [])
		}

		// Insert more rows than the StORMCursor().limit
		for i in 0..<200 {
			let obj = UserTest()
			obj.firstname = "Wintermute\(i)"
			do {
				try obj.save { id in obj.id = id as! Int }
			} catch {
				XCTFail(String(describing: error))
			}
		}
		
		// Check that all the rows are returned
		do {
			let obj = UserTest()
			do {
				try obj.findAll()
				XCTAssertEqual(obj.results.rows.count, 200, "Object should have found the all the rows just inserted. Not limited by the default cursor limit.")
				XCTAssertEqual(obj.results.cursorData.totalRecords, 200, "Object should have found the all the rows just inserted")
			} catch {
				XCTFail("findAll error: \(obj.error.string())")
			}
		}
	}
	

	/* =============================================================================================
	Test array set & retrieve
	============================================================================================= */
	func testStringArray() {
		let obj = UserTest()
		obj.stringArray = ["a", "b", "zee"]

		do {
			try obj.save { id in obj.id = id as! Int }
		} catch {
			XCTFail(String(describing: error))
		}

		let obj2 = UserTest()

		do {
			try obj2.get(obj.id)
      try obj.delete()
      try obj2.delete()
		} catch {
			XCTFail(String(describing: error))
		}
		XCTAssert(obj.id == obj2.id, "Object not the same (id)")
		XCTAssert(obj.stringArray == obj2.stringArray, "Object not the same (stringArray)")
	}

  func testIntArray() {
    let obj = UserTest()
    obj.intarray = [3, 1, 2]

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    let obj2 = UserTest()

    do {
      try obj2.get(obj.id)
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert(obj.intarray == obj2.intarray, "Object not the same (intarray)")
  }

  func testDouble() {
    let obj = UserTest()
    obj.optional = 1500000000.0
    

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    let obj2 = UserTest()

    do {
      try obj2.get(obj.id)
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert(obj.optional! == obj2.optional!, "Object not the same (json)")
  }

  func testJson() {
    let obj = UserTest()
    obj.Json = ["a": "b", "c": "zee"]

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    let obj2 = UserTest()

    do {
      try obj2.get(obj.id)
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert((obj.Json! as NSDictionary).isEqual(obj2.Json ?? [:]), "Object not the same (json)")
  }

  func testJsonArray() {
    let obj = UserTest()
    obj.jsonArray = [["a": "b", "c": "zee"], ["foo": "bar", "one": "two"]]

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    let obj2 = UserTest()

    do {
      try obj2.get(obj.id)
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert(obj.jsonArray.elementsEqual(obj2.jsonArray, by: { ($0 as NSDictionary).isEqual(to: $1) } ), "Object not the same (jsonarray)")
  }

  func testStructArray() {
    let obj = UserTest()
    obj.structarray = [Data(foo: "Hello", bar: 1), Data(foo: "World", bar: 4)]

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    let obj2 = UserTest()

    do {
      try obj2.get(obj.id)
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert(obj.structarray.elementsEqual(obj2.structarray), "Object not the same (structarray)")
  }

  func testStruct() {
    let obj = UserTest()
    obj.struct_ = Data(foo: "Hello", bar: 1)

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    let obj2 = UserTest()

    do {
      try obj2.get(obj.id)
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert(obj.struct_ == obj2.struct_, "Object not the same (struct)")
  }

  func testSelectJson() {
    // Ensure table is empty
    do {
      let obj = UserTest()
      let tableName = obj.table()
      _ = try? obj.sql("DELETE FROM \(tableName)", params: [])
    }

    let obj = UserTest()
    obj.Json = ["foo": "Hello", "bar": 1]

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    let obj2 = UserTest()

    do {
      try obj2.find(["\"Json\"->>'foo'": "Hello"])
//      try obj2.select(whereclause: "json->>'foo' = $1", params: ["Hello"], orderby: [])
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert((obj.Json! as NSDictionary).isEqual(obj2.Json ?? [:]), "Object not the same (json)")
  }

  func testPush() {
    let obj = UserTest()
    obj.intarray = [0, 4, 2, 6, 1]

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    do {
      try obj.push(cols: ["intarray"], params: [[8, 9]], idName: "id", idValue: obj.id)
    } catch {
      XCTFail(String(describing: error))
    }
    print(obj.errorMsg)
    XCTAssert(obj.id > 0, "Object not saved (update)")

    let obj2 = UserTest()
    obj2.id = obj.id

    do {
      try obj2.get()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert((obj.intarray + [8, 9]).elementsEqual(obj2.intarray), "Object not the same (intarray)")

    do {
      try obj.push(cols: ["intarray"], params: [10], idName: "id", idValue: obj.id)
    } catch {
      XCTFail(String(describing: error))
    }
    print(obj.errorMsg)
    XCTAssert(obj.id > 0, "Object not saved (update)")

    do {
      try obj2.get()
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert((obj.intarray + [8, 9, 10]).elementsEqual(obj2.intarray), "Object not the same (intarray)")
  }

  func testPull() {
    let obj = UserTest()
    obj.intarray = [0, 4, 2, 6, 1]

    do {
      try obj.save { id in obj.id = id as! Int }
    } catch {
      XCTFail(String(describing: error))
    }

    do {
      try obj.pull(cols: ["intarray"], params: [[4, 6, 8]], idName: "id", idValue: obj.id)
    } catch {
      XCTFail(String(describing: error))
    }
    print(obj.errorMsg)
    XCTAssert(obj.id > 0, "Object not saved (update)")

    let obj2 = UserTest()
    obj2.id = obj.id

    do {
      try obj2.get()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert([0, 1, 2].elementsEqual(obj2.intarray.sorted()), "Object not the same (intarray)")

    do {
      try obj.pull(cols: ["intarray"], params: [1], idName: "id", idValue: obj.id)
    } catch {
      XCTFail(String(describing: error))
    }
    print(obj.errorMsg)
    XCTAssert(obj.id > 0, "Object not saved (update)")

    do {
      try obj2.get()
      try obj.delete()
      try obj2.delete()
    } catch {
      XCTFail(String(describing: error))
    }
    XCTAssert(obj.id == obj2.id, "Object not the same (id)")
    XCTAssert([0, 2].elementsEqual(obj2.intarray), "Object not the same (intarray)")
  }

    /* =============================================================================================
     parseRows (JSON Aggregation)
     ============================================================================================= */
    func testJsonAggregation() {
        
        // In the parseRows function we changed to cast into either [String:Any] type, OR [[String:Any]] type as aggregated JSON type.
        
        let encodedJSONArray = "[{\"id\":\"101\",\"name\":\"Pushkar\",\"salary\":\"5000\"}, {\"id\":\"102\",\"name\":\"Rahul\",\"salary\":\"4000\"},{\"id\":\"103\",\"name\":\"tanveer\",\"salary\":\"56678\"}]"
        
        do {
            let decodedJSONArray    = try encodedJSONArray.jsonDecode()
            
            if decodedJSONArray as? [[String:Any]] == nil {
                XCTAssert(false, "Failed to cast decoded JSON to array of type [String : Any]")
            }
            
        } catch {
            XCTFail("Failed to decode array of JSON.")
        }
        
        let encodedJSON = "{\"id\":\"101\",\"name\":\"Pushkar\",\"salary\":\"5000\"}"
        
        do {
            let decodedJSON         = try encodedJSON.jsonDecode()
            
            if decodedJSON as? [String:Any] == nil {
                XCTAssert(false, "Failed to cast decoded JSON into [String:Any] type.")
            }
        } catch {
            XCTFail("Failed to decode JSON.")
        }
    
    }

	static var allTests : [(String, (PostgresStORMTests) -> () throws -> Void)] {
		return [
			("testSaveNew", testSaveNew),
			("testSaveUpdate", testSaveUpdate),
			("testSaveCreate", testSaveCreate),
			("testGetByPassingID", testGetByPassingID),
			("testGetByID", testGetByID),
			("testGetByPassingIDtooLarge", testGetByPassingIDtooLarge),
			("testGetByPassingIDnoRecord", testGetByPassingIDnoRecord),
			("testGetBySettingIDnoRecord", testGetBySettingIDnoRecord),
			("testCheckDeleteSQL", testCheckDeleteSQL),
			("testFind", testFind),
			("testFindAll", testFindAll),
			("testArray", testStringArray),
      ("testJson", testJson),
      ("testJsonArray", testJsonArray),
      ("testStructArray", testStructArray),
      ("testStruct", testStruct),
      ("testIntArray", testIntArray),
      ("testSelectJson", testSelectJson),
      ("testPush", testPush),
      ("testPull", testPull),
      ("testJsonAggregation", testJsonAggregation)
		]
	}

}
