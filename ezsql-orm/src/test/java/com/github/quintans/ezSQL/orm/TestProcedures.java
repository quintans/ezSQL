package com.github.quintans.ezSQL.orm;

import com.github.quintans.ezSQL.sp.SqlProcedure;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static com.github.quintans.jdbc.sp.SqlParameter.IN;
import static com.github.quintans.jdbc.sp.SqlParameter.OUT;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestProcedures extends TestBootstrap {

	public TestProcedures(String environment) {
		super(environment);
	}

	@Test
	public void testProcedure() throws Exception {
		// calls the function SYSDATE
		tm.transactionNoResult(db -> {
			MyFunctionsDao spDao = new MyFunctionsDao(db);
			Integer five = spDao.hiFive();
			assertEquals("failed getting hiFive", 5, five.intValue());
		});
	}

	class MyFunctionsDao {
		private SqlProcedure hiFive;
		private SqlProcedure mockUpdate;
		private SqlProcedure mockRead;

		public MyFunctionsDao(Db db) {
			// it's declared as a function because it has a parameter before the
			// function name
			this.hiFive = new SqlProcedure(db,
				OUT("return", Types.INTEGER), // return parameter
				"hiFive" // function name
			);
			// it's declared as a procedure because it has no parameters before
			// the procedure name
			this.mockUpdate = new SqlProcedure(db,
				"MOCK_UPDATE", // procedure name
				IN("cod", Types.VARCHAR), // parameter
				IN("dsc", Types.VARCHAR) // parameter
			);
			// it's declared as a function because it has a parameter before the
			// function name
			this.mockRead = new SqlProcedure(db,
				OUT("return", Types.VARCHAR), // return parameter
				"MOCK_READ", // function
				IN("cod", Types.VARCHAR) // parameter
			);
		}

		public Integer hiFive() {
			Map<String, Object> results = this.hiFive.call();
			return (Integer) results.get("return");
		}

		public void mockUpdate(String codigo, String descricao) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("cod", codigo);
			map.put("dsc", descricao);
			this.mockUpdate.call(map);
		}

		public String mockRead(String codigo) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("cod", codigo);
			Map<String, Object> results = this.mockRead.call(map);
			return (String) results.get("return");
		}
	}
	
	/*
	// TODO use proxies - warning: there are output parameters that can be resultsets.
	private interface xpto {
	    @Procedure("MOCK_READ")
	    @Result(Types.VARCHAR) 
	    String mockRead(@In(Types.VARCHAR) String codigo);
	}
	*/
}
