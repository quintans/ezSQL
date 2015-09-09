package pt.quintans.ezSQL.orm;

import static pt.quintans.ezSQL.sp.SqlParameter.IN;
import static pt.quintans.ezSQL.sp.SqlParameter.OUT;

import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import pt.quintans.ezSQL.sp.SqlProcedure;

/**
 * Unit test for simple App.
 */
public class TestProcedures extends TestBootstrap {

    @Test
	public void testProcedure() throws Exception {
		// calls the function SYSDATE
		try {
			MyFunctionsDao spDao = new MyFunctionsDao(db);
			Date date = spDao.getSysdate();
			System.out.println("date: " + date);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	class MyFunctionsDao {
		private SqlProcedure sysdate;
		private SqlProcedure mockUpdate;
		private SqlProcedure mockRead;

		public MyFunctionsDao(Db db) {
			// it's declared as a function because it has a parameter before the
			// function name
			this.sysdate = new SqlProcedure(db,
				OUT("return", Types.DATE), // return parameter
				"SYSDATE" // function name
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

		public Date getSysdate() {
			Map<String, Object> results = this.sysdate.call();
			return (Date) results.get("return");
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
