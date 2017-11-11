package com.github.quintans.ezSQL.orm.app.daos;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import com.github.quintans.ezSQL.orm.domain.IORMRowTransformerFactory;
import com.github.quintans.ezSQL.orm.domain.ORMTransformer;
import com.github.quintans.ezSQL.orm.domain.SimpleEntityCache;
import com.github.quintans.ezSQL.transformers.IQueryRowTransformer;
import com.github.quintans.ezSQL.transformers.IRowTransformer;

public class EmployeeDAOBase implements IArtistDAO, IORMRowTransformerFactory<Employee> {

	public static final IORMRowTransformerFactory<Employee> factory = new EmployeeDAOBase();

	@Override
	public IQueryRowTransformer<Employee> createTransformer(Query query, boolean reuse) {
		return new Transformer(query, reuse);
	}

	@Override
	public IQueryRowTransformer<Employee> createTransformer(Association foreignKey, IRowTransformer<?> other) {
		return new Transformer(foreignKey, (ORMTransformer<?>) other);
	}

	public static class Transformer extends ORMTransformer<Employee> {
		public Transformer(Query query, boolean reuse) {
			super(query, reuse);
		}

		public Transformer(Association foreignKey, ORMTransformer<?> other) {
			super(foreignKey, other);
		}

		@Override
		public Employee transform(ResultSet rs, int[] columnTypes) throws SQLException {
			// in a outer join, an entity can have null for all of its fields, even for the id
			Long id = getLong(TEmployee.C_ID);
			if (id == null)
				return null;

			Employee temp = new Employee();
			temp.setId(id);

			Employee employee = null;

			if (isReuse())
				employee = SimpleEntityCache.cache(temp);

			if (employee == null) {
				employee = temp;

				employee.setName(getString(TEmployee.C_NAME));
				employee.setSex(getBoolean(TEmployee.C_SEX));
				employee.setCreation(getDate(TEmployee.C_CREATION));
			}

			return employee;
		}

	}
}
