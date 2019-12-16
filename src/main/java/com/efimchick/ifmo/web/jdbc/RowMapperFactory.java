package com.efimchick.ifmo.web.jdbc;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class RowMapperFactory {

    public RowMapper<Employee> employeeRowMapper() {
        RowMapper rowMapper = new RowMapper<Employee>() {
            @Override
            public Employee mapRow(ResultSet resultSet) {
                try {
                    BigInteger id = new BigInteger(resultSet.getString("ID"));

                    String firstname = new String(resultSet.getString("FIRSTNAME"));
                    String lastname = new String(resultSet.getString("LASTNAME"));
                    String middlename = new String(resultSet.getString("MIDLENAME"));
                    FullName fullname = new FullName(firstname, lastname, middlename);

                    String stringPosition = new String(resultSet.getString("POSITION"));
                    Position position = Position.valueOf(stringPosition);

                    LocalDate hiredate = LocalDate.parse(resultSet.getString("HIREDATE"));
                    BigDecimal salary = new BigDecimal(resultSet.getString("SALARY"));

                    return new Employee(id, fullname, position, hiredate, salary);
                } catch (SQLException ex) {
                    return null;
                }
            }
        };

        return rowMapper;
    }
}
