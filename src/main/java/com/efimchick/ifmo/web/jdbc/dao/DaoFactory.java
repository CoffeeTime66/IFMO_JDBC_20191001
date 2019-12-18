package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.*;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    public static final ConnectionSource connectionSource = ConnectionSource.instance();

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = String.format("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=%s", department.getId());
                    final ResultSet resultSet = statement.executeQuery(query);
                    return rsToEmployees(resultSet);
                } catch (SQLException ex) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = String.format("SELECT * FROM EMPLOYEE WHERE MANAGER=%d", employee.getId());
                    final ResultSet resultSet = statement.executeQuery(query);
                    return rsToEmployees(resultSet);
                } catch (SQLException ex) {
                    return null;
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger id) {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = String.format("SELECT * FROM EMPLOYEE WHERE ID=%d", id);
                    final ResultSet resultSet = statement.executeQuery(query);
                    if (resultSet.next()) {
                        return Optional.of(rsToEmployee(resultSet));
                    } else {
                        return Optional.empty();
                    }
                } catch (SQLException ex) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = "SELECT * FROM EMPLOYEE";
                    final ResultSet resultSet = statement.executeQuery(query);
                    return rsToEmployees(resultSet);
                } catch (SQLException ex) {
                    return null;
                }
            }

            @Override
            public Employee save(Employee employee) {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = String.format(
                            "INSERT INTO EMPLOYEE VALUES (%d, '%s', '%s', '%s', '%s', '%d', '%s', '%d', '%d'",
                            employee.getId(),
                            employee.getFullName().getFirstName(),
                            employee.getFullName().getLastName(),
                            employee.getFullName().getMiddleName(),
                            employee.getPosition().toString(),
                            employee.getManagerId(),
                            employee.getHired().toString(),
                            employee.getSalary().toBigInteger(),
                            employee.getDepartmentId());
                    statement.executeUpdate(query);
                    return employee;
                } catch (SQLException ex) {
                    return null;
                }
            }

            @Override
            public void delete(Employee employee) {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = String.format("DELETE FROM EMPLOYEE WHERE ID=%d", employee.getId());
                    statement.executeQuery(query);
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        };

    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger id) {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = String.format("SELECT * FROM DEPARTMENT WHERE ID=%d", id);
                    final ResultSet resultSet = statement.executeQuery(query);
                    if (resultSet.next()) {
                        return Optional.of(rsToDepartment(resultSet));
                    } else {
                        return Optional.empty();
                    }
                } catch (SQLException ex) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Department> getAll() {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = "SELECT * FROM DEPARTMENT";
                    final ResultSet resultSet = statement.executeQuery(query);
                    return rsToDepartments(resultSet);
                } catch (SQLException ex) {
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query1 = String.format("SELECT * FROM DEPARTMENT WHERE ID=%d", department.getId());
                    ResultSet resultSet = statement.executeQuery(query1);
                    String query;
                    if (resultSet.next()) {
                        query = String.format(
                                "UPDATE DEPARTMENT SET NAME='%s', LOCATION='%s' WHERE ID=%d",
                                department.getName(),
                                department.getLocation(),
                                department.getId()
                        );
                    } else {
                        query = String.format(
                                "INSERT INTO DEPARTMENT VALUES(%d, '%s', '%s')",
                                department.getId(),
                                department.getName(),
                                department.getLocation()
                        );
                    }
                    statement.executeUpdate(query);
                    return department;
                } catch (SQLException ex) {
                    return null;
                }
            }

            @Override
            public void delete(Department department) {
                try (Connection con = connectionSource.createConnection();
                     Statement statement = con.createStatement()) {
                    String query = String.format("DELETE FROM DEPARTMENT WHERE ID=%d", department.getId());
                    statement.executeUpdate(query);
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        };
    }

    public Employee rsToEmployee(ResultSet rs) throws SQLException {
        BigInteger id = new BigInteger(rs.getString("ID"));

        String firstname = new String(rs.getString("FIRSTNAME"));
        String lastname = new String(rs.getString("LASTNAME"));
        String middlename = new String(rs.getString("MIDDLENAME"));
        FullName fullname = new FullName(firstname, lastname, middlename);

        String stringPosition = new String(rs.getString("POSITION"));
        Position position = Position.valueOf(stringPosition);

        LocalDate hiredate = LocalDate.parse(rs.getString("HIREDATE"));
        BigDecimal salary = new BigDecimal(rs.getString("SALARY"));

        BigInteger manager = (rs.getBigDecimal("MANAGER") != null) ?
                rs.getBigDecimal("MANAGER").toBigInteger() :
                BigInteger.ZERO;

        BigInteger department = (rs.getBigDecimal("DEPARTMENT") != null) ?
                rs.getBigDecimal("DEPARTMENT").toBigInteger() :
                BigInteger.ZERO;

        return new Employee(id, fullname, position, hiredate, salary, manager, department);
    }

    public List<Employee> rsToEmployees(ResultSet rs) throws SQLException {
        List<Employee> res = new ArrayList<Employee>();
        while(rs.next()) {
            res.add(rsToEmployee(rs));
        }
        return res;
    }

    public Department rsToDepartment(ResultSet rs) throws SQLException {
        BigInteger id = new BigInteger(rs.getString("ID"));
        String name = rs.getString("NAME");
        String location = rs.getString("LOCATION");

        return new Department(id, name, location);
    }

    public List<Department> rsToDepartments(ResultSet rs) throws SQLException {
        List<Department> res = new ArrayList<Department>();
        while(rs.next()) {
            res.add(rsToDepartment(rs));
        }
        return res;
    }
}
