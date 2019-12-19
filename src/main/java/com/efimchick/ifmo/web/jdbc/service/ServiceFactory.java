package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;

public class ServiceFactory {

    private ResultSet getResultSet(String query) {
        try {
            ConnectionSource connectionSource = ConnectionSource.instance();
            Connection connection = connectionSource.createConnection();
            return connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).
                    executeQuery(query);
        } catch (SQLException ex) {
            return null;
        }
    }

    private List<Department> getDepartments() {
        String query = "SELECT * FROM DEPARTMENT";
        ResultSet rs = getResultSet(query);

        List<Department> res = new ArrayList<Department>();
        try {
            while (rs.next()) {
                try {
                    res.add(new Department(
                            new BigInteger(rs.getString("ID")),
                            rs.getString("NAME"),
                            rs.getString("LOCATION")
                    ));
                } catch (SQLException ex) {
                    res.add(null);
                }
            }
            return res;
        } catch (SQLException ex) {
            return null;
        }
    }

    private Employee rsToEmployee(ResultSet rs, boolean managerRequired, boolean chainRequired) {
        try {
            BigInteger id = new BigInteger(String.valueOf(rs.getString("ID")));

            FullName fullName = new FullName(
                    rs.getString("FIRSTNAME"),
                    rs.getString("LASTNAME"),
                    rs.getString("MIDDLENAME")
            );

            Position position = Position.valueOf(rs.getString("POSITION"));
            BigInteger managerId = new BigInteger(rs.getString("MANAGER"));
            LocalDate hireDate = LocalDate.parse(rs.getString("HIREDATE"));
            BigDecimal salary = new BigDecimal(rs.getString("SALARY"));
            BigInteger departmentId = new BigInteger(rs.getString("DEPARTMENT"));

            Employee manager = null;
            List<Department> departments = getDepartments();
            Department department = null;

            for (Department temp : departments) {
                if (temp.getId().equals(departmentId)) {
                    department = temp;
                }
            }

            if (managerId != null && managerRequired) {
                managerRequired = chainRequired;

                String query = "SELECT * FROM EMPLOYEE";
                ResultSet tempRS = getResultSet(query);
                while(tempRS.next()) {
                    if (BigInteger.valueOf(tempRS.getInt("ID")).equals(managerId)) {
                        manager = rsToEmployee(tempRS, managerRequired, chainRequired);
                    }
                }
            }
            return new Employee(id, fullName, position, hireDate, salary, manager, department);
        } catch(SQLException ex) {
            return null;
        }
    }

    private List<Employee> employeesToList(ResultSet rs, boolean managerRequired, boolean chainRequired) {
        List<Employee> res = new ArrayList<Employee>();
        if (rs == null) {
            return null;
        }
        while(rs.next()) {
            Employee employee = rsToEmployee(rs, managerRequired, chainRequired);
            res.add(employee);
        }
        return res;
    }

    private List<Employee> cutEmployee(List<Employee> employees, Paging paging) {
        int cutSize = paging.itemPerPage;
        int begin = paging.itemPerPage * (paging.page - 1);
        return employees.subList(begin, Math.min(cutSize * paging.page, employees.size()));
    }

    public EmployeeService employeeService(){
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                String query = "SELECT * FROM EMPLOYEE ORDER BY HIREDATE";
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging)    ;
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                String query = "SELECT * FROM EMPLOYEE ORDER BY LASTNAME";
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                String query = "SELECT * FROM EMPLOYEE ORDER BY SALARY";
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                String query = "SELECT * FROM EMPLOYEE ORDER BY DEPARTMENT, LASTNAME";
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                String query = String.format("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=%s ORDER BY HIREDATE", department.getId());
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                String query = String.format("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=%s ORDER BY SALARY", department.getId());
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                String query = String.format("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=%s ORDER BY LASTNAME", department.getId());
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                String query = String.format("SELECT * FROM EMPLOYEE WHERE MANAGER=%s ORDER BY LASTNAME", manager.getId());
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                String query = String.format("SELECT * FROM EMPLOYEE WHERE MANAGER=%s ORDER BY HIREDATE", manager.getId());
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                String query = String.format("SELECT * FROM EMPLOYEE WHERE MANAGER=%s ORDER BY SALARY", manager.getId());
                ResultSet rs = getResultSet(query);
                return cutEmployee(Objects.requireNonNull(employeesToList(rs, true, false)), paging);
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                String query = String.format("SELECT * FROM EMPLOYEE WHERE MANAGER=%s", employee.getId());
                ResultSet rs = getResultSet(query);
                return Objects.requireNonNull(employeesToList(rs, true, true)).get(0);
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                String query = String.format("SELECT * FROM EMPLOYEE WHEERE DEPARTMENT=%s, ORDER BY SALARY DESC LIMIT 1 OFFSET $d",
                        department.getId(),
                        salaryRank - 1
                );
                ResultSet rs = getResultSet(query);
                return Objects.requireNonNull(employeesToList(rs, true, false)).get(0);
            }
        };
    }
}
