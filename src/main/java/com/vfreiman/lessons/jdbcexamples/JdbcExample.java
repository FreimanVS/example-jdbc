package com.vfreiman.lessons.jdbcexamples;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class JdbcExample {
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost/a?useSSL=false";
    private static final String LOGIN = "root";
    private static final String PASSWORD = "pass";


    private static final String TABLE_NAME = "test";
    private static final String INSERT = "INSERT INTO " + TABLE_NAME + "(name) VALUES (?), (?), (?)";
    private static final String CALL = "{call p(?, ?)}";

    private void main() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try (Connection con = getConnection(true, Connection.TRANSACTION_READ_COMMITTED);
             Statement statement = con.createStatement();
             PreparedStatement preparedStatement = con.prepareStatement(INSERT);
             CallableStatement callableStatement = con.prepareCall(CALL)
        ) {

            createTable(statement);
            insertValues(preparedStatement);
            selectAll(statement);
            createProcedure(statement);
            call(callableStatement);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Statement statement) throws SQLException {
        final String DROP = "DROP TABLE IF EXISTS " + TABLE_NAME + ";";

        String CREATE = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(";
        CREATE += "name VARCHAR(64) NULL";
        CREATE += ");";

        statement.executeUpdate(DROP);
        statement.executeUpdate(CREATE);
    }

    private static void insertValues(PreparedStatement ps) throws SQLException {
        final String VALUE1 = "a";
        final String VALUE2 = "b";
        final String VALUE3 = "c";

        ps.setString(1, VALUE1);
        ps.setString(2, VALUE2);
        ps.setString(3, VALUE3);

        ps.executeUpdate();
    }

    private static void selectAll(Statement st) throws SQLException {
        final String SELECT = "SELECT * FROM " + TABLE_NAME + ";";
        try (ResultSet resultSet = st.executeQuery(SELECT)) {
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                System.out.println(name);
            }
        }
    }

    private static void dropProcedure(Statement st) throws SQLException {
        final String DROP_PROCEDURE = "DROP PROCEDURE IF EXISTS p; ";
        st.executeUpdate(DROP_PROCEDURE);
    }

    private static void createProcedure(Statement st) throws SQLException {
        dropProcedure(st);
        String CREATE_PROCEDURE = "CREATE PROCEDURE p(OUT p_out VARCHAR(20), IN p_in VARCHAR(20)) ";
        CREATE_PROCEDURE += "BEGIN ";
        CREATE_PROCEDURE += "SELECT CONCAT(p_in, ' is result') INTO p_out; ";
        CREATE_PROCEDURE += "END ";
        st.executeUpdate(CREATE_PROCEDURE);
    }

    private static void call(CallableStatement cs) throws SQLException {
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.setString(2, "hi");
        cs.executeUpdate();
        System.out.println(cs.getString(1));
    }

    private static Connection getConnection(boolean autocommit, int isolation_level) throws SQLException {
        Connection con = DriverManager.getConnection(URL, LOGIN, PASSWORD);
        con.setAutoCommit(autocommit);
        con.setTransactionIsolation(isolation_level);
        return con;
    }

    public static void main(String[] args) {
        new JdbcExample().main();
    }
}
