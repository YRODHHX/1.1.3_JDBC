package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.beans.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class UserDaoJDBCImpl implements UserDao {
    private static final String CREATE_USER_TABLE = "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(45) , last_name VARCHAR(45) , age INT)";
    private static final String DROP_USER_TABLE = "DROP TABLE users";
    private static final String SAVE_USER_TABLE = "INSERT INTO users (name, last_name, age) VALUES (?, ?, ?)";
    private static final String REMOVE_USER_TABLE = "DELETE FROM users WHERE id = ?";
    private static final String GET_ALL_USER_TABLE = "SELECT * FROM users";
    private static final String CLEAR_USER_TABLE = "DELETE FROM users";

    private final Connection connection = Util.getConnection();

    public UserDaoJDBCImpl() {

    }


    private void rollback(String sql) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            ps.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    public void createUsersTable() {
        rollback(CREATE_USER_TABLE);
    }

    public void dropUsersTable() {
        rollback(DROP_USER_TABLE);
    }

    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement ps = connection.prepareStatement(SAVE_USER_TABLE)) {
            connection.setAutoCommit(false);
            ps.setString(1, name);
            ps.setString(2, lastName);
            ps.setByte(3, age);
            ps.executeUpdate();
            connection.commit();
            System.out.println("User с иминем - " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void removeUserById(long id) {
        try (PreparedStatement ps = connection.prepareStatement(REMOVE_USER_TABLE)) {
            connection.setAutoCommit(false);
            ps.setLong(1, id);
            ps.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(GET_ALL_USER_TABLE)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                long id = rs.getLong(1);
                String name = rs.getString(2);
                String lastName = rs.getString(3);
                byte age = rs.getByte(4);
                User user = new User(name, lastName, age);
                user.setId(id);
                users.add(user);
            }
            connection.commit();
            return users;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void cleanUsersTable() {
        rollback(CLEAR_USER_TABLE);

    }
}


