package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;


public class UserDaoJDBCImpl implements UserDao {
    private static final String CREATE_USER_TABLE = "CREATE TABLE IF NOT EXISTS users (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(45), last_name VARCHAR(45), age INT)";
    private static final String DROP_USER_TABLE = "DROP TABLE IF EXISTS users";
    private static final String SAVE_USER_TABLE = "INSERT INTO users (name, last_name, age) VALUES (?, ?, ?)";
    private static final String REMOVE_USER_TABLE = "DELETE FROM users WHERE id = ?";
    private static final String GET_ALL_USER_TABLE = "SELECT * FROM users";
    private static final String CLEAR_USER_TABLE = "DELETE FROM users";

    private static final Logger LOGGER = Logger.getLogger(UserDaoJDBCImpl.class.getName());

    private final Connection connection = Util.getConnection();

    public UserDaoJDBCImpl() {
    }

    private void executeWithTransaction(String sql, Consumer<PreparedStatement> statementConsumer) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            statementConsumer.accept(ps);
            ps.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Ошибка выполнения SQL", e);
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                LOGGER.log(Level.SEVERE, "Ошибка при откате транзакции", rollbackEx);
                throw new RuntimeException(rollbackEx);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createUsersTable() {
        executeWithTransaction(CREATE_USER_TABLE, ps -> {
        });
    }

    @Override
    public void dropUsersTable() {
        executeWithTransaction(DROP_USER_TABLE, ps -> {
        });
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        executeWithTransaction(SAVE_USER_TABLE, ps -> {
            try {
                ps.setString(1, name);
                ps.setString(2, lastName);
                ps.setByte(3, age);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Пользователь с именем – " + name + " добавлен в базу данных");
        });
    }

    @Override
    public void removeUserById(long id) {
        executeWithTransaction(REMOVE_USER_TABLE, ps -> {
            try {
                ps.setLong(1, id);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
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
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Ошибка при получении пользователей", e);
            throw new RuntimeException(e);
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        executeWithTransaction(CLEAR_USER_TABLE, ps -> {
        });
    }
}


