package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserServiceImpl;

public class Main {
    public static void main(String[] args) {
        UserServiceImpl userService = new UserServiceImpl();
        userService.createUsersTable();
        userService.saveUser("Nikita", "Volkov", (byte) 30);
        userService.saveUser("Max", "Zina", (byte) 26);
        userService.saveUser("Ilya", "Trubch", (byte) 26);
        userService.saveUser("Karim", "Buss", (byte) 27);
        userService.getAllUsers();
        System.out.println(userService.getAllUsers());
        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
