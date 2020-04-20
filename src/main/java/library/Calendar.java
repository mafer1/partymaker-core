package library;
import model.Thing;
import model.User;
import java.sql.*;
import java.util.List;

public class Calendar {

    private static String CONNECTION_URL = "jdbc:postgresql://localhost:8012/event_base";
    private static String DB_USER = "postgres";
    private static String DB_PASSWORD = "password";

    private Connection databaseConnection;
    private Statement sqlStatement;

    public Calendar() {
        try {
            databaseConnection = DriverManager.getConnection(CONNECTION_URL, DB_USER, DB_PASSWORD);
            sqlStatement = databaseConnection.createStatement();

            databaseConnection.setAutoCommit(false);
            sqlStatement.addBatch("CREATE TABLE IF NOT EXISTS users(user_id serial primary key, name varchar, surname varchar)");
            sqlStatement.addBatch("CREATE TABLE IF NOT EXISTS things(thing_id serial primary key, name varchar)");
            int count[] = sqlStatement.executeBatch();
            databaseConnection.commit();

            System.out.println("Connected with database and successfuly create Users and Things tables");
        } catch (SQLException e) {
            System.err.println("Problem during connection");
            e.printStackTrace();
        }
    }

    public long insertUser(User user) {

        String SQL = "INSERT INTO users(name, surname) "
                + "VALUES(?,?)";

        long id = 0;

        try {PreparedStatement pstmt = databaseConnection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1, user.getName());
            pstmt.setString(2, user.getSurname());

            int affectedRows = pstmt.executeUpdate();
            if (affectedRows > 0) {
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        id = rs.getLong(1);
                    }
                }catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return id;
    }

    public void insertUsers(List<User> list) {
        String SQL = "INSERT INTO users(name, surname) "
                + "VALUES(?,?)";
        try (
                PreparedStatement statement = databaseConnection.prepareStatement(SQL);) {
            int count = 0;

            for (User user : list) {
                statement.setString(1, user.getName());
                statement.setString(2, user.getSurname());

                statement.addBatch();
                count++;
                if (count % 100 == 0 || count == list.size()) {
                    statement.executeBatch();
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public int getUserCount() {
        String SQL = "SELECT count(*) FROM users";
        int count = 0;

        try (Statement stmt = databaseConnection.createStatement();
             ResultSet rs = stmt.executeQuery(SQL)) {
            rs.next();
            count = rs.getInt(1);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        return count;
    }

    public void findUserByID(int userID) {
        String SQL = "SELECT user_id, name, surname "
                + "FROM users "
                + "WHERE user_id = ?";

        try (PreparedStatement pstmt = databaseConnection.prepareStatement(SQL)) {

            pstmt.setInt(1, userID);
            ResultSet rs = pstmt.executeQuery();
            displayUser(rs);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void getUsers() {
        String SQL = "SELECT user_id, name, surname FROM users";

        try (Statement stmt = databaseConnection.createStatement();
             ResultSet rs = stmt.executeQuery(SQL)) {
            displayUser(rs);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private void displayUser(ResultSet rs) throws SQLException {
        while (rs.next()) {
            System.out.println(rs.getString("user_id") + "\t"
                    + rs.getString("name") + " "
                    + rs.getString("surname"));
        }
    }

    public int deleteUser(int id) {
        String SQL = "DELETE FROM users WHERE user_id = ?";

        int affectedrows = 0;

        try (PreparedStatement pstmt = databaseConnection.prepareStatement(SQL)) {

            pstmt.setInt(1, id);

            affectedrows = pstmt.executeUpdate();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return affectedrows;
    }


    public long insertThing(Thing thing) {

        String SQL = "INSERT INTO things(name) "
                + "VALUES(?)";

        long id = 0;

        try {PreparedStatement pstmt = databaseConnection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1, thing.getName());

            int affectedRows = pstmt.executeUpdate();
            if (affectedRows > 0) {
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        id = rs.getLong(1);
                    }

                }catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return id;
    }

    public void insertThings(List<Thing> list) {
        String SQL = "INSERT INTO things(name) "
                + "VALUES(?)";
        try (
                PreparedStatement statement = databaseConnection.prepareStatement(SQL);) {
            int count = 0;

            for (Thing thing : list) {
                statement.setString(1, thing.getName());

                statement.addBatch();
                count++;
                // execute every 100 rows or less
                if (count % 100 == 0 || count == list.size()) {
                    statement.executeBatch();
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public int getThingCount() {
        String SQL = "SELECT count(*) FROM things";
        int count = 0;

        try (Statement stmt = databaseConnection.createStatement();
             ResultSet rs = stmt.executeQuery(SQL)) {
            rs.next();
            count = rs.getInt(1);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        return count;
    }

    public void findThingByID(int thingID) {
        String SQL = "SELECT thing_id, name "
                + "FROM things "
                + "WHERE thing_id = ?";

        try (PreparedStatement pstmt = databaseConnection.prepareStatement(SQL)) {

            pstmt.setInt(1, thingID);
            ResultSet rs = pstmt.executeQuery();
            displayThing(rs);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void getThings() {
        String SQL = "SELECT thing_id, name FROM things";

        try (Statement stmt = databaseConnection.createStatement();
             ResultSet rs = stmt.executeQuery(SQL)) {
            displayThing(rs);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private void displayThing(ResultSet rs) throws SQLException {
        while (rs.next()) {
            System.out.println(rs.getString("thing_id") + "\t"
                    + rs.getString("name") + " ");
        }
    }

    public int deleteThing(int id) {
        String SQL = "DELETE FROM things WHERE thing_id = ?";

        int affectedrows = 0;

        try (PreparedStatement pstmt = databaseConnection.prepareStatement(SQL)) {

            pstmt.setInt(1, id);

            affectedrows = pstmt.executeUpdate();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return affectedrows;
    }

    public void closeConnection() {
        try {
            databaseConnection.close();
        } catch (SQLException e) {
            System.err.println("Error during closing connection");
            e.printStackTrace();
        }
    }
}