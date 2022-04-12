import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class App {

    private final static String url = "jdbc:postgresql://postgres.ckchnuu4aegi.ap-south-1.rds.amazonaws.com:5432/postgres";
    private final static String user = "postgres";
    private final static String password = "postgres";

    public static void main(String[] args) {
        parameterizedBatchUpdate();
        // query1();
    }


    // public static void printSQLException(SQLException ex) {
    //     for (Throwable e: ex) {
    //         if (e instanceof SQLException) {
    //             e.printStackTrace(System.err);
    //             System.err.println("SQLState: " + ((SQLException) e).getSQLState());
    //             System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
    //             System.err.println("Message: " + e.getMessage());
    //             Throwable t = ex.getCause();
    //             while (t != null) {
    //                 System.out.println("Cause: " + t);
    //                 t = t.getCause();
    //             }
    //         }
    //     }
    // }

    private static void query1() {

        String INSERT_USERS_SQL = "SELECT * FROM benchmark WHERE columnA=2050 AND columnB=25000";

        try (Connection connection = DriverManager.getConnection(url, user, password);
            Statement preparedStatement = connection.createStatement()) {
            long start = System.nanoTime();
            ResultSet result = preparedStatement.executeQuery(INSERT_USERS_SQL);
            long end = System.nanoTime();
            long ans = end - start;
            System.out.println("Time elasped:: " + ans);
            // connection.setAutoCommit(true);
        } catch (BatchUpdateException batchUpdateException) {
            printBatchUpdateException(batchUpdateException);
        } catch (SQLException e) {
            printSQLException(e);
        }
    }

    public static void printSQLException(SQLException ex) {
        for (Throwable e: ex) {
            if (e instanceof SQLException) {
                e.printStackTrace(System.err);
                System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                System.err.println("Message: " + e.getMessage());
                Throwable t = ex.getCause();
                while (t != null) {
                    System.out.println("Cause: " + t);
                    t = t.getCause();
                }
            }
        }
    }

    private static void parameterizedBatchUpdate() {

        String INSERT_USERS_SQL_A = "INSERT INTO benchmark" + "  (thekey, columna, columnb, filler) VALUES " + " (?, ?, ?, ?);";
        
        try (Connection connection = DriverManager.getConnection(url, user, password);
            // Step 2:Create a statement using connection object
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_USERS_SQL_A)) {
            connection.setAutoCommit(false);
            Random rand1 = new Random();
            int batchTotal = 0;
            int upperBound1 = 50000;
            // List<Integer> pks = new ArrayList();
            // for(int i = 1; i <= 5000000; i++) {
            //     pks.add(i);
            // }
            // List<Integer> pkL = pks;
            // Collections.shuffle(pkL);
            System.out.println("Starting work!!");
            long start = System.nanoTime();
            // int ind = 0;
            for(int i = 1; i <= 5000000; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setInt(2, rand1.nextInt(upperBound1));
                preparedStatement.setInt(3, rand1.nextInt(upperBound1));
                preparedStatement.setString(4, "sdfdsafsdafsadfsdafsdafsda");
                preparedStatement.addBatch();
                if (batchTotal++ == 4096) {
                    System.out.println(i + " done");
                    int[] result = preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                    batchTotal=0;                    
                }
                // ind++;
            }
            if (batchTotal > 0) {
                int[] result = preparedStatement.executeBatch();
            }
            // int[] updateCounts = preparedStatement.executeBatch();
            connection.commit();
            long end = System.nanoTime();
            long ans = end - start;
            System.out.println("Time elasped:: " + ans);
            connection.setAutoCommit(true);
        } catch (BatchUpdateException batchUpdateException) {
            printBatchUpdateException(batchUpdateException);
        } catch (SQLException e) {
            printSQLException(e);
        }
    }

    public static void printBatchUpdateException(BatchUpdateException b) {

        System.err.println("----BatchUpdateException----");
        System.err.println("SQLState:  " + b.getSQLState());
        System.err.println("Message:  " + b.getMessage());
        System.err.println("Vendor:  " + b.getErrorCode());
        System.err.print("Update counts:  ");
        int[] updateCounts = b.getUpdateCounts();

        for (int i = 0; i < updateCounts.length; i++) {
            System.err.print(updateCounts[i] + "   ");
        }
    }
}