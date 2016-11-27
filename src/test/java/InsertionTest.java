import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class InsertionTest {

    static {
        try {
            Class.forName("org.h2.Driver");
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    String dropTable = "DROP TABLE IF EXISTS EMPLOYEE";
    String createTable = "CREATE TABLE EMPLOYEE (\n" +
            "SID           INTEGER,\n" +
            "REPORT_TO     INTEGER,\n" +
            "NAME_FIRST    NVARCHAR(32),\n" +
            "NAME_LAST     NVARCHAR(32),\n" +
            "EMAIL         VARCHAR(128),\n" +
            "ADDRESS       NVARCHAR(256),\n" +
            "DATE_OF_BIRTH DATE\n" +
            ");\n";
    String createIndexId = "CREATE INDEX IDX_SID ON EMPLOYEE(SID)";
    String createIndexReportTo = "CREATE INDEX IDX_REPORT_TO_SID ON EMPLOYEE(REPORT_TO)";
    String selectCount = "SELECT COUNT(1) FROM EMPLOYEE";

    int target = 10_000_000;

    @BeforeSuite
    public void beforeSuite() throws IOException {
        FileUtils.deleteDirectory(new File("/tmp/h2-test"));
    }

    private Connection connectToH2FileDB() throws Exception {
        return connectToH2FileDB(UUID.randomUUID().toString());
    }

    private Connection connectToH2FileDB(String fileName) throws Exception {
        return DriverManager.getConnection("jdbc:h2:/tmp/h2-test/db-" + fileName, "sa", "");
    }

    private Connection connectToInMemoryDB() throws Exception {
        return DriverManager.getConnection("jdbc:h2:mem:test" + UUID.randomUUID(), "sa", "");
    }

    private Connection connectToMySQL() throws Exception {
        return DriverManager.getConnection("jdbc:mysql://localhost:3307/pt", "root", "");
    }


    protected int insertDataAndReturnTotal_SQL(Connection connection, int target, boolean autoCommit) throws Exception {
        Statement statement = connection.createStatement();
        statement.execute(dropTable);
        statement.execute(createTable);
        statement.execute(createIndexId);
        statement.execute(createIndexReportTo);

        connection.setAutoCommit(autoCommit);

        long startTime = System.nanoTime();
        int id = 1;

        while (id <= target) {
            statement.execute(String.format("INSERT INTO EMPLOYEE (SID, REPORT_TO, NAME_FIRST, NAME_LAST, EMAIL, ADDRESS, DATE_OF_BIRTH)" +
                    " VALUES (%d, %d, 'EMP-FIRST-NAME', 'EMP-LAST-NAME', 'email@company.com', '#1 Hacker Road', '1980-11-25');", id, id - 1));
            if (!autoCommit && id % 50 == 0) {
                connection.commit();
            }
            id++;
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        System.out.println("insertion duration: [" + duration + "] million seconds for " + target + " records");

        ResultSet rs = statement.executeQuery(selectCount);
        int total = 0;
        if (rs.next()) {
            total = rs.getInt(1);
        }

        return total;
    }

    protected int insertDataAndReturnTotal_PS(Connection connection, int target, boolean autoCommit) throws Exception {
        Statement statement = connection.createStatement();
        statement.execute(dropTable);
        statement.execute(createTable);
        statement.execute(createIndexId);
        statement.execute(createIndexReportTo);

        connection.setAutoCommit(autoCommit);

        PreparedStatement ps = connection.prepareStatement("INSERT INTO EMPLOYEE (SID, REPORT_TO, NAME_FIRST, NAME_LAST, EMAIL, ADDRESS, DATE_OF_BIRTH)" +
                " VALUES (?, ?, 'EMP-FIRST-NAME', 'EMP-LAST-NAME', 'email@company.com', '#1 Hacker Road', '1980-11-25');");

        long startTime = System.nanoTime();
        int id = 1;

        while (id <= target) {
            ps.setInt(1, id);
            ps.setInt(2, id - 1);
            ps.execute();

            if (!autoCommit && id % 50 == 0) {
                connection.commit();
            }

            id++;
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        System.out.println("insertion duration: [" + duration + "] million seconds for " + target + " records - " + connection.getClass());

        ResultSet rs = statement.executeQuery(selectCount);
        int total = 0;
        if (rs.next()) {
            total = rs.getInt(1);
        }

        return total;
    }

    protected int insertDataPSAndReturnTotal_PSBatch(Connection connection, int target, boolean autoCommit) throws Exception {
        Statement statement = connection.createStatement();
        statement.execute(dropTable);
        statement.execute(createTable);
        statement.execute(createIndexId);
        statement.execute(createIndexReportTo);

        connection.setAutoCommit(autoCommit);

        PreparedStatement ps = connection.prepareStatement("INSERT INTO EMPLOYEE (SID, REPORT_TO, NAME_FIRST, NAME_LAST, EMAIL, ADDRESS, DATE_OF_BIRTH)" +
                " VALUES (?, ?, 'EMP-FIRST-NAME', 'EMP-LAST-NAME', 'email@company.com', '#1 Hacker Road', '1980-11-25');");

        long startTime = System.nanoTime();
        int id = 1;

        while (id <= target) {
            ps.setInt(1, id);
            ps.setInt(2, id - 1);
            ps.addBatch();

            if (id % 50 == 0) {
                ps.executeBatch();

                if(!autoCommit) {
                    connection.commit();
                }
            }
            id++;
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        System.out.println("insertion duration: [" + duration + "] million seconds for " + target + " records");

        ResultSet rs = statement.executeQuery(selectCount);
        int total = 0;
        if (rs.next()) {
            total = rs.getInt(1);
        }

        return total;
    }

    @Test(enabled = false)
    public void testH2_Insertion_PureSQL_AutoCommit() throws Exception {
        Connection conn = connectToH2FileDB();
        int actual_records_inserted = insertDataAndReturnTotal_SQL(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }

    @Test(enabled = false)
    public void testH2_Insertion_PureSQL_ManualCommit() throws Exception {
        Connection conn = connectToH2FileDB();
        int actual_records_inserted = insertDataAndReturnTotal_SQL(conn, target, false);
        assertEquals(actual_records_inserted, target);
    }

    @Test
    public void testH2_Insertion_PS_AutoCommit() throws Exception {
        Connection conn = connectToH2FileDB("fixed-path-db");
        int actual_records_inserted = insertDataAndReturnTotal_PS(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }

    @Test(enabled = false)
    public void testH2_Insertion_PS_ManualCommit() throws Exception {
        Connection conn = connectToH2FileDB();
        int actual_records_inserted = insertDataAndReturnTotal_PS(conn, target, false);
        assertEquals(actual_records_inserted, target);
    }

    @Test(enabled = false)
    public void testH2_Insertion_PS_BatchAutoCommit() throws Exception {
        Connection conn = connectToH2FileDB();
        int actual_records_inserted = insertDataPSAndReturnTotal_PSBatch(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }

    @Test(enabled = false)
    public void testH2_Insertion_PSBatch_ManualCommit() throws Exception {
        Connection conn = connectToH2FileDB();
        int actual_records_inserted = insertDataPSAndReturnTotal_PSBatch(conn, target, false);
        assertEquals(actual_records_inserted, target);
    }



    @Test(enabled = false)
    public void testH2InMemory_Insertion_PS_BatchAutoCommit() throws Exception {
        Connection conn = connectToInMemoryDB();
        int actual_records_inserted = insertDataPSAndReturnTotal_PSBatch(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }

    @Test(enabled = false)
    public void testH2InMemory_Insertion_PS_AutoCommit() throws Exception {
        Connection conn = connectToInMemoryDB();
        int actual_records_inserted = insertDataAndReturnTotal_PS(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }

    @Test(enabled = false)
    public void testH2InMemory_Insertion_SQL_AutoCommit() throws Exception {
        Connection conn = connectToInMemoryDB();
        int actual_records_inserted = insertDataAndReturnTotal_SQL(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }

    @Test(enabled = false)
    public void testMySQL_Insertion_PS_BatchAutoCommit() throws Exception {
        Connection conn = connectToMySQL();
        int actual_records_inserted = insertDataPSAndReturnTotal_PSBatch(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }

    @Test
    public void testMySQL_Insertion_PS_AutoCommit() throws Exception {
        Connection conn = connectToMySQL();
        int actual_records_inserted = insertDataAndReturnTotal_PS(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }

    @Test(enabled = false)
    public void testMySQL_Insertion_SQL_AutoCommit() throws Exception {
        Connection conn = connectToMySQL();
        int actual_records_inserted = insertDataAndReturnTotal_SQL(conn, target, true);
        assertEquals(actual_records_inserted, target);
    }
}