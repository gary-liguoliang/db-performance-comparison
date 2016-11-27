import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class JoinTest {

    public static final int EXPECTED_TOTAL = 1_000_000;

    static {
        try {
            Class.forName("org.h2.Driver");
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    String salGetIndividualEmployee = "SELECT * FROM EMPLOYEE e\n" +
            "LEFT JOIN EMPLOYEE e2 ON e.SID = e2.REPORT_TO\n";
//            "WHERE e2.SID IS NULL";

    Connection connectionH2_InMemory;
    Connection connectionH2_FileBased;
    Connection connectionMySql;

    @BeforeSuite
    public void setUp() throws Exception {
        InsertionTest insertionTest = new InsertionTest();

        connectionH2_InMemory = DriverManager.getConnection("jdbc:h2:mem:test", "sa", "");
        insertionTest.insertDataAndReturnTotal_PS(connectionH2_InMemory, EXPECTED_TOTAL, true);

        connectionH2_FileBased = DriverManager.getConnection("jdbc:h2:/tmp/h2-test/db-" + UUID.randomUUID(), "sa", "");
        insertionTest.insertDataAndReturnTotal_PS(connectionH2_FileBased, EXPECTED_TOTAL, true);

        connectionMySql = DriverManager.getConnection("jdbc:mysql://localhost:3307/pt", "root", "");
        insertionTest.insertDataAndReturnTotal_PS(connectionMySql, EXPECTED_TOTAL, true);
    }


    private int doJoin(Connection connection) throws Exception {
        int total = 0;
        long startTime = System.nanoTime();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(salGetIndividualEmployee);
        while (rs.next()) {
            int sid = rs.getInt(1);
            String name = rs.getString(3);
            // System.out.println("individual employees: " + sid + " - " + name);
            total++;
        }

        long endTime = System.nanoTime();
        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        System.out.println("join duration: [" + duration + "] million seconds - " + connection.getClass());
        return total;
    }

    @Test
    public void testJoin_H2File() throws Exception {
        Assert.assertEquals(doJoin(connectionH2_FileBased), EXPECTED_TOTAL);
    }

    @Test
    public void testJoin_MySQL() throws Exception {
        Assert.assertEquals(doJoin(connectionMySql), EXPECTED_TOTAL);

    }

    @Test
    public void testJoin_InMemory() throws Exception {
        Assert.assertEquals(doJoin(connectionH2_InMemory), EXPECTED_TOTAL);

    }
}
