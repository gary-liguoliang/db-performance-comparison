# db-performance-comparison
JDBC with H2(Embedded), MySQL

**Please note that this is a one-time run result with my MacBook.**


## Insertion

### Inserting 1M records (Records / Second)
|DB|Pure SQL Insertion|Prepared Statement|Prepared Statement - BatchSize:50|
|----| ----:|-----:|----:|
|H2 - FileBased|62,305|104,866|107,469|
|H2 - InMemory|54,630|75,392|79,859|
|MySQL|6,835|6,727|7,009|


### Inserting 10M (Prepared Statement AutoCommit)

|DB|Duration|
|----|----:|
|H2 - FileBased|102,176|
|MySQL|7,526|


## Joining - 1M records join with 1M records

|DB|Join Duration (MillionSeconds)|
|---|----:|
|H2 - FileBased|16554|
|H2 - InMemory|2490|
|MySQL|9140|



## Conclusion
**Insertion**
 - PreparedStatement can increase the insertion performance
 - Insert to FileBased embedded H2 database is faster than InMemory H2
 - Joining in InMemory H2 database is faster than MySQL and FileBased H2

## Code in Details

**Table Structure**
```SQL
CREATE TABLE EMPLOYEE
(
    SID INT(11),
    REPORT_TO INT(11),
    NAME_FIRST VARCHAR(32),
    NAME_LAST VARCHAR(32),
    EMAIL VARCHAR(128),
    ADDRESS VARCHAR(256),
    DATE_OF_BIRTH DATE
);
CREATE INDEX IDX_REPORT_TO_SID ON EMPLOYEE (REPORT_TO);
CREATE INDEX IDX_SID ON EMPLOYEE (SID);
```

**Pure SQL insertion**

```Java
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
```

**Prepared Statement**

```Java
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
```

**Prepared Statement with BatchUpdate**

```Java
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

```

**Join**
```Java
String salGetIndividualEmployee = "SELECT * FROM EMPLOYEE e\n" +
            "LEFT JOIN EMPLOYEE e2 ON e.SID = e2.REPORT_TO";

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
```
