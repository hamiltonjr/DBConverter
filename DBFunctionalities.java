/*
 * This code implements a simplified Oracle to MongoDB Database Converter
 * Itis part of Database Lab discipline assessment 1st semester 2017
 *
 * A script is generated. The same database is directly generated in the 
 * MongoDB dbms.
 */
package DBConverter;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;
import java.sql.Types;

/**
 * Database functionalities for the DBConverter Project
 *
 * @author hamilton
 */
public class DBFunctionalities {

    //constatnts for drivers and connections
    static final String JDBC_ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    static final String ORACLE_URL = "jdbc:oracle:thin:@grad.icmc.usp.br:15215:orcl";
    static final String USERNAME = "teste";
    static final String PASSWORD = "teste";
    static final String MONGO_URL = "127.0.0.1";
    static final int PORT = 27017;

    //connections
    Connection connection = null;
    MongoClient mongo = null;

    //database
    DB db = null;

    //buffer for the script
    StringBuilder script;

    //Result sets
    ResultSet table = null;
    ResultSet tuple = null;
    ResultSetMetaData metadata = null;

    //constructor
    public DBFunctionalities() {

        //connect to the databases
        connectToOracle();
        //connectToMongoDB();

        //initial piece of the script
        script = new StringBuilder("use FUT;\n");

    }

    /**
     * This method connects to the Oracle database
     */
    private void connectToOracle() {

        //connecting with Oracle database
        try {
            Class.forName(JDBC_ORACLE_DRIVER);
            connection = DriverManager.getConnection(ORACLE_URL, USERNAME, PASSWORD);
            System.out.println("Successfuly connected in Oracle!");
        } catch (ClassNotFoundException ex) {
            System.out.println("Error trying to open connection with Oracle database");
            System.out.println("Verify the database driver");
            System.exit(1);
        } catch (SQLException ex) {
            System.out.println("Error trying to open connection with Oracle database");
            System.out.println("Verify user and password");
            System.exit(1);
        }
    }

    /**
     * This method connects to MongoDB
     */
    private void connectToMongoDB() {
        //connecting with MongoDB
        try {
            mongo = new MongoClient(MONGO_URL, PORT);
            db = mongo.getDB("test");
            System.out.println("Successfuly connected in MongoDB!");
        } catch (Exception e) {
            System.out.println("Error trying to open connection with MongoDB");
            System.out.println("Verify the database driver, user and password");
            System.exit(1);
        }
    }

    /**
     * This method do the conversion of Oracle database to the same database in
     * MongoDB.
     */
    public void convertToMongoDB() {

        try {
            //initialize script piece many used for all the code
            String scriptPiece = "";

            //do the table manes query
            String tableName = null;
            String query = null;
            query = "SELECT TABLE_NAME FROM USER_TABLES "
                    + "WHERE TABLE_NAME LIKE 'F%'";
            table = connection.createStatement().executeQuery(query);

            //iterate for tables
            while (table.next()) {
                tableName = table.getString("TABLE_NAME");

                //query the columns (names and data)
                query = "SELECT * FROM " + tableName;
                tuple = connection.createStatement().executeQuery(query);
                metadata = tuple.getMetaData();

                //status of primary key (simple or compound)
                int pk = getPKStatus(tableName);

                //iterate in tuples
                while (tuple.next()) {
                    //bad code
                    String key = tuple.getString(1);
                    if (tableName.equals("F04_TIME")) {
                        String q = "SELECT * FROM F04_TIME WHERE TTIME = '" + key + "'";
                        ResultSet timeRS = connection.createStatement().executeQuery(q);
                        timeRS.next();
                        String sp = "db.F04_TIME.insert({_id:{ttime:'" + timeRS.getString("TTIME") + "'}, anofundacao:"
                                + timeRS.getInt("ANOFUNDACAO") + ", FK_TIME_CIDADE:{cidade:'"
                                + timeRS.getString("CIDADE") + "', siglac:'"
                                + timeRS.getString("ESTADO") + "'}, cpft:"
                                + timeRS.getInt("CPFT") + "});\n";
                        script.append(sp);
                    } else if (tableName.equals("F09_ESTADIO")) {
                        String q = "SELECT * FROM F09_ESTADIO WHERE IDESTADIO = " + key;
                        ResultSet estadioRS = connection.createStatement().executeQuery(q);
                        estadioRS.next();
                        String sp = "db.F09_ESTADIO.insert({_id:{idestadio:"
                                + estadioRS.getInt("IDESTADIO") + "}, estadio:'"
                                + estadioRS.getString("ESTADIO") + "', FK_CIDADE_ESTADO:{cidade:'"
                                + estadioRS.getString("CIDADE") + "', siglac:'"
                                + estadioRS.getString("ESTADO") + "'}, capacidade:"
                                + estadioRS.getInt("CAPACIDADE") + "});\n";
                        script.append(sp);
                    //end of bad code
                    } else {

                        int column = 1;
                        int currentColumn = 0;

                        //maximum index of columns
                        int count = metadata.getColumnCount();

                        //initialize a script line
                        scriptPiece = "db." + tableName + ".insert({_id:{";

                        //initialize many used flags
                        boolean hasBlob = false;
                        boolean hasQM = false;
                        boolean a, b, c, d, n;

                        //first column always present
                        scriptPiece += metadata.getColumnName(1).toLowerCase() + ":";
                        a = metadata.getColumnType(1) == Types.VARCHAR;
                        b = metadata.getColumnType(1) == Types.BLOB;
                        c = metadata.getColumnType(1) == Types.CHAR;
                        d = metadata.getColumnType(1) == Types.DATE;
                        n = metadata.getColumnType(1) == Types.NUMERIC;

                        hasQM = a || c || d; //has quotation mark
                        hasBlob = b;

                        String data = tuple.getString(1);

                        if (hasQM) {
                            data = "'" + data + "'";
                        }

                        scriptPiece += data;
                        currentColumn = 1;

                        //if primary key is compound
                        if (pk == 2) {
                            //ṕut second column
                            scriptPiece += ", ";
                            scriptPiece += metadata.getColumnName(2).toLowerCase() + ":";
                            a = metadata.getColumnType(2) == Types.VARCHAR;
                            b = metadata.getColumnType(2) == Types.BLOB;
                            c = metadata.getColumnType(2) == Types.CHAR;
                            d = metadata.getColumnType(2) == Types.DATE;
                            n = metadata.getColumnType(2) == Types.NUMERIC;

                            hasQM = a || c || d; //has quotation mark
                            hasBlob = hasBlob || b;

                            data = tuple.getString(2);

                            if (hasQM) {
                                data = "'" + data + "'";
                            }
                            scriptPiece += data;
                            scriptPiece += "}"; //always close
                            currentColumn = 2;
                        } else {
                            scriptPiece += "}"; //pk == 1 closeand continue
                        }

                        //for tables with many columns
                        currentColumn++;
                        if (count > 3) {

                            for (column = currentColumn; column < count; column++) {
                                scriptPiece += ", " + metadata.getColumnName(column).toLowerCase() + ":";
                                a = metadata.getColumnType(column) == Types.VARCHAR;
                                b = metadata.getColumnType(column) == Types.BLOB;
                                c = metadata.getColumnType(column) == Types.CHAR;
                                d = metadata.getColumnType(column) == Types.DATE;
                                n = metadata.getColumnType(column) == Types.NUMERIC;

                                hasQM = a || c || d; //has quotation mark
                                hasBlob = hasBlob || b;

                                data = tuple.getString(column);
                                if (hasQM) {
                                    data = "'" + data + "'";
                                }
                                scriptPiece += data;
                            }

                            //last column
                            scriptPiece += ", " + metadata.getColumnName(count).toLowerCase() + ":";
                            a = metadata.getColumnType(count) == Types.VARCHAR;
                            b = metadata.getColumnType(count) == Types.BLOB;
                            c = metadata.getColumnType(count) == Types.CHAR;
                            d = metadata.getColumnType(count) == Types.DATE;
                            n = metadata.getColumnType(count) == Types.NUMERIC;

                            hasQM = a || c || d; //has quotation mark
                            hasBlob = hasBlob || b;

                            data = tuple.getString(count);
                            if (hasQM) {
                                data = "'" + data + "'";
                            }
                            scriptPiece += data + "}";

                        } else if (count == 3) {
                            if (pk == 1) {
                                for (column = currentColumn; column < count; column++) {
                                    scriptPiece += ", " + metadata.getColumnName(column).toLowerCase() + ":";
                                    a = metadata.getColumnType(column) == Types.VARCHAR;
                                    b = metadata.getColumnType(column) == Types.BLOB;
                                    c = metadata.getColumnType(column) == Types.CHAR;
                                    d = metadata.getColumnType(column) == Types.DATE;
                                    n = metadata.getColumnType(column) == Types.NUMERIC;

                                    hasQM = a || c || d; //has quotation mark
                                    hasBlob = hasBlob || b;

                                    data = tuple.getString(column);
                                    if (hasQM) {
                                        data = "'" + data + "'";
                                    }
                                    scriptPiece += data;
                                }
                            }

                            //last column
                            scriptPiece += ", " + metadata.getColumnName(count).toLowerCase() + ":";
                            a = metadata.getColumnType(count) == Types.VARCHAR;
                            b = metadata.getColumnType(count) == Types.BLOB;
                            c = metadata.getColumnType(count) == Types.CHAR;
                            d = metadata.getColumnType(count) == Types.DATE;
                            n = metadata.getColumnType(count) == Types.NUMERIC;

                            hasQM = a || c || d; //has quotation mark
                            hasBlob = hasBlob || b;

                            data = tuple.getString(count);
                            if (hasQM) {
                                data = "'" + data + "'";
                            }
                            scriptPiece += data + "}";

                        } else if (pk == 1) {
                            //last column
                            scriptPiece += ", " + metadata.getColumnName(count).toLowerCase() + ":";
                            a = metadata.getColumnType(count) == Types.VARCHAR;
                            b = metadata.getColumnType(count) == Types.BLOB;
                            c = metadata.getColumnType(count) == Types.CHAR;
                            d = metadata.getColumnType(count) == Types.DATE;
                            n = metadata.getColumnType(count) == Types.NUMERIC;

                            hasQM = a || c || d; //has quotation mark
                            hasBlob = hasBlob || b;

                            data = tuple.getString(count);
                            if (hasQM) {
                                data = "'" + data + "'";
                            }
                            scriptPiece += data + "}";
                        }

                        scriptPiece += ");\n";
                        if (hasBlob) {
                            scriptPiece += "//BLOB detected and no implemented!\n";
                        }

                        //append the created script line
                        script.append(scriptPiece);

                    } //end of tuple iteration

                } //end of table iteration
            }

        } catch (SQLException ex) {
            System.out.println("Error trying to convert database");
        }

    }

    /**
     * This method queries the primary key status
     *
     * @param tableName the table name
     * @return the primary key status (1-simple 2-compound)
     */
    public int getPKStatus(String tableName) {
        ResultSet rs = null;
        String query = "SELECT MAX(POSITION) AS MAXIMO FROM USER_CONS_COLUMNS "
                + "WHERE TABLE_NAME LIKE 'F%' AND CONSTRAINT_NAME LIKE 'PK%' "
                + "AND TABLE_NAME = '" + tableName + "' "
                + "GROUP BY TABLE_NAME";

        try {
            rs = connection.createStatement().executeQuery(query);
        } catch (SQLException ex) {
            System.out.println("Error trying to get primary key result set");
        }

        int position = 0;
        try {
            rs.next();
            position = rs.getInt("MAXIMO");
        } catch (SQLException ex) {
            System.out.println("Error trying to get key type");
        }

        return position;
    }

    /**
     * This method queries the foreign key constraint name
     *
     * @param tableName the table name
     * @return the foreign key constraint name
     */
    public String getFKConstraintName(String tableName) {
        ResultSet result = null;
        String fkConstraintName = null;
        String query = "SELECT CONSTRAINT_NAME FROM USER_CONS_COLUMNS "
                + "WHERE TABLE_NAME = '" + tableName + "' "
                + "AND CONSTRAINT_NAME LIKE 'FK%' "
                + "GROUP BY CONSTRAINT_NAME";
        try {
            result = connection.createStatement().executeQuery(query);
            result.next();
            fkConstraintName = result.getString("CONSTRAINT_NAME");
        } catch (SQLException ex) {
            System.out.println("Error trying to get foreign key constraint name");
        }

        return fkConstraintName;
    }

    /**
     * This method queries the foreign key column name
     *
     * @param tableName the table name
     * @return the foreign key column name
     */
    public String getFKConstraintColumnName(String tableName) {
        ResultSet result = null;
        String fkConstraintColumnName = null;
        String query = "SELECT COLUMN_NAME FROM USER_CONS_COLUMNS "
                + "WHERE TABLE_NAME = '" + tableName + "' "
                + "AND CONSTRAINT_NAME LIKE 'FK%'";

        try {
            result = connection.createStatement().executeQuery(query);
            result.next();
            fkConstraintColumnName = result.getString("CONSTRAINT_NAME");
        } catch (SQLException ex) {
            System.out.println("Error trying to get foreign key constraint column name");
        }

        return fkConstraintColumnName;
    }

    /**
     * Thismethod shows the script
     */
    public void showScript() {
        System.out.println(script.toString());
    }

    /**
     * This method saves the script in a text file (with .js extension)
     */
    public void saveScript() {
        try (PrintWriter out = new PrintWriter("FUT.js")) {
            out.println("//This is a simple BSON script");
            out.println(script.toString());
        } catch (FileNotFoundException ex) {
            System.out.println("Unable to open file");
            System.exit(1);
        }
    }

}
