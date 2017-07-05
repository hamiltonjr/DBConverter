/*
 * This code implements a simplified Oracle to MongoDB Database Converter
 * Itis part of Database Lab discipline assessment 1st semester 2017
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

    //constatnts for the drivers and connections
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

    public void convertToMongoDB() throws SQLException {

        String tableName = null;
        String query = null;

        query = "SELECT TABLE_NAME FROM USER_TABLES "
                + "WHERE TABLE_NAME LIKE 'F%'";
        table = connection.createStatement().executeQuery(query);

        while (table.next()) {
            tableName = table.getString("TABLE_NAME");
            String scriptPiece = "";

            query = "SELECT * FROM " + tableName;
            tuple = connection.createStatement().executeQuery(query);
            metadata = tuple.getMetaData();
            int pk = pkStatus(tableName);

            //iterate in tuples
            while (tuple.next()) {
                int column = 1;
                int currentColumn = 0;

                //maximum index of columns
                int count = metadata.getColumnCount();

                //initialize script piece
                scriptPiece += "db." + tableName + ".insert({_id:{";

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
            } //end of tuple iteration

            script.append(scriptPiece);
        } //end of table iteration
    }

    public int pkStatus(String tableName) {
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

    public int fkStatus(String tableName) {
        ResultSet rs = null;
        String query = "SELECT MAX(POSITION) FROM USER_CONS_COLUMNS "
                + "WHERE TABLE_NAME LIKE 'F%' AND CONSTRAINT_NAME LIKE 'FK%' "
                + "AND TABLE_NAME = '" + tableName + "' "
                + "GROUP BY TABLE_NAME";
        try {
            rs = connection.createStatement().executeQuery(query);
        } catch (SQLException ex) {
            System.out.println("Error trying to get primary key result set");
        }

        int position = 0;
        try {
            position = rs.getInt("MAX(POSITION)");
        } catch (SQLException ex) {
            System.out.println("Error trying to get key type");
        }

        return position;
    }

    boolean isVarchar(ResultSetMetaData md, int column) {
        int type = 0;
        try {
            type = md.getColumnType(column);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column type");
        }
        return (type == Types.VARCHAR);
    }

    boolean isChar(ResultSetMetaData md, int column) {
        int type = 0;
        try {
            type = md.getColumnType(column);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column type");
        }
        return (type == Types.CHAR);
    }

    boolean isNumber(ResultSetMetaData md, int column) {
        int type = 0;
        try {
            type = md.getColumnType(column);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column type");
        }
        return (type == Types.INTEGER);
    }

    boolean isDate(ResultSetMetaData md, int column) {
        int type = 0;
        try {
            type = md.getColumnType(column);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column type");
        }
        return (type == Types.DATE);
    }

    public void showScript() {
        System.out.println(script.toString());
    }

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
