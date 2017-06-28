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
    ResultSet rs = null;
    ResultSet columnRS = null;
    ResultSetMetaData rsmd = null;

    //constructor
    public DBFunctionalities() {

        //connect to the databases
        connectToOracle();
        //connectToMongoDB();

        //initial piece of the script
        script = new StringBuilder("use test;\n");

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

        rs = resultSetTable();

        String tableName = null;
        String scriptPiece = null;
        String columnName = null;
        int totalColumns = 0;
        int column = 0;

        rs.next();
        tableName = rs.getString("TABLE_NAME");

        columnRS = resultSetColumns(tableName);
        rsmd = resultSetMetaData();
        totalColumns = rsmd.getColumnCount();

        while (columnRS.next()) {
            scriptPiece = "db." + tableName + ".insert({_id:{";
            column = 1;
            columnName = rsmd.getColumnName(column).toLowerCase();
            scriptPiece += columnName + ":";

            if (isChar(rsmd, column) || isVarchar(rsmd, column)) {
                scriptPiece += "'" + columnRS.getString(column) + "'";
            } else if (isNumber(rsmd, column)) {
                scriptPiece += Integer.toString(columnRS.getInt(column));
            } else if (isDate(rsmd, column)) {
                scriptPiece += "'" + columnRS.getDate(column).toString() + "'";
            }

            for (column = 2; column <= totalColumns; column++) {
                columnName = rsmd.getColumnName(column).toLowerCase();
                scriptPiece += ", " + columnName + ":";

                if (isChar(rsmd, column) || isVarchar(rsmd, column)) {
                    scriptPiece += "'" + columnRS.getString(column) + "'";
                } else if (isNumber(rsmd, column)) {
                    scriptPiece += Integer.toString(columnRS.getInt(column));
                } else if (isDate(rsmd, column)) {
                    scriptPiece += "'" + columnRS.getDate(column).toString() + "'";
                }

            }

            scriptPiece += "});\n";
            script.append(scriptPiece);

        }

        showScript();

    }

    public ResultSet resultSetTable() {

        //crate a query in Oracle database
        String query = "SELECT DISTINCT TABLE_NAME FROM USER_TAB_COLUMNS "
                + "WHERE TABLE_NAME LIKE 'F%' ORDER BY TABLE_NAME";

        ResultSet result = null;
        try {
            result = connection.createStatement().executeQuery(query);
        } catch (SQLException ex) {
            System.out.println("Error trying to create query statement");
        }

        return result;
    }

    public ResultSet resultSetColumns(String tableName) {

        //inner iteration: gets the table columns
        String query = "SELECT * FROM " + tableName;

        ResultSet result = null;
        try {
            result = connection.createStatement().executeQuery(query);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column result set");
        }

        return result;
    }

    public ResultSetMetaData resultSetMetaData() {
        ResultSetMetaData result = null;
        try {
            result = columnRS.getMetaData();
        } catch (SQLException ex) {
            System.out.println("Error trying to create metadata result set");
        }

        return result;
    }

    public ResultSet resultSetPKTest(String tableName) {
        ResultSet rs = null;
        String query = "SELECT POSITION FROM USER_CONS_COLUMNS "
                + "WHERE TABLE_NAME LIKE 'F%' AND CONSTRAINT_NAME LIKE 'PK%' "
                + "AND TABLE_NAME = '" + tableName + "' AND POSITION = 2";
        try {
            rs = connection.createStatement().executeQuery(query);
        } catch (SQLException ex) {
            System.out.println("Error trying to get primary key result set");
        }

        return rs;
    }

    public ResultSet resultSetFKTest(String tableName) {
        ResultSet rs = null;
        String query = "SELECT POSITION FROM USER_CONS_COLUMNS "
                + "WHERE TABLE_NAME LIKE 'F%' AND CONSTRAINT_NAME LIKE 'FK%' "
                + "AND TABLE_NAME = '" + tableName + "' AND POSITION = 2";
        try {
            rs = connection.createStatement().executeQuery(query);
        } catch (SQLException ex) {
            System.out.println("Error trying to get foreign key result set");
        }

        return rs;
    }

    boolean isVarchar(ResultSetMetaData rsmd, int column) {
        int type = 0;
        try {
            type = rsmd.getColumnType(column);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column type");
        }
        return (type == Types.VARCHAR);
    }

    boolean isChar(ResultSetMetaData rsmd, int column) {
        int type = 0;
        try {
            type = rsmd.getColumnType(column);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column type");
        }
        return (type == Types.CHAR);
    }

    boolean isNumber(ResultSetMetaData rsmd, int column) {
        int type = 0;
        try {
            type = rsmd.getColumnType(column);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column type");
        }
        return (type == Types.INTEGER);
    }

    boolean isDate(ResultSetMetaData rsmd, int column) {
        int type = 0;
        try {
            type = rsmd.getColumnType(column);
        } catch (SQLException ex) {
            System.out.println("Error trying to get column type");
        }
        return (type == Types.DATE);
    }

    public boolean isSimpleKey(ResultSet rs) {
        int position = 0;
        try {
            position = rs.getInt("POSITION");
        } catch (SQLException ex) {
            System.out.println("Error trying to get key type");
        }

        return (position != 2);
    }

    public boolean isForeignKey(ResultSet rs) {
        int position = 0;
        try {
            position = rs.getInt("POSITION");
        } catch (SQLException ex) {
            System.out.println("Error trying to get key type");
        }

        return (position == 2);
    }

    public String getForeignKeyConstraintName(ResultSetMetaData rs, int column) {
        return "";
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
