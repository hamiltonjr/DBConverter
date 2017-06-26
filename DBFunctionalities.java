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

    //constructor
    public DBFunctionalities() {
        connectToOracle();
        //connectToMongoDB();
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

    public void convertToMongoDB() {

        //initialize the script
        script = new StringBuilder("use test;\n");

        //stabilish connection with the Oracle database
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
        } catch (SQLException ex) {
            System.out.println("Error trying to connect to the Oracle database");
        }

        //crate a query in Oracle database
        String query = "SELECT DISTINCT TABLE_NAME "
                + "FROM USER_TAB_COLUMNS "
                + "WHERE TABLE_NAME LIKE 'F%' "
                + "ORDER BY TABLE_NAME";
            
        //create the result set
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery(query);
        } catch (SQLException ex) {
            System.out.println("Error trying to create query statement");
        }
            
        script.append("db.");
            
        try {
            rs.next(); //catches the first table name
        } catch (SQLException ex) {
            System.out.println("Error trying to read the first query result set");
        }
        
        String tableName = null;
        try {
            tableName = rs.getString("TABLE_NAME");
        } catch (SQLException ex) {
            System.out.println("Error trying to get table name");
        }
        script.append(tableName).append(".insert({id:{");
            
        //inner iteration (tuples)
        Statement innerStmt = null;
        try {
            innerStmt = connection.createStatement();
        } catch (SQLException ex) {
            System.out.println("Error trying to create inner statement"); 
        }
        
        String innerQuery = "SELECT * FROM " + tableName;
        
        ResultSet innerRS = null;
        try {
            innerRS = innerStmt.executeQuery(innerQuery);
        } catch (SQLException ex) {
            System.out.println("Error trying to create inner result set"); 
        }
        
        ResultSetMetaData rsmd = null;
        try {
            rsmd = innerRS.getMetaData();
        } catch (SQLException ex) {
            System.out.println("Error trying to create inner metadata result set"); 
        }
            
        int count = 0;  //columns counter
        count++;

        String columnName = null;
        try {
            innerRS.next();
        } catch (SQLException ex) {
            System.out.println("Error in iterator");
        }
        
        try {
            columnName = rsmd.getColumnName(count);
        } catch (SQLException ex) {
            System.out.println("Error trying to get the column name");
        }
        script.append(columnName).append(":'");
          
        String data = "";
        try {
            data = innerRS.getString(columnName);
        } catch (SQLException ex) {
            System.out.println("Error trying to get data");
        }
        script.append(data).append("', ");
           
        count++;
        try {
            columnName = rsmd.getColumnName(count);
        } catch (SQLException ex) {
            System.out.println("Error trying to get the column name");        }
        script.append(columnName).append(":'");
            
        try {
            data = innerRS.getString(columnName);
        } catch (SQLException ex) {
            System.out.println("Error trying to get data");
        }
        script.append(data).append("'}");
            
        //end
        script.append("});\n");
            
        //showing script
        showScript();
        
        //saving script
        saveScript();
        
        //the end
        System.out.println("MongoDB script generated!");
        //System.out.println("MongoDB database generated!");
        
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
