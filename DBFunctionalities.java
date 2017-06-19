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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Database functionalities for the DBConverter Project
 * @author hamilton
 */
public class DBFunctionalities {
    
    static final String JDBC_ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    static final String ORACLE_URL = "jdbc:oracle:thin:@grad.icmc.usp.br:15215:orcl";
    static final String USERNAME = "teste";
    static final String PASSWORD = "teste";
    static final String MONGO_URL = "locathost";
    static final int PORT = 27017;

    Connection connection = null;
    Statement stmt_tables = null;
    Statement stmt_columns = null;
    Statement stmt_data = null;
    ResultSet rs_tables = null;
    ResultSet rs_columns = null;
    ResultSet rs_data = null;
    String table_query;
    String columns_query;
    String data_query;
    String table_name;
    String column_name;
    StringBuilder script;
    
    //constructor
    public DBFunctionalities() {}

    public void connectToOracle() {
    
        //connecting with Oracle database
        try {
            Class.forName(JDBC_ORACLE_DRIVER);
            connection = DriverManager.getConnection(ORACLE_URL, USERNAME, PASSWORD);
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
    
    public void connectToMongoDB() {}
    
    public void convertToMongoDB() {
        
        //initialize the script
        script  = new StringBuilder("use FUT;\n");
        
        
        System.out.println("Creating statement...");
        try {
            //set statements
            stmt_tables = connection.createStatement();
            stmt_columns = connection.createStatement();
            stmt_data = connection.createStatement();

            //querying the table names
            table_query = "SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME LIKE 'F%'";

            //execute query for table manes
            rs_tables = stmt_tables.executeQuery(table_query);

            //iteration in the tables
            while (rs_tables.next()) {
                table_name = rs_tables.getString("TABLE_NAME");
                
                //querying the column names (using the table name)
                columns_query = "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + table_name + "'";
                
                //execute query for the column names
                rs_columns = stmt_columns.executeQuery(columns_query);
                
                //iteration in the columns
                while (rs_columns.next()) {
                    column_name = rs_columns.getString("COLUMN_NAME");
                    
                    script.append("db.");
                    script.append(table_name);
                    script.append(".insert({");
                    script.append(column_name);
                    script.append(":");
                    script.append("data");
                    script.append("});\n");
                    
                    //querying the data (using table names e column names)
                    data_query = "SELECT " + column_name + " FROM " + table_name;
                    
                    //execute query for data
                    rs_data = stmt_data.executeQuery(data_query);
                    
                    while (rs_data.next()) {
                        
                    }
                }
                System.out.println("");
            }
            
            //closing resources
            rs_tables.close();
            stmt_tables.close();
            connection.close();

        } catch (SQLException ex) {
            System.out.println("Error trying to make query in the Oracle database");
            System.exit(1);
        }
        
    }
    
    public void showStringBuilder() {
        System.out.println(script.toString());
    }
    
    public void saveScript() {
        try (PrintWriter out = new PrintWriter("FUT.js")) {
            out.println(script);
        } catch (FileNotFoundException ex) {
            System.out.println("Unable to open file");
            System.exit(1);
        }
    }
}
