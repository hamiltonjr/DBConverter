/*
 * This code implements a simplified Oracle to MongoDB Database Converter
 * Itis part of Database Lab discipline assessment 1st semester 2017
 */
package DBConverter;

import java.sql.SQLException;

/**
 * DBConverter main class for the DBConverter Project
 * @author hamilton
 */
public class DBConverter {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws SQLException {
        DBFunctionalities dbf = new DBFunctionalities();
        dbf.convertToMongoDB();
        dbf.showScript();
        dbf.saveScript();
    }
}
