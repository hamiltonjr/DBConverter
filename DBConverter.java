/*
 * This code implements a simplified Oracle to MongoDB Database Converter
 * Itis part of Database Lab discipline assessment 1st semester 2017
 */
package DBConverter;

/**
 * DBConverter main class for the DBConverter Project
 * @author hamilton
 */
public class DBConverter {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        DBFunctionalities db = new DBFunctionalities();
        
        db.connectToOracle();
        //db.connectToMongoDB();
        
        db.generateScript();
        
    }
    
}
