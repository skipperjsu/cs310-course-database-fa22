package edu.jsu.mcis.cs310;

import java.sql.*;
import org.json.simple.*;
import org.json.simple.parser.*;

public class Database {
    
    private final Connection connection;
    private final int TERMID_SP22 = 1;
    
    /* CONSTRUCTOR */

    public Database(String username, String password, String address) {
        this.connection = openConnection(username, password, address);
    }
    
    /* PUBLIC METHODS */

    public String getSectionsAsJSON(int termid, String subjectid, String num) {
        String result = null;
        
        try {
            // set up query
            String query = "SELECT * FROM section WHERE termid = ? AND subjectid = ? AND num = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setInt(1, termid);
            pstmt.setString(2, subjectid);
            pstmt.setString(3, num);
            
            // run query and determine if it worked
            boolean hasResults = pstmt.execute();
            
            if (hasResults) {
                ResultSet results = pstmt.getResultSet();
                result = getResultSetAsJSON(results);
            }
            
        } catch (Exception e) { e.printStackTrace(); }
        
        return result;
    }
    
    public int register(int studentid, int termid, int crn) {
        
        int result = 0;
        
        try {
            
            String query = "INSERT INTO registration (studentid, termid, crn) VALUES (?,?,?)";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setInt(1, studentid);
            pstmt.setInt(2, termid);
            pstmt.setInt(3, crn);
            
            // return 1 if it worked, 0 otherwise
            result = pstmt.executeUpdate();
            
        } catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }

    public int drop(int studentid, int termid, int crn) {
        
        int result = 0;
        
        try {
            
            String query = "DELETE FROM registration WHERE studentid = ? AND termid = ? AND crn = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setInt(1, studentid);
            pstmt.setInt(2, termid);
            pstmt.setInt(3, crn);
            
            result = pstmt.executeUpdate();
            
        } catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    public int withdraw(int studentid, int termid) {
        
        int result = 0;
        
        try {
            
            String query = "DELETE FROM registration WHERE studentid = ? AND termid = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setInt(1, studentid);
            pstmt.setInt(2, termid);
            
            result = pstmt.executeUpdate();
            
        } catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    public String getScheduleAsJSON(int studentid, int termid) {
        
        String result = null;
        
        try {
            // outer query handles the schedule
            String oquery = "SELECT * FROM registration WHERE studentid = ? AND termid = ?";
            PreparedStatement opstmt = connection.prepareStatement(oquery);
            opstmt.setInt(1, studentid);
            opstmt.setInt(2, termid);
            
            boolean hasResultsO = opstmt.execute();
            
            if (hasResultsO) {
                ResultSet oresults = opstmt.getResultSet();
                // container for the separate parts of the final string
                JSONArray json = new JSONArray();
                while (oresults.next()) {
                    
                    int crn = oresults.getInt(3);
                    // inner query handles the link between registrations and sections
                    String iquery = "SELECT * FROM section WHERE crn = ?";
                    PreparedStatement ipstmt = connection.prepareStatement(iquery);
                    ipstmt.setInt(1, crn);
                    
                    // object that will be added for each row
                    JSONObject obj = new JSONObject();
                    
                    boolean hasResultsI = ipstmt.execute();
                    
                    if (hasResultsI) {
                        
                        ResultSet iresults = ipstmt.getResultSet();
                        ResultSetMetaData metadata = iresults.getMetaData();
                        int columnCount = metadata.getColumnCount();
                        
                        while (iresults.next()) {
                            // get registration info, then section info
                            obj.put("studentid", oresults.getString(1));
                            obj.put("termid", oresults.getString(2));
                            for (int i = 1; i <= columnCount; i++) {
                                obj.put(metadata.getColumnName(i), iresults.getString(i));
                            }
                        }
                    }
                    
                    json.add(obj);
                    
                }
                
                result = JSONValue.toJSONString(json);
                
            }
            
        } catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    public int getStudentId(String username) {
        
        int id = 0;
        
        try {
        
            String query = "SELECT * FROM student WHERE username = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, username);
            
            boolean hasresults = pstmt.execute();
            
            if ( hasresults ) {
                
                ResultSet resultset = pstmt.getResultSet();
                
                if (resultset.next())
                    
                    id = resultset.getInt("id");
                
            }
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return id;
        
    }
    
    public boolean isConnected() {

        boolean result = false;
        
        try {
            
            if ( !(connection == null) )
                
                result = !(connection.isClosed());
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    /* PRIVATE METHODS */

    private Connection openConnection(String u, String p, String a) {
        
        Connection c = null;
        
        if (a.equals("") || u.equals("") || p.equals(""))
            
            System.err.println("*** ERROR: MUST SPECIFY ADDRESS/USERNAME/PASSWORD BEFORE OPENING DATABASE CONNECTION ***");
        
        else {
        
            try {

                String url = "jdbc:mysql://" + a + "/jsu_sp22_v1?autoReconnect=true&useSSL=false&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=America/Chicago";
                // System.err.println("Connecting to " + url + " ...");

                c = DriverManager.getConnection(url, u, p);

            }
            catch (Exception e) { e.printStackTrace(); }
        
        }
        
        return c;
        
    }
    
    private String getResultSetAsJSON(ResultSet resultset) {
        
        String result;
        
        /* Create JSON Containers */
        
        JSONArray json = new JSONArray();
        JSONArray keys = new JSONArray();
        
        try {
            
            /* Get Metadata */
        
            ResultSetMetaData metadata = resultset.getMetaData();
            int columnCount = metadata.getColumnCount();
            
            while (resultset.next()) {
                JSONObject obj = new JSONObject();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metadata.getColumnName(i);
                    obj.put(columnName, resultset.getString(i));
                }
                json.add(obj);
            }
        }
        catch (Exception e) { e.printStackTrace(); }
        
        /* Encode JSON Data and Return */
        
        result = JSONValue.toJSONString(json);
        return result;
        
    }
    
}