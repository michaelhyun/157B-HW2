import java.sql.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class GridFileManager {
	
	//path to test.sqlite file PLEASE CHANGE THIS
	private static String path = "src/test.sqlite";
	
	public GridFileManager(String databaseName) {
		Connection conn = null;
		try{
			//connect to SQLite
			System.out.println("Connecting to SQLite Server...");
			String url = "jdbc:sqlite:" + path;
			conn = DriverManager.getConnection(url);
			System.out.println("Connection to database has been established.");
            
			//drop and create tables
			dropTables(conn);
			createTables(conn);
            
	    } catch (SQLException e) {
	        System.out.println(e.getMessage());
	    } finally {
	        try {
	        	//close connection
	            if (conn != null) {
	                conn.close();
	                System.out.println("Close Connection \n");
	            }
	        } catch (SQLException ex) {
	            System.out.println(ex.getMessage());
	            			
	        }
	    }
	}
	
	public static void main(String[] args){
		//get arguments from command line
		String dbName = args[0];
		String filename = args[1];
		
		//create new GridFileManager, which establishes connection and drops/creates tables
		GridFileManager fileManager = new GridFileManager(dbName);
		
		try {
			//read from the instructions.txt file
			File file = new File("src/" + filename);
			Scanner in = new Scanner(file);
			while(in.hasNextLine()){
				String file_name = "";
				if(in.hasNext()){
					//get the first letter of each line
					String firstLetter = in.next();
					
					switch(firstLetter){
						//method create Grid File if the line starts with c
						case("c"):
							file_name = in.next();
							int lowX = in.nextInt();
							int highX = in.nextInt();
							int numLinesX = in.nextInt();
							int lowY = in.nextInt();
							int highY = in.nextInt();
							int numLinesY = in.nextInt();
							int numBuckets = in.nextInt();
							if(fileManager.createGridFile(file_name, lowX, highX, numLinesX, lowY, highY, numLinesY, numBuckets)){
								System.out.println("Finished Insert Successfully \n");
							}
							else{
								System.out.println("Error");
							}
							break;
							
						//add to the gridfile if line starts with i
						case("i"):
							file_name = in.next();
							String label = in.next();
							float x_value = in.nextFloat();
							float y_value = in.nextFloat();
							GridRecord record = new GridRecord(label, x_value, y_value);
							fileManager.add(file_name, record);
							break;
						
						//lookup function if the line starts with l
						case("l"):
							file_name = in.next();
							float p1_x = in.nextFloat();
							float p1_y = in.nextFloat();
							float p2_x = in.nextFloat();
							float p2_y = in.nextFloat();
							int limit_offset = in.nextInt();
							int limit_count = in.nextInt();
							GridPoint pt1 = new GridPoint(p1_x, p1_y);
							GridPoint pt2 = new GridPoint(p2_x, p2_y);
							fileManager.lookup(file_name, pt1, pt2, limit_offset, limit_count);
							break;
							
						//otherwise, the line doesn't start with any of these three strings
						default:
							System.out.println("Instruction does not have correct format.");
					}
				}
				else{
					break;
				}
				
				
			}
			in.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	public boolean createGridFile(String fileName, int lowX, int highX, int numLinesX, int lowY, int highY, int numLinesY, int numBuckets){
		System.out.print(fileName + " " + lowX + " " + highX + " " + numLinesX + " " + lowY + " " + highY + " " + numLinesY + " " + numBuckets + "\n");
		Connection conn = null;
		try{
			//connect to SQLite
			System.out.println("Connecting to SQLite Server...");
			String url = "jdbc:sqlite:" + path;
			conn = DriverManager.getConnection(url);
			System.out.println("Connection to database has been established.");
            
			//create row in GRID_FILE
	        String createGridFile = "INSERT INTO GRID_FILE(NAME, NUM_BUCKETS) VALUES (" + "'" + fileName + "' , " + numBuckets+ ")";
	        PreparedStatement createFile = conn.prepareStatement(createGridFile);
	        createFile.execute();
	        System.out.println("Created GRID FILE");
	        
	        //create row in GRIDX
	        String createGridX = "INSERT INTO GRIDX(LOW_VALUE, HIGH_VALUE, NUM_LINES) VALUES (" + lowX + "," + highX + "," + numLinesX + ")";
	        PreparedStatement createX = conn.prepareStatement(createGridX);
	        createX.execute();
	        System.out.println("Created GRIDX row");        
	        
	        //create row in GRIDY
	        String createGridY = "INSERT INTO GRIDY(LOW_VALUE, HIGH_VALUE, NUM_LINES) VALUES (" + lowY + "," + highY + "," + numLinesY + ")";
	        PreparedStatement createY = conn.prepareStatement(createGridY);
	        createY.execute();
	        System.out.println("Created GRIDY row");
	        return true;
	        
	    } catch (SQLException e) {
	        return false;
	    } finally {
	        try {
	        	//close connection
	            if (conn != null) {
	                conn.close();
	                System.out.println("Close Connection");
	            }
	        } catch (SQLException ex) {
	            System.out.println(ex.getMessage());
	            			
	        }
	    }
	}
	
	
	public boolean add(String fileName, GridRecord record){
		System.out.print(fileName+ " " + record.toString() + "\n");
		try{
			//get the ID and NUM_BUCKETS from GRID_FILE
			int id = 0; 
			int num_buckets = 0;
			String query = "SELECT ID, NUM_BUCKETS FROM GRID_FILE WHERE NAME = '" + fileName + "'";
			String url = "jdbc:sqlite:" + path;
	        Connection con = DriverManager.getConnection(url);                  
	        Statement stmt = con.createStatement();                                                 
	        ResultSet rs = stmt.executeQuery(query);
	        while(rs.next()) {                                                                      
	          id = rs.getInt(1);
	          num_buckets = rs.getInt(2);
	          System.out.println("ID: " + id + ", NUM_BUCKETS: " + num_buckets);
	        }                                 
	        rs.close();                                                                             
	        stmt.close();                                                                           

	        
	        //Get the entry of GRIDX where the ID matches the ID of the grid file
	        float low_x = 0;
	        float high_x = 0;
	        int num_x = 0;
	        String gridX = "SELECT LOW_VALUE, HIGH_VALUE, NUM_LINES FROM GRIDX WHERE GRID_FILE_ID = " + id;
	        ResultSet gridXset = stmt.executeQuery(gridX);
	        while(gridXset.next()){
	        	low_x = rs.getFloat(1);
	        	high_x = rs.getFloat(2);
	        	num_x = rs.getInt(3);
	        	System.out.println("LOW_X: " + low_x + " HIGH_X: " + high_x + " NUM_X: " + num_x);
	        }
	        gridXset.close();
	        
	      //Get the entry of GRIDY where the ID matches the ID of the grid file
	        float low_y = 0;
	        float high_y = 0;
	        int num_y = 0;
	        String gridY = "SELECT LOW_VALUE, HIGH_VALUE, NUM_LINES FROM GRIDY WHERE GRID_FILE_ID = " + id;
	        ResultSet gridYset = stmt.executeQuery(gridY);
	        while(gridYset.next()){
	        	low_y = rs.getFloat(1);
	        	high_y = rs.getFloat(2);
	        	num_y = rs.getInt(3);
	        	System.out.println("LOW_Y: " + low_y + " HIGH_Y: " + high_y + " NUM_Y: " + num_y);
	        }
	        gridYset.close();
	        
	        float recordX = record.point.x;
	        float recordY = record.point.y;
	        float x_increment = (high_x - low_x)/(num_x - 1);
	        float y_increment = (high_y - low_y)/(num_y-1);
	        int bucketID = 0;
	        
	        for(float x = low_x; x < high_x; x += x_increment){
	        	for(float y = low_y; y < high_y; y += y_increment){
	        		bucketID++;
	        		System.out.println("("+ x + "," + y + "), ");
	        		if(recordX >= x &&  recordX < x + x_increment && recordY >= y && recordY <= y + y_increment){
	        	        //Insert into Grid_FILE_ROW
	        	        String insertGridRecord = "INSERT INTO GRID_FILE_ROW VALUES ("+ id + "," + bucketID + ","+ recordX + "," + recordY + ", '" + record.label + "')";
	        	        PreparedStatement insert = con.prepareStatement(insertGridRecord);
	        	        insert.execute();
	        	        System.out.println("Inserted into Bucket: " + bucketID);
	        	        break;
	        		}
	        		
	        	}
	        }
	        
	        System.out.println("\n");
	        
	        con.close();     
	        return true;
		} catch (Exception e) {
			System.out.println("Error: Unable to Insert");
			return false;
		}
	}
	
	public GridRecord[] lookup(String fileName, GridPoint pt1, GridPoint pt2, int limit_offset, int limit_count){
		System.out.println(fileName  + " " + pt1.toString()  + " " + pt2.toString()  + " " + limit_offset  + " " + limit_count);
		
		ArrayList<GridRecord> recordList = new ArrayList<GridRecord>();
		
		try{
			//get the ID and NUM_BUCKETS from GRID_FILE
			int id = 0; 
			String query = "SELECT ID FROM GRID_FILE WHERE NAME = '" + fileName + "'";
			String url = "jdbc:sqlite:" + path;
	        Connection con = DriverManager.getConnection(url);                  
	        Statement stmt = con.createStatement();                                                 
	        ResultSet rs = stmt.executeQuery(query);
	        //get the ID of the Gridfile with the name "filename"
	        while(rs.next()) {                                                                      
	          id = rs.getInt(1);
	        }       
	        if(id == 0){
	        	System.out.println("Grid not found");
	        	return null;
	        }
	        rs.close();                                                                             
	        stmt.close();   
	        
	        //Get the entry of GRIDX where the ID matches the ID of the grid file
	        float low_x = 0;
	        float high_x = 0;
	        int num_x = 0;
	        String gridX = "SELECT LOW_VALUE, HIGH_VALUE, NUM_LINES FROM GRIDX WHERE GRID_FILE_ID = " + id;
	        ResultSet gridXset = stmt.executeQuery(gridX);
	        while(gridXset.next()){
	        	low_x = rs.getFloat(1);
	        	high_x = rs.getFloat(2);
	        	num_x = rs.getInt(3);
	        }
	        gridXset.close();
	        
	      //Get the entry of GRIDY where the ID matches the ID of the grid file
	        float low_y = 0;
	        float high_y = 0;
	        int num_y = 0;
	        String gridY = "SELECT LOW_VALUE, HIGH_VALUE, NUM_LINES FROM GRIDY WHERE GRID_FILE_ID = " + id;
	        ResultSet gridYset = stmt.executeQuery(gridY);
	        while(gridYset.next()){
	        	low_y = rs.getFloat(1);
	        	high_y = rs.getFloat(2);
	        	num_y = rs.getInt(3);
	        }
	        gridYset.close();
	        
	        float pt1X = pt1.x;
	        float pt1Y = pt1.y;
	        float pt2X = pt2.x;
	        float pt2Y = pt2.y;
	        float x_increment = (high_x - low_x)/(num_x - 1);
	        float y_increment = (high_y - low_y)/(num_y-1);
	        int bucketID = 0;

	        
	        for(float x = low_x; x < high_x; x += x_increment){
	        	for(float y = low_y; y < high_y; y += y_increment){
	        		bucketID++;
	        		//check if the current bucketId is within the selection rectangle
	        		if((pt1X <= x + x_increment && pt2X >= x) && (pt1Y <= y + y_increment && pt2Y >= y)){

	        			String coordinates = "SELECT X, Y, LABEL FROM GRID_FILE_ROW WHERE BUCKET_ID = " + bucketID + " AND GRID_FILE_ID = " + id;
	        			ResultSet coordinateSet = stmt.executeQuery(coordinates);
	        			
	        			//get the x and y coords of all the points inside the bucket
	        			while(coordinateSet.next()){
	        	        	float resultX = coordinateSet.getFloat(1);
	        	        	float resultY = coordinateSet.getFloat(2);
	        	        	String label = coordinateSet.getString(3);
	        	        	
	        	        	//if the gridpoint is also within the selection rect, add it to the list
	        	        	if(resultX >= pt1X && resultX <= pt2X && resultY >= pt1Y && resultY <= pt2Y){
	    	        			//if the bucketId is within the selection rect, print out the bucketId
	    	        			System.out.println("BUCKET ID: " + bucketID);
	        	        		System.out.println("ADDED: X: " + resultX + " Y: " + resultY + " LABEL: " + label);
	        	        		GridRecord record = new GridRecord(label, resultX, resultY);
	        	        		recordList.add(record);
	        	        		
	        	        	}
	        	        	
	        	        	
	        	        }
	        			coordinateSet.close();
	        			
	        		}
	        	}
	        }
	        
	        //copy the arraylist of gridpoints to an array, but only starting from limit_offset to limit_count
	        int offset = limit_offset;
	        GridRecord[] recordArray = new GridRecord[recordList.size()];
	        System.out.print("FINAL RECORD: [");
	        for(int i = 0; i < recordList.size() && i < limit_count + limit_offset; i++){
	        	if(offset != 0){
	        		offset--;
	        	}
	        	else{
	        		 System.out.print(recordList.get(i).label + ",");
		        	recordArray[i] = recordList.get(i);
	        	}
	        	
	        }
	        System.out.println("]\n");
	        
	        
	        return recordArray;
	       
		}
		 catch (Exception e) {
			 System.out.println("Failed:" + e.toString());
			 return null;
		}
		
	}
	
	//create the tables here
	public static void createTables(Connection conn){
		try{
			String createGridFile = "Create Table GRID_FILE(ID INTEGER PRIMARY KEY, NAME VARCHAR(64), NUM_BUCKETS INTEGER);";
			PreparedStatement create = conn.prepareStatement(createGridFile);
			create.execute();
	        System.out.println("Created GRID FILE");
	        
			String createGridX = "Create Table GRIDX(GRID_FILE_ID INTEGER PRIMARY KEY, LOW_VALUE INTEGER, HIGH_VALUE INTEGER, NUM_LINES INTEGER);";
			PreparedStatement create1 = conn.prepareStatement(createGridX);
			create1.execute();
	        System.out.println("Created GRIDX");
		
			String createGridY = "Create Table GRIDY(GRID_FILE_ID INTEGER PRIMARY KEY, LOW_VALUE INTEGER, HIGH_VALUE INTEGER, NUM_LINES INTEGER);";
			PreparedStatement create2 = conn.prepareStatement(createGridY);
			create2.execute();
			System.out.println("Created GRIDY");
	        		
			String createGridFileRow = "Create Table GRID_FILE_ROW(GRID_FILE_ID INTEGER, BUCKET_ID INTEGER, X REAL, Y REAL, LABEL CHAR(16), PRIMARY KEY(GRID_FILE_ID, BUCKET_ID, LABEL));";
			PreparedStatement create3 = conn.prepareStatement(createGridFileRow);
			create3.execute();
	        System.out.println("Created GRID FILE ROW");
			
	    } catch (SQLException e) {
	        System.out.println(e.getMessage());
	    }
	}
	
	//drop the tables here
	public static void dropTables(Connection conn){
		try{
			//drop GRID FILE
	        String dropGridFile = "Drop table IF EXISTS  GRID_FILE;";
	        PreparedStatement drop = conn.prepareStatement(dropGridFile);
	        drop.execute();
	        System.out.println("Dropped GRID FILE");

	        
	        //drop GRIDX
	        String dropGridX = "Drop table IF EXISTS GRIDX;";
	        PreparedStatement drop1 = conn.prepareStatement(dropGridX);
	        drop1.execute();
	        System.out.println("Dropped GRIDX");
	        
	        //drop GRIDY
	        String dropGridY = "Drop table IF EXISTS GRIDY;";
	        PreparedStatement drop2 = conn.prepareStatement(dropGridY);
	        drop2.execute();
	        System.out.println("Dropped GRIDY");
	        
	        //drop GRID FILE ROW
	        String dropGridFileRow = "Drop table IF EXISTS GRID_FILE_ROW;";
	        PreparedStatement drop3 = conn.prepareStatement(dropGridFileRow);
	        drop3.execute();
	        System.out.println("Dropped GRID FILE ROW\n");
	    } catch (SQLException e) {
	        System.out.println(e.getMessage());
	    }
	}
}
