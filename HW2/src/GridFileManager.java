import java.sql.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class GridFileManager {
	
	//path to test.sqlite file PLEASE CHANGE THIS
	private String path = "/Users/administrator/Documents/Year4.5/CS157B/HW2/157B-HW2/HW2/src/test.sqlite";
	
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
	                System.out.println("Close Connection");
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
			File file = new File("/Users/administrator/Documents/Year4.5/CS157B/HW2/157B-HW2/HW2/src/instructions.txt");
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
							fileManager.createGridFile(file_name, lowX, highX, numLinesX, lowY, highY, numLinesY, numBuckets);
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
		return false;
	}
	
	public boolean add(String fileName, GridRecord record){
		System.out.print(fileName+ " " + record.toString() + "\n");
		return false;
	}
	
	public GridRecord[] lookup(String fileName, GridPoint pt1, GridPoint pt2, int limit_offset, int limit_count){
		System.out.println(fileName  + " " + pt1.toString()  + " " + pt2.toString()  + " " + limit_offset  + " " + limit_count);
		return null;
	}
	
	//create the tables here
	public static void createTables(Connection conn){
		try{
			String createGridFile = "Create Table GRID_FILE(ID INTEGER PRIMARY KEY, NAME VARCHAR(64), NUM_BUCKETS);";
			PreparedStatement create = conn.prepareStatement(createGridFile);
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
