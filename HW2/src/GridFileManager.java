import java.sql.*;

public class GridFileManager {
	
	//path to test.sqlite file PLEASE CHANGE THIS
	private String path = "/Users/administrator/Documents/Year4.5/CS157B/HW2/157B-HW2/HW2/src/test.sqlite";
	public GridFileManager(String databaseName) {
		Connection conn = null;
		try{
			System.out.println("Connecting to SQLite Server...");
			String url = "jdbc:sqlite:" + path;
			conn = DriverManager.getConnection(url);
			System.out.println("Connection to database has been established.");
            
			dropTables(conn);
			createTables(conn);
            
	    } catch (SQLException e) {
	        System.out.println(e.getMessage());
	    } finally {
	        try {
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
		String dbName = args[0];
		String file = args[1];
		GridFileManager fileManager = new GridFileManager(dbName);
	}
	
	public boolean createGridFile(String fileName, int lowX, int highX, int numLinesX, int lowY, int highY, int numLinesY, int numBuckets){
		return false;
	}
	
	public boolean add(String fileName, GridRecord record){
		return false;
	}
	
	public GridRecord[] lookup(String fileName, GridPoint pt1, GridPoint pt2, int limit_offset, int limit_count){
		return null;
	}
	
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
