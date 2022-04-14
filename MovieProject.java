import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class MovieProject {	
	public static void main(String[] args) {
		try {
			Class.forName("org.sqlite.JDBC");
		} 
		catch(ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		Connection conn =null;
		try {
			conn =DriverManager.getConnection("jdbc:sqlite:MuleSoftProject.db");
			System.out.println("Open DATABASE CONNECTION");
			try {
				deleteTable(conn);
			}
			catch(Exception ignored){			
			}
			createTable(conn);	
			System.out.println("INSERTING DATA");
			insertMovie(conn,"BEAST","VIJAY","POOJA HEGDE","NELSON",2022);
			insertMovie(conn,"KURUVI","VIJAY","TRISHA","DHARANI",2008);
			insertMovie(conn,"REMO","SIVAKARTHIKEYAN","KEERTHI SURESH","BAKKIYARAJ KANNAN",2016);
			insertMovie(conn,"24","SURIYA","SAMNTHA","VIKRAM KUMAR",2016);
			insertMovie(conn,"MAARI","DHANUSH","KAJAL AGGARWAL","BALAJI MOHAN",2015);
			System.out.println();
			System.out.println("Displaying databases");
			displayDatabase(conn,"Movies");
		}catch(SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		finally {
			if(conn!=null) {
				try {
					conn.close();
					
				}catch(SQLException e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
				}				
				}
		}
	}
	
	//TO INSERT MOVIES LIST IN THE TABLE
	public static void insertMovie(Connection conn,String movie_title,String Actor,String Actress,String Director,int DateOfRelease)throws SQLException {
		String insertSQL="INSERT INTO Movies(movie_title,Actor,Actress,Director,DateOfRelease) VALUES(?,?,?,?,?)";
		PreparedStatement pstmt=conn.prepareStatement(insertSQL);
		pstmt.setString(1,movie_title);
		pstmt.setString(2,Actor);
		pstmt.setString(3,Actress);
		pstmt.setString(4,Director);
		pstmt.setInt(5,DateOfRelease);
		pstmt.executeUpdate();		
	}
	
	//TO CREATE THE TABLE MOVIES
	private static void createTable(Connection conn)throws SQLException{
		String createTablesql=""+"CREATE TABLE Movies"+"("+"movie_title varchar(255),"+
								"Actor varchar(255), "+
								"Actress varchar(255), "+
								"Director varchar(255), "+
								"DateOfRelease integer "+
								"); "+
								"";
		Statement stmt=conn.createStatement();
		stmt.execute(createTablesql);
								
	}
	
	//DISPLAY THE THE COMPLETE TABLE 	
	public static void displayDatabase(Connection conn,String tableName)throws SQLException{
		
		//DISPLAY THE THE COMPLETE TABLE 
		String selectSQL="SELECT * from "+tableName+";";
		Statement stmt=conn.createStatement();
		ResultSet rs=stmt.executeQuery(selectSQL);
		
		System.out.println("----------"+tableName+"----------");
		while(rs.next()){
			System.out.print("MOVIE "+rs.getString("movie_title")+",");
			System.out.print(rs.getString("Actor")+",");
			System.out.print(rs.getString("Actress")+",");
			System.out.print(rs.getString("Director")+",");
			System.out.println(rs.getInt("DateOfRelease"));		
		}
		System.out.println("-------------------------------");
		//TO DISPLAY BASED where the specific Actor name is given 		
		String selectSQL1="SELECT * from "+tableName+ " where Actor='VIJAY';";
		ResultSet rs1=stmt.executeQuery(selectSQL1);
		System.out.println("----------"+tableName+ " WITH ACTOR VIJAY----------");
		while(rs1.next()){
			System.out.print("MOVIE "+rs1.getString("movie_title")+",");
			System.out.print(rs1.getString("Actor")+",");
			System.out.print(rs1.getString("Actress")+",");
			System.out.print(rs1.getString("Director")+",");
			System.out.println(rs1.getInt("DateOfRelease"));
		}		
		System.out.println("-------------------------------");
	}	
	private static void deleteTable(Connection conn)throws SQLException{
		String deleteTableSQL="DROP TABLE Movies";
		Statement stmt=conn.createStatement();
		stmt.execute(deleteTableSQL);		
	}
}
