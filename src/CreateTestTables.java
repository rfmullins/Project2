/******************************************************************/
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import simpledb.remote.SimpleDriver;
public class CreateTestTables {
 final static int maxSize=100;
 /**
  * @param args
  */
 public static void main(String[] args) {
  // TODO Auto-generated method stub
  Connection conn=null;
  Driver d = new SimpleDriver();
  String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
  String url = "jdbc:simpledb://" + host; 
  
  Random rand=null;
  Statement s=null;
  try {
   conn = d.connect(url, null);
   s=conn.createStatement();
   s.executeUpdate("Create table test1" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test2" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test3" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test4" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test5" +
     "( a1 int," +
     "  a2 int"+
   ")");

   s.executeUpdate("create sh index idx1 on test1 (a1)");
   //s.executeUpdate("create ex index idx2 on test2 (a1)");
   s.executeUpdate("create bt index idx3 on test3 (a1)");
   
   for(int i=1;i<6;i++){
    if(i!=5){
     rand=new Random(1);// ensure every table gets the same data
     
     for(int j=0;j<maxSize;j++){
      s.executeUpdate("insert into test"+i+" (a1,a2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
     }
    }
    
    else{//case where i=5
     for(int j=0;j<maxSize/2;j++){// insert 50000 records into test5
      s.executeUpdate("insert into test"+i+" (a1,a2) values("+j+","+j+ ")");
     }
    }
   }
   
   //TESTING PART 1-2
   long start, end, microseconds1, microseconds2, microseconds3, microseconds4; 
   
   System.out.println("Query: 'Select all from testi Where a1 = 1'");
   
   Statement stmt = conn.createStatement();
   start = System.nanoTime();
   ResultSet rs = stmt.executeQuery("Select a1, a2 from test4 Where a1 = 1");
   end = System.nanoTime();
   microseconds1 = (end - start) / 1000;
   System.out.println("Time to access table 4 with no index: " + microseconds1 + " microseconds");

   // Loop through the result set
   /*while (rs.next()) {
		int val =rs.getInt("a1");
		System.out.println("Value of a1: " + val);
   }*/
   
   start = System.nanoTime();
   rs = stmt.executeQuery("Select a1, a2 from test1 Where a1 = 1");
   end = System.nanoTime();
   microseconds1 = (end - start) / 1000;
   System.out.println("Time to access table 1 with static hash index: " + microseconds1 + " microseconds");

   // Loop through the result set
   /*while (rs.next()) {
		int val =rs.getInt("a1");
		System.out.println("Value of a1: " + val);
   }*/
   
   /*
   start = System.nanoTime();
   rs = stmt.executeQuery("Select a1, a2 from test2 Where a1 = 1");
   end = System.nanoTime();
   microseconds1 = (end - start) / 1000;
   System.out.println("Time to access table 2 with extensible hash index: " + microseconds1 + " microseconds");

   // Loop through the result set
   while (rs.next()) {
		int val =rs.getInt("a1");
		System.out.println("Value of a1: " + val);
   }*/
   
   start = System.nanoTime();
   rs = stmt.executeQuery("Select a1, a2 from test3 Where a1 = 1");
   end = System.nanoTime();
   microseconds1 = (end - start) / 1000;
   System.out.println("Time to access table 3 with B-tree index: " + microseconds1 + " microseconds");

   // Loop through the result set
   /*while (rs.next()) {
		int val =rs.getInt("a1");
		System.out.println("Value of a1: " + val);
   }*/
   
   conn.close();

  } catch (SQLException e) {
   // TODO Auto-generated catch block
   e.printStackTrace();
  }finally
  {
   try {
    conn.close();
   } catch (SQLException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
   }
  }
 }
}