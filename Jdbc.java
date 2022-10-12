import java.sql.*;
import java.sql.Connection;


public class Jdbc {
    public static void main(String[] args){
        Connection connection =null;
        Statement statement=null;

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/temp", "postgres", "root12345");
            String query="select * from ipl";
            String filepath="/Users/hariharanthirumurugan/Downloads/temp_new.csv";
            statement=connection.createStatement();
            statement.executeUpdate(query);

            if (connection != null) {
                System.out.println("Connected");
                System.out.println("finished");
            } else {
                System.out.println("Connection failed!!!");
            }


        }
        catch(Exception e){
            System.out.println(e);
        }

    }
}
