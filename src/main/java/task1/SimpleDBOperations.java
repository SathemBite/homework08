package task1;

import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;


/**
 * Created by anton on 20.11.17.
 */
public class SimpleDBOperations {
    private static final String user = "Anton1221";
    private static final String pswd = "253649";

    private static final String tableCreationQuery = "CREATE TABLE vehicle(id INT PRIMARY KEY,class VARCHAR(30),producer VARCHAR(20),cost INT)";
    private static final String selectQuery = "SELECT * FROM vehicle WHERE class=?";
    private static final String updateQuery = "UPDATE vehicle SET cost=? WHERE cost<?";
    private static final String selectEntryQuery = "SELECT * FROM vehicle WHERE id=?";
    private static final String insertQuery = "INSERT INTO vehicle(id,class,producer,cost) VALUES(?,?,?,?)";
    private static final String dropTableQuery = "DROP TABLE vehicle";

    private static PreparedStatement entriesSelection;
    private static PreparedStatement tableUpdating;
    private static PreparedStatement entrySelection;
    private static PreparedStatement entryInsertion;
    private static PreparedStatement tableDropping;


    public static void main(String[] args) {

        Connection conn;
        ResultSet result;

        try{
            Class.forName("org.h2.Driver");
        } catch(ClassNotFoundException e){
            e.printStackTrace();
            return;
        }

        try{
            conn = DriverManager.getConnection("jdbc:h2:~/test", user, pswd);
            Statement st = conn.createStatement();
            st.execute(tableCreationQuery);
            entriesSelection = conn.prepareStatement(selectQuery);
            tableUpdating = conn.prepareStatement(updateQuery);
            entrySelection = conn.prepareStatement(selectEntryQuery);
            entryInsertion = conn.prepareStatement(insertQuery);
            tableDropping = conn.prepareStatement(dropTableQuery);



            entriesSelection.setString(1, "car");
            result = entriesSelection.executeQuery();

            tableUpdating.setInt(1, 20000);
            tableUpdating.setInt(2, 20000);
            tableUpdating.executeUpdate();

            entrySelection.setInt(1, 1);
            result = entriesSelection.executeQuery();

            entryInsertion.setInt(1, 7);
            entryInsertion.setString(2, "car");
            entryInsertion.setString(3, "Lotus");
            entryInsertion.setInt(4, 7_800_000);
            entryInsertion.executeUpdate();

            tableDropping.executeUpdate();

        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

}
