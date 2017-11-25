package task2;

import java.sql.*;

/**
 * Created by anton on 18.11.17.
 */
public class LibraryDAO implements AutoCloseable{
    private static ConnectionPool connectionPool;
    private Connection connection;

    private PreparedStatement bookSearch;
    private PreparedStatement bookInsertion;
    private PreparedStatement bookDeleting;

    static{
        connectionPool = ConnectionPool.getInstance();
    }

    public LibraryDAO() throws InterruptedException, SQLException{
        connection = connectionPool.getConnection();
        bookSearch = connection.prepareStatement(
                "SELECT * FROM booklibrary WHERE id=?");
        bookInsertion = connection.prepareStatement(
                "INSERT INTO booklibrary(id, title, author) VALUES(?,?,?)");
        bookDeleting = connection.prepareStatement(
                "DELETE * FROM booklibrary WHERE id=?");
    }

    @Override
    public void close(){
        connectionPool.closeConnection(connection);
    }

    public void insert(Book book) throws SQLException{
        bookInsertion.setInt(1, book.getId());
        bookInsertion.setString(2, book.getTitle());
        bookInsertion.setString(3, book.getAuthor());
        bookInsertion.executeUpdate();
    }

    public Book searchById(int id) throws SQLException{
        bookSearch.setInt(1, id);
        ResultSet res = bookSearch.executeQuery();
        return new Book(
                res.getInt(1),
                res.getString(2),
                res.getString(3)
        );
    }

    public void deleteBook(int id) throws SQLException{
        bookDeleting.setInt(1, id);
        bookDeleting.executeUpdate();
    }

    private void closeStatements() throws SQLException{
        bookInsertion.close();
        bookSearch.close();
        bookDeleting.close();
    }
}
