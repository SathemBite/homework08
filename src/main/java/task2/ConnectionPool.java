package task2;


import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;


/**
 * Created by anton on 17.11.17.
 */
public class ConnectionPool {
    private BlockingQueue<Connection> connectionQueue;
    private BlockingQueue<Connection> givenAwayConQueue;
    private static ConnectionPool connectionPool;
    public static boolean isConnectionPoolExists;

    private static String driverName;
    private static String url;
    private static String user;
    private static String pswd;
    private static int poolSize;

    private ConnectionPool() throws SQLException, ClassNotFoundException{
        DBResourceManager dbResourseManager = DBResourceManager.getInstance();
        this.driverName = dbResourseManager.getValue(DBParameters.DB_DRIVER);
        this.url = dbResourseManager.getValue(DBParameters.DB_URL);
        this.user = dbResourseManager.getValue(DBParameters.DB_USER);
        this.pswd = dbResourseManager.getValue(DBParameters.DB_PSWD);

        try {
            this.poolSize = Integer.parseInt(dbResourseManager
                    .getValue(DBParameters.DB_POOL_SIZE));
        } catch (NumberFormatException e) {
            poolSize = 4;
        }

        initPoolData();
    }

    private void initPoolData() throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        givenAwayConQueue = new ArrayBlockingQueue<>(poolSize);
        connectionQueue = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            Connection connection = DriverManager.getConnection(url,
                    user,
                    pswd);
            PooledConnection pooledConnection = new PooledConnection(
                    connection);
            connectionQueue.add(pooledConnection);
        }
    }

    public static ConnectionPool getInstance() {
        if (isConnectionPoolExists){
            return connectionPool;
        }else{
            try{
                connectionPool = new ConnectionPool();
                return connectionPool;
            }catch (SQLException|ClassNotFoundException e){
                System.out.println("Connection pool didn't create");
                throw new RuntimeException(e);
            }
        }
    }

    public Connection getConnection() throws InterruptedException {
        Connection connection = null;
        try {
            connection = connectionQueue.take();
            givenAwayConQueue.add(connection);
        } catch (InterruptedException e) {
            System.out.println("Error getting connection");
            throw e;
        }
        return connection;
    }

    public void closeConnection(Connection con) {
        try {
            con.close();
        } catch (SQLException e) {
            System.out.println("Connection didn't return in pool");
        }
    }

    private void closeConnectionsQueue(BlockingQueue<Connection> queue)
            throws SQLException {
        Connection connection;
        while ((connection = queue.poll()) != null) {
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
            ((PooledConnection) connection).reallyClose();
        }
    }









    class PooledConnection implements Connection{
        Connection connection;

        public PooledConnection(Connection conn) throws SQLException {
            connection = conn;
            connection.setAutoCommit(true);
        }

        public void reallyClose() throws SQLException{
            connection.close();
        }

        @Override
        public void close() throws SQLException {
            if (connection.isClosed()) {
                throw new SQLException("Attempting to close closed connection.");
            }
            if (connection.isReadOnly()) {
                connection.setReadOnly(false);
            }
            if (!givenAwayConQueue.remove(this)) {
                throw new SQLException("Error deleting connection from the given away connections pool.");
            }
            if (!connectionQueue.offer(this)) {
                throw new SQLException("Error returning connection in the pool.");
            }
        }

        @Override
        public void setSchema(String schema) throws java.sql.SQLException {
            this.connection.setSchema(schema);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws java.sql.SQLException {
            return this.connection.prepareStatement(sql, columnNames);
        }

        @Override
        public void clearWarnings() throws java.sql.SQLException {
            this.connection.clearWarnings();
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws java.sql.SQLException {
            this.connection.setNetworkTimeout(executor, milliseconds);
        }

        @Override
        public void abort(Executor executor) throws java.sql.SQLException {
            this.connection.abort(executor);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws java.sql.SQLException {
            return this.connection.isWrapperFor(iface);
        }

        @Override
        public void beginRequest() throws java.sql.SQLException {
            this.connection.beginRequest();
        }

        @Override
        public Blob createBlob() throws java.sql.SQLException {
            return this.connection.createBlob();
        }

        @Override
        public CallableStatement prepareCall(String sql) throws java.sql.SQLException {
            return this.connection.prepareCall(sql);
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return this.connection.createSQLXML();
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            this.connection.setAutoCommit(autoCommit);
        }

        @Override
        public String getCatalog() throws SQLException {
            return this.connection.getCatalog();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return this.connection.prepareStatement(sql);
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return this.connection.unwrap(iface);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return this.connection.getTransactionIsolation();
        }

        @Override
        public void commit() throws SQLException {
            this.connection.commit();
        }

        @Override
        public int getHoldability() throws SQLException {
            return this.connection.getHoldability();
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return this.connection.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return this.connection.isValid(timeout);
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return this.connection.getWarnings();
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            this.connection.releaseSavepoint(savepoint);
        }

        @Override
        public boolean isClosed() throws SQLException {
            return this.connection.isClosed();
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return this.connection.nativeSQL(sql);
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return this.connection.getNetworkTimeout();
        }

        @Override
        public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
            this.connection.setShardingKey(shardingKey, superShardingKey);
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return this.connection.setSavepoint();
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public Statement createStatement() throws SQLException {
            return this.connection.createStatement();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            this.connection.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return this.connection.isReadOnly();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return this.connection.getClientInfo();
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            this.connection.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return this.connection.getClientInfo(name);
        }

        @Override
        public Clob createClob() throws SQLException {
            return this.connection.createClob();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return this.connection.getMetaData();
        }

        @Override
        public String getSchema() throws SQLException {
            return this.connection.getSchema();
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            this.connection.setTypeMap(map);
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            this.connection.rollback(savepoint);
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            this.connection.setClientInfo(name, value);
        }

        @Override
        public void setShardingKey(ShardingKey shardingKey) throws SQLException {
            this.connection.setShardingKey(shardingKey);
        }

        @Override
        public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
            return this.connection.setShardingKeyIfValid(shardingKey, timeout);
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            this.connection.setCatalog(catalog);
        }

        @Override
        public NClob createNClob() throws SQLException {
            return this.connection.createNClob();
        }

        @Override
        public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException {
            return this.connection.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return this.connection.getTypeMap();
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return this.connection.createStruct(typeName, attributes);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return this.connection.prepareStatement(sql, columnIndexes);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return this.connection.getAutoCommit();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return this.connection.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override
        public void rollback() throws SQLException {
            this.connection.rollback();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            this.connection.setTransactionIsolation(level);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            this.connection.setHoldability(holdability);
        }

        @Override
        public void endRequest() throws SQLException {
            this.connection.endRequest();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return this.connection.setSavepoint(name);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return this.connection.createArrayOf(typeName, elements);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }
    }
}
