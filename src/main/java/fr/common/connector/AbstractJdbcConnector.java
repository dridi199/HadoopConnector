package fr.edf.dco.common.connector.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import fr.edf.dco.common.connector.Connectable;
import fr.edf.dco.common.connector.base.ConnectorException;

/**
 * JDBC compatible databases connector abstraction
 * All Jdbc like connector should extend this class
 * 
 */
public abstract class AbstractJdbcConnector implements Connectable {
  
  //-----------------------------------------------------------------
  // CONSTRUCTOR
  //-----------------------------------------------------------------

  /**
   * Constructs an instance of JDBC connector
   * @param host        : host address
   * @param port        : port
   * @param user        : database user name
   * @param password    : database user password
   * @param database    : database schema name
   */
  public AbstractJdbcConnector(String host,
                               int port,
                               String user,
                               String password,
                               String database) 
  {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.database = database;
  }
  
  //-----------------------------------------------------------------
  // IMPLEMENTATION
  //-----------------------------------------------------------------
 
  /**
   * JDBC Connection
   */
  public void connect() throws ConnectorException {
    try {
    Class.forName(driverClassName);
    this.connection = DriverManager.getConnection(connectionUrl, user, password);
    } catch (ClassNotFoundException e) {
      throw new ConnectorException("JDBC driver not found in classpath : " + e.getMessage());
    } catch (SQLException e) {
      throw new ConnectorException("Error while trying to connect to Server : " + e.getMessage());
    }
  }
  
  /**
   * JDBC Disconnection
   */
  public void disconnect() throws ConnectorException {
    try {
      this.connection.close();
    } catch (SQLException e) {
     throw new ConnectorException("Error while closing connection to SQLServer " + e.getMessage());
    }
  }
  
  /**
   * Calling procedures
   * TODO: IMPLEMENT
   * 
   * @param proc
   */
  public ResultSet procedure(String proc) throws SQLException {
    CallableStatement proc_stm = connection.prepareCall("{? = call " + proc + "() }");
    proc_stm.registerOutParameter(1, Types.INTEGER);
    proc_stm.execute();

    proc_stm.getMoreResults();
    ResultSet result = proc_stm.getResultSet();

    return result;
  }
  
  /**
   * 
   * @param connection
   * @param proc
   * @param mapParam
   * @throws SQLException
   */
  public void procedureExecWithParam(String proc,Map<Integer,String> mapParam) throws SQLException {
    String columnNumber = "("; // (?,?,?,?,?)
    for(int k : mapParam.keySet()){
      columnNumber = columnNumber+"?";
      if(k<mapParam.size())
        columnNumber = columnNumber+",";
    }
    CallableStatement proc_stm = connection.prepareCall("{call " + proc +columnNumber+ ") }");
    for(int k : mapParam.keySet()){
      proc_stm.setString(k, mapParam.get(k));
    }
    proc_stm.execute();
  }
  
  /**
   * 
   * @param proc
   * @param arr
   * @throws SQLException
   */
  public void procedureExecWithParams(String proc,String[] arr) throws SQLException {
    String columnNumber = "("; // (?,?,?,?,?)
    for(int i=0;i<arr.length;i++){
      columnNumber = columnNumber+"?";
      if(i<arr.length-1)
        columnNumber = columnNumber+",";
    }
    CallableStatement proc_stm = connection.prepareCall("{call " + proc +columnNumber+ ") }");
    for(int i=0;i<arr.length;i++){
      proc_stm.setString(i+1, arr[i]);
    }
    proc_stm.execute();
  }
  /**
   * without getMoreResults()
   * @param proc
   * @return
   * @throws SQLException
   */
  public ResultSet procedureExec(String proc) throws SQLException {
    CallableStatement proc_stm = connection.prepareCall("{? = call " + proc + "() }");
    proc_stm.registerOutParameter(1, Types.INTEGER);
    proc_stm.execute();

    ResultSet result = proc_stm.getResultSet();

    return result;
  }

  /**
   * Execute SQL Query (update, insert or delete)
   * @param query query to execute
   */
  public void executeUpdate(String query) throws SQLException {
    Statement statement = connection.createStatement();
    statement.executeUpdate(query);
    statement.close();
  }
  
  /**
   * Execute SQL Query (select)
   * @param query query to execute
   */
  public ResultSet executeQuery(String query) throws SQLException {
    if (connection != null) {
      Statement statement = connection.createStatement();
      return statement.executeQuery(query);
    } else {
      return null;
    }
  }
  
  //-----------------------------------------------------------------
  // ACCESSORS
  //-----------------------------------------------------------------
  
  public Connection getConnection() {
    return this.connection;
  }
  
  public String getDriverClassName() {
    return this.driverClassName;
  }
  
  public String getConnectionUrl() {
    return this.connectionUrl;
  }
  
  public String getUser() {
    return this.user;
  }
  
  public String getUserPwd() {
    return this.password;
  }
  
  public String getHost() {
    return this.host;
  }
    
  public String getDatabase() {
    return this.database;
  }

  public int getPort() {
    return this.port;
  }
    
  //-----------------------------------------------------------------
  // DATA MEMBERS
  //-----------------------------------------------------------------
  
  protected int         port;
  protected String      host;
  protected String      database;
  protected String      user;
  protected String      password;
  protected Connection  connection;
  protected String      connectionUrl;
  protected String      driverClassName;
  protected String      instanceName;
}
