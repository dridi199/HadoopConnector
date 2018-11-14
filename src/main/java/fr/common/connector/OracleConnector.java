package fr.edf.dco.common.connector.jdbc;

import fr.edf.dco.common.connector.base.Constants;

/**
 * Oracle database access connector
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class OracleConnector extends AbstractJdbcConnector {

  //-----------------------------------------------------------------
  // CONSTRUCTOR
  //-----------------------------------------------------------------

  /**
   * Constructs an instance of SQLServercontainer
   * @param host        : sql server host adress
   * @param port        : sql saerver port
   * @param user        : database user name
   * @param password    : database user password
   * @param database    : database schema name
   */
  public OracleConnector(String host,
      int port,
      String user,
      String password,
      String database) 
  {
    super(host, port, user, password, database);
  }

  /**
   * Constructs an instance of SQLServercontainer using default port
   * @param host        : sql server host adress
   * @param user        : database user name
   * @param password    : database user password
   * @param database    : database schema name
   */
  public OracleConnector(String host,
                         String instance,
                         String user,
                         String password,
                         String database) 
  {
    super(host, Constants.ORACLE_DEFAULT_PORT, user, password, database);
    driverClassName = "oracle.jdbc.driver.OracleDriver";
    connectionUrl = "jdbc:oracle:thin:@" + this.host + ":" + port + "/" + instance;
  }
}
