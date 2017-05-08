package a.b.c.common.connector.hadoop;

import javax.security.auth.login.Configuration;

import a.b.c.common.connector.Connectable;

/**
 * Abstract representation of Hadoop HDFS based connectors such as HDFS, Hive or
 * HBase
 * 
 * @author ahmed-externe.dridi@edf.fr
 *
 */

public abstract class AbstractHadoopConnector implements Connectable {

  //-----------------------------------------------------------------
  // CONSTRUCTOR
  //-----------------------------------------------------------------

  /**
   * Constructs from running instance properties xml files
   * @param core      core site file
   * @param base      base site file (hdfs site file, hbase site file ...)
   * @param krb5      kerberos configuration file
   * @param user      kerberos user identifier
   * @param keytab    kerberos keytab file path
   */
  public AbstractHadoopConnector(String core,
                                 String base,
                                 String krb5,
                                 String user,
                                 String keytab) 
  {
    this.coreSiteFile = core;
    this.baseSiteFile = base;
    this.krb5File = krb5;
    this.user = user;
    this.keytab = keytab;
    secured = true;
  }

  /**
   * Create a non secured connection
   * @param core
   * @param base
   */
  public AbstractHadoopConnector(String core, String base) {
    this.coreSiteFile = core;
    this.baseSiteFile = base;
    secured = false;
  }
  
  //-----------------------------------------------------------------
  // IMPLEMENTATION
  //-----------------------------------------------------------------

  /**
   * Handles Kerberos authentication process
   */
//  protected void secure() throws ConnectorException, IOException {
//    Utils.configureKerberos(krb5File);
//    UserGroupInformation.setConfiguration(conf);
//
//    UserGroupInformation.loginUserFromKeytab(user, keytab);
//    
//    System.setProperty("javax.net.ssl.trustStore", "hive_root_ca.truststore.jks");
//  }

  //-----------------------------------------------------------------
  // ACCESSORS
  //-----------------------------------------------------------------

  /**
   * Sets the Kerberos user identifier
   * @param user
   */
  public void setUser(String user) {
    this.user = user;
  }

  /**
   * Sets the Kerberos keytab file path
   * @param keytab
   */
  public void setKeytab(String keytab) {
    this.keytab = keytab;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  //-----------------------------------------------------------------
  // DATA MEMBERS
  //-----------------------------------------------------------------

  protected Configuration   conf;
  protected String          coreSiteFile;
  protected String          baseSiteFile;
  protected String          krb5File;
  protected String          user;
  protected String          keytab;
  protected boolean         secured;
}

