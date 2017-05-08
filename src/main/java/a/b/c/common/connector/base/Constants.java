package a.b.c.common.connector.base;


/**
 * Constants Interface for connectors package
 * 
 * @author fahd-externe.essid@edf.fr
 */
public interface Constants {
  
  // Databases Default parameters
  public static final int       SQL_SERVER_DEFAULT_PORT         = 1433;
  public static final int       ORACLE_DEFAULT_PORT             = 1521;
  
  // ElasticSearch default TCP port number
  public static final int       ELASTIC_DEFAULT_TCP_PORT        = 9300;
  
  // Kafka default parameters
  public static final int       KAFKA_DEFAULT_POSITION          = 1;
  public static final int       KAFKA_DEFAULT_POLL_TIMEOUT      = 10000;
  public static final int       KAFKA_DEFAULT_BROKER_PORT       = 9092;
  public static final int       KAFKA_DEFAULT_ZOOKEEPER_PORT    = 2181;
  
  // Hbase default scan parameters
  public static final int       HBASE_DEFAULT_SCAN_CACHE        = 50;
  public static final int       HBASE_DEFAULT_SCAN_BATCH        = 100;

  // properties files default names
  public static final String    DEFAULT_CORE_SITE               = "core-site.xml";
  public static final String    DEFAULT_HDFS_SITE               = "hdfs-site.xml";
  public static final String    DEFAULT_HBASE_SITE              = "hbase-site.xml";
  public static final String    DEFAULT_HIVE_SITE               = "hive-site.xml";
  public static final String    DEFAULT_KRB_CONF                = "krb5.conf";
}
