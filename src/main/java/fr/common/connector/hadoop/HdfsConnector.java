package fr.common.connector.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import fr.common.connector.base.Constants;

/**
 * @author ahmed-externe.dridi@edf.fr
 *
 */
public class HdfsConnector extends AbstractHadoopConnector {

	/**
	 * Creating HDFS connector using configuration xml files Non secured
	 */
	public HdfsConnector(String coreSiteFile, String hdfsSiteFile) {
		super(coreSiteFile, hdfsSiteFile);
	}

	/**
	 * Create a hdfs connector with default parameters
	 * 
	 * @param user
	 * @param keytab
	 * @param secured
	 */
	// public HdfsConnector(String user, String keytab, boolean secured) {
	// super(Constants.DEFAULT_CORE_SITE, Constants.DEFAULT_HDFS_SITE,
	// Constants.DEFAULT_KRB_CONF, user, keytab);
	// this.secured = secured;
	// }

	/**
	 * Configure hdfs connector from running instance xml properties files
	 */
	@SuppressWarnings("unused")
	private void configure() {
		conf = new Configuration();

		conf.addResource(coreSiteFile);
		conf.addResource(baseSiteFile);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	}

	public void connect() {
		configure();
		try {
			this.fs=FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void disconnect() {
		try{
			fs.close();
		}catch (Exception e){
			e.getMessage();
		}

	}

	protected Configuration				conf;
	private FileSystem 					fs;
}
