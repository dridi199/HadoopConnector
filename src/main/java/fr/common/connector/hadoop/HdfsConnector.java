package fr.common.connector.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;


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
			this.fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void disconnect() {
		try {
			fs.close();
		} catch (Exception e) {
			e.getMessage();
		}

	}

	/**
	 * Returns Array of Hadoop File Objects located in the given path If
	 * Compressed files are found, they will be decompressed in the tempFolder
	 */
	public List<HadoopFile> getFiles(String folder) throws IOException {
		Path path = new Path(folder);
		ArrayList<HadoopFile> files = new ArrayList<HadoopFile>();

		if (path != null) {
			if (fs.exists(path)) {
				RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(path, true);
				while (fileStatusIterator.hasNext()) {
					LocatedFileStatus fileStatus = fileStatusIterator.next();
					String fileName = fileStatus.getPath().getName();

					// ignoring files like _SUCCESS
					if (fileName.startsWith("_")) {
						continue;
					}

					// // if extension patterns are set, ignoring not matching
					// files
					// if (fileExtensionsPatterns != null) {
					// if (! extensionMatch(fileName)) {
					// continue;
					// }
					// }

					files.add(new HadoopFile(fs, fileStatus.getPath()));
				}
			} else {
				// throw new ConnectorException("Path not found in HDFS : " +
				// folder);
			}
		}

		return files;
	}

	// -----------------------------------------------------------------
	// INNER CLASS : HadoopFile
	// -----------------------------------------------------------------

	/**
	 * A representation of a file located in HDFS
	 */
	public static class HadoopFile {
		/**
		 * Constructor
		 * 
		 * @param fs
		 * @param path
		 * @param archiveName
		 *            file archive name if located under a compressed file
		 * @throws IOException
		 */
		public HadoopFile(FileSystem fs, Path path) throws IOException {
			this.fs = fs;
			this.path = path;
			this.name = path.getName();
			this.archiveName = buildArchiveName();
			createReader();

		}

		private String buildArchiveName() {
			String result = path.toString().replace("/" + path.getName(), "");
			result = result.substring(result.lastIndexOf("/") + 1, result.length());

			return (result.contains(".zip") || result.contains(".gz")) ? result : null;
		}

		/**
		 * Creates a reader upon file
		 */
		public void createReader() throws IOException {
			reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		}

		/**
		 * Reads next line from file
		 */
		public String readLine() throws IOException {
			return reader.readLine();
		}

		/**
		 * Deletes file from HDFS
		 */
		public void delete() throws IOException {
			fs.delete(path, false);
		}

		public long getLastModificationDate() throws IOException {
			return fs.getFileStatus(path).getModificationTime();
		}
		
	    //-------------------------------------------------------------------
	    // ACCESSORS
	    //-------------------------------------------------------------------

	 

		private BufferedReader reader;
		public BufferedReader getReader() {
			return reader;
		}

		public void setReader(BufferedReader reader) {
			this.reader = reader;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Path getPath() {
			return path;
		}

		public void setPath(Path path) {
			this.path = path;
		}

		public FileSystem getFs() {
			return fs;
		}

		public void setFs(FileSystem fs) {
			this.fs = fs;
		}

		public String getArchiveName() {
			return archiveName;
		}

		public void setArchiveName(String archiveName) {
			this.archiveName = archiveName;
		}

		private String archiveName; // Archive Name or parent folder
		private String name;
		private Path path;
		private FileSystem fs;
	}

	@SuppressWarnings("unused")
	private String[] archiveNamePatterns;
	@SuppressWarnings("unused")
	private String[] fileNamePatterns;
	@SuppressWarnings("unused")
	private String[] fileExtensionsPatterns;
	protected Configuration conf;
	private FileSystem fs;
}
