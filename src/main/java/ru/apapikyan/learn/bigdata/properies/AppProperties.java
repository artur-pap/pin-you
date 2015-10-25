package ru.apapikyan.learn.bigdata.properies;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

import org.apache.log4j.Logger;

public class AppProperties {

	static Logger LOG = Logger.getLogger(AppProperties.class);

	static String[] FILE_LIST;
	static String FS_DEFAULT_NAME;

	public static String SS = "";

	private static final String PROP_FILE_NAME = "config.properties";
	private static AppProperties self = null;
	private Properties props;

	private AppProperties() {
		self = this;

		InputStream isProps = null;

		try {
			this.props = new Properties();

			isProps = this.getClass().getClassLoader().getResourceAsStream(PROP_FILE_NAME);

			this.props.load(isProps);
		} catch (IOException e) {
			// log.warn(String.format("Error loading properties file [%s].",
			// PROP_FILE_NAME), e);
			e.printStackTrace();
		} finally {
			if (isProps != null) {
				try {
					isProps.close();
				} catch (Exception e) {
					// do nothing
				}
			}
		}
	}

	public static String[] getFIleListFromConfig() {
		if (FILE_LIST == null) {
			FILE_LIST = getInstance().getStringProp("hdfs.source.files.list").split(",");
		}

		return FILE_LIST;
	}

	public static String getFSdefName() {
		if (FS_DEFAULT_NAME == null) {
			FS_DEFAULT_NAME = getInstance().getStringProp("hdfs.fs.default.name");
		}

		return FS_DEFAULT_NAME;
	}

	public static String getSourceHDFSFilePath() {
		return getInstance().getStringProp("hdfs.source.file.path");
	}

	public static String getResultHDFSFilePath() {
		return getInstance().getStringProp("hdfs.result.file.path");
	}

	public static String getTestHDFSFilePath() {
		return getInstance().getStringProp("hdfs.source.test.file.path");
	}

	public static String getTestHDFSFileName() {
		return getInstance().getStringProp("hdfs.source.test.file.name");
	}

	public static String getHDFSuser() {
		return getInstance().getStringProp("hdfs.user.name");
	}
	

	public static synchronized AppProperties getInstance() {
		return (self == null) ? new AppProperties() : self;
	}

	private String getStringProp(String key) {
		String retVal = null;

		if (!props.containsKey(key)) {
			retVal = "!props.containsKey(key)";
		} else {
			retVal = this.props.getProperty(key).trim();
		}

		if (LOG.isInfoEnabled()) {
			//LOG.debug(String.format(Locale.getDefault(),  "Value for key [%s] is [%s].", key, retVal));
			LOG.info(String.format("Value for key [%s] is [%s].", key, retVal));
		}

		return retVal;
	}

	/*
	 * 
	 * public static void test() { AppProperties app = new AppProperties();
	 * app.printThemAll(); }
	 * 
	 * private void printThemAll() {
	 * 
	 * Properties prop = new Properties(); InputStream input = null;
	 * 
	 * try {
	 * 
	 * String filename = "config.properties"; // System.out.println("Path is " +
	 * // getClass().getClassLoader().toString()); input =
	 * getClass().getClassLoader().getResourceAsStream(filename); if (input ==
	 * null) { System.out.println("Sorry, unable to find " + filename); return;
	 * } else {
	 * 
	 * }
	 * 
	 * prop.load(input);
	 * 
	 * Enumeration<?> e = prop.propertyNames(); while (e.hasMoreElements()) {
	 * String key = (String) e.nextElement(); String value =
	 * prop.getProperty(key); System.out.println("Key : " + key + ", Value : " +
	 * value); }
	 * 
	 * } catch (IOException ex) { ex.printStackTrace(); } finally { if (input !=
	 * null) { try { input.close(); } catch (IOException e) {
	 * e.printStackTrace(); } } }
	 * 
	 * }
	 */

}