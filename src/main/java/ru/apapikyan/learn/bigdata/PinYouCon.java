package ru.apapikyan.learn.bigdata;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.RollingFileAppender;

import ru.apapikyan.learn.bigdata.properies.AppProperties;

/**
 * PinYouCon!
 *
 */
public class PinYouCon {

	static Logger LOG = Logger.getLogger(PinYouCon.class);

	public static void main(String[] args) throws IOException {		
		String inPath = AppProperties.getSourceHDFSFilePath();
		String outPath = AppProperties.getResultHDFSFilePath();
		String hdfsHost = AppProperties.getFSdefName();
		//
		URI uri = URI.create(hdfsHost);
		Path path = new Path(uri);

		Configuration conf = new Configuration();
		// GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf.set("fs.default.name", AppProperties.getFSdefName());

		String user = AppProperties.getHDFSuser();
		FileSystem fs = null;

		try {
	        fs = FileSystem.get(uri, conf, user);
        } catch (Exception e) {
	        LOG.error(String.format( "Cannot connect to hdfs [%s].\n Processing is stopped.", uri), e);
	        System.exit(1);
	        
        }
		// fs = FileSystem.get(conf);

		LOG.info("HDFS Home Dir " + fs.getHomeDirectory());

		String[] inputFilenames = AppProperties.getFIleListFromConfig();

		// Calculation
		//String inputFilePath = new File(AppProperties.getTestHDFSFilePath(), "bid.20130606.txt.bz2").toString();
		String inputFilePath = new File(AppProperties.getTestHDFSFilePath(), AppProperties.getTestHDFSFileName()).toString();

		LOG.info("Processing " + inputFilePath + " file");

		FSDataInputStream fsdis = fs.open(new Path(inputFilePath));
		BufferedInputStream bis = new BufferedInputStream(fsdis);
		BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bis);
		BufferedReader br = new BufferedReader(new InputStreamReader(bzIn));
		// BufferedReader br = new BufferedReader(new InputStreamReader(bis));

		TreeMap<String, Integer> calculationMap = new TreeMap<String, Integer>();

		String line = br.readLine();

		int linesCount = 0;
		int pushMemInfoFlag = 0;
		while (line != null) {
			String iPinYouId = line.split("\\t")[2];
			Integer iPinYouIdCount = calculationMap.get(iPinYouId);
			iPinYouIdCount = (iPinYouIdCount == null) ? 1 : iPinYouIdCount + 1;
			calculationMap.put(iPinYouId, iPinYouIdCount);
			line = br.readLine();

			linesCount++;
			pushMemInfoFlag++;
			
			if(pushMemInfoFlag == 100000) {

				Runtime runtime = Runtime.getRuntime();
				NumberFormat format = NumberFormat.getInstance();

				StringBuilder sb = new StringBuilder();
				long maxMemory = runtime.maxMemory();
				long allocatedMemory = runtime.totalMemory();
				long freeMemory = runtime.freeMemory();

				String memMsg = String.format("free.mem=[%s]\talloc.mem=[%s]\tmaxMem=[%s]\tlines = [%s]",
						format.format(freeMemory / 1024),
						format.format(allocatedMemory / 1024),
						format.format(maxMemory / 1024),
						linesCount
						);
				
				LOG.info(memMsg);

				pushMemInfoFlag = 0;
			}
		}

		LOG.info(linesCount + " lines of " + inputFilePath + " file has been processed");
	}
}