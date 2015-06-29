package slmt.courses.cp.hw3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFS {
	
	public static boolean mv(String oldPath, String newPath) {
		try {
			Path pathObj = new Path(oldPath);
			FileSystem hdfs = pathObj.getFileSystem(new Configuration());
			
			return hdfs.rename(pathObj, new Path(newPath));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static boolean rmdir(String path) {
		try {
			Path pathObj = new Path(path);
			FileSystem hdfs = pathObj.getFileSystem(new Configuration());
			
			return hdfs.delete(pathObj, true);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
}
