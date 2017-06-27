package hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Task2_ListFilesDeepSingle {
	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 1) {
			System.out.println("Pass one argument");
			System.exit(1);
		}
		String inputPath = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		listFiles(inputPath,fs,true);
		fs.close();
	}
	
	public static void listFiles(String hdfsPath, FileSystem fs, boolean recursive) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));
		if(fileStatuses!=null && fileStatuses.length > 0) {
			for(FileStatus fileStatus: fileStatuses) {
				System.out.println((fileStatus.isDirectory()?"Directory":"File")+"\t"+fileStatus.getPath().toString());
				if(fileStatus.isDirectory() && recursive) {
					listFiles(fileStatus.getPath().toString(),fs,recursive);
				}
			}
		}
	} 

}
