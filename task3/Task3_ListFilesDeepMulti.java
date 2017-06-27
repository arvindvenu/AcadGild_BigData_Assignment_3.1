package hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Task3_ListFilesDeepMulti {
	public static void main(String[] args) throws Exception {
		if (args == null || args.length == 0) {
			System.out.println("Pass at least one argument");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		for(int i=0;i<args.length;i++) {
			System.out.println("File listing for "+args[i]);
			String inputPath = args[i];
			listFiles(inputPath, fs, true);
			System.out.println();
		}
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
