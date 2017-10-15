package com.test1;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Main {
	Configuration conf;
	FileSystem cl;
	FileSystem loc;
	Main() throws IOException{
		Configuration conf=new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/mapred-site.xml"));
		cl=FileSystem.get(URI.create("hdfs://quickstart.cloudera:8020"), conf);
		loc=FileSystem.getLocal(conf);
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		

		Main obj=new Main();
		obj.list("/vishal");
		System.out.println("=================================");
		obj.copyFromLocal("/home/cloudera/Desktop/sample","/vishal/");
		System.out.println("=================================");
//		obj.copyToLocal("/vishal/tr", "/home/cloudera/Desktop");
//		obj.list("/vishal");
		obj.cat("/vishal/sample");
	}
	
	public void mkdir(String source) throws IOException{
		String filename = source.substring(source.lastIndexOf('/') + 1,source.length());
		String path=source.replaceFirst(filename, "");
		Path p=new Path(path);
		if(!cl.exists(p)){
			System.out.println(": No such file or directory");
			return;
		}
		p=new Path(source);
		cl.mkdirs(p);
		System.out.println("created");
	}

	@SuppressWarnings("deprecation")
	public void rmdir(String source) throws IOException{
		Path p=new Path(source);
		if(!cl.exists(p)){
			System.out.println(source +": No such file or directory");
			return;
		}
		cl.delete(p);
		System.out.println("removed");
	}
	
	public void copyFromLocal(String src,String des) throws IOException{
		Path a=new Path(src);
		Path b=new Path(des);
		if(!loc.exists(a)){
			System.out.println(src +": No such file or directory");
			return;
		}
		if(!cl.exists(b)){
			System.out.println(des +": No such file or directory");
			return;
		}
		String filename = src.substring(src.lastIndexOf('/') + 1,src.length());
		cl.copyFromLocalFile(a, b);
		System.out.println(src+" : copied");
	}
	
	public void copyToLocal(String src,String des) throws IOException{
		Path a=new Path(src);
		Path b=new Path(des);
		if(!cl.exists(a)){
			System.out.println(src +": No such file or directory");
			return;
		}
		if(!loc.exists(b)){
			System.out.println(des +": No such file or directory");
			return;
		}
		String filename = src.substring(src.lastIndexOf('/') + 1,src.length());
		cl.copyToLocalFile(a, b);
		System.out.println(src+" : copied");
	}
	
	public void moveToLocal(String src,String des) throws IOException{
		Path a=new Path(src);
		Path b=new Path(des);
		if(!cl.exists(a)){
			System.out.println(src +": No such file or directory");
			return;
		}
		if(!loc.exists(b)){
			System.out.println(des +": No such file or directory");
			return;
		}
		String filename = src.substring(src.lastIndexOf('/') + 1,src.length());
		cl.moveToLocalFile(a, b);
		System.out.println(src+" : moved");
	}
	
	public void list(String src) throws FileNotFoundException, IllegalArgumentException, IOException{
		if(src=="")
			src="/";
		FileStatus[] status = cl.listStatus(new Path(src));
		if(status.length==0){
			System.out.println(src+"/ is empty");
			return;
		}
        for(int i=0;i<status.length;i++){
        	String temp=status[i].getPath().toString();
        	String filename = temp.substring(temp.lastIndexOf('/') + 1,temp.length());
        	System.out.print(filename);
        	if(status[i].isDirectory())
        		System.out.println("/");
        	else
        	System.out.println();
        }
	}
	
	public void cat(String src) throws IOException{
		Path a=new Path(src);
		if(!cl.exists(a)){
			System.out.println(src +": No such file or directory");
			return;
		}
		InputStream in=cl.open(a);
		IOUtils.copyBytes(in, System.out,4096);
	}
}
