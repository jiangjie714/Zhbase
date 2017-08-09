package hbase.dao.imp.test;

import java.awt.Image;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

public class Test {
	
	public static void main(String[] args) {
		 try {
			 FileInputStream finput =new FileInputStream(new File("C:/Users/jie/Desktop/命令2.txt"));
//			Image image = ImageIO.read(new File("C:/Users/jie/Desktop/头像.jpg"));
//			image.flush();
			 byte[] b=new byte[finput.available()];
			 finput.read(b);
			 finput.close();
			 String str2=new String(b);//再将字节数组中的内容转化成字符串形式输出
	          System.out.println(str2);
			 //finput.read(o, off, len)
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//这里是你要读取的图像文件
	}

}
