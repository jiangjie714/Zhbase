package hbase.dao.imp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;


//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HConnection;
//import org.apache.hadoop.hbase.client.HConnectionManager;
//import org.apache.hadoop.hbase.client.HTableInterface;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.filter.PrefixFilter;



import hbase.dao.HBaseDAO;

public class HBaseDAOImp implements HBaseDAO {

	HConnection hTablePool = null;
	//HBaseAdmin admin;
	static Configuration conf = null;
	
//	 {
//        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.property.clientPort","2181"); 
//        conf.set("hbase.zookeeper.quorum", "hadoop-matser.com,hadoop-s2.com,hadoop-s1.com");
//        
//        System.out.println("_______________'");
//        try {
//			hTablePool = HConnectionManager.createConnection(conf) ;//����hbase���ӳ�
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//    }
	
	public HBaseDAOImp()
	{
		Configuration conf = new Configuration();//Hadoop-common �е������ļ� ����Ḳ�Ǽ�Ⱥ�ϵ������ļ�
		String zk_list = "hadoop-s1.com" ;//zookeeper�е�ip�б�
		conf.set("hbase.zookeeper.property.clientPort", "2181"); 
		conf.set("hbase.zookeeper.quorum", zk_list);
		//conf.set("hbase.master", "hadoop-s1.com:600000"); 
		try {
			hTablePool = HConnectionManager.createConnection(conf) ;
			//����hbase���ӳ�
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public String  insert(String tableName, String rowKey, String family,String quailifer, String value) {
		// TODO Auto-generated method stub
		String flag ="0";
		
		HTableInterface table = null;//�����
		try {
			table = hTablePool.getTable(tableName) ;//���ɱ�
			Put put = new Put(rowKey.getBytes());//����put ���������rowKey ��put��ȷ����һʵ���� rowKey��Ψһ��
			put.add(family.getBytes(), quailifer.getBytes(), value.getBytes()) ;//������� ���� �����ֵ
			table.put(put);//�������� 
			flag="1";
		} catch (Exception e) {
			e.printStackTrace();
			flag="E1";
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
				flag="E2";
			}
		}
		return flag;
	}


	public Result getOneRow(String tableName, String rowKey) {
		HTableInterface table = null;
		
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Get get = new Get(rowKey.getBytes()) ;
			rsResult = table.get(get) ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}
	 /*
     * ����rwokey��ѯ
     * 
     * @rowKey rowKey
     * 
     * @tableName ����
     */
//    public static Result getResult(String tableName, String rowKey)
//            throws IOException {
//        Get get = new Get(rowKey.getBytes());
//        
//        HTable table = new HTable(conf, tableName.getBytes());// ��ȡ��
//        Result result = table.get(get);
//        for (KeyValue kv : result.list()) {
//            System.out.println("family:" + Bytes.toString(kv.getFamily()));
//            System.out
//                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
//            System.out.println("value:" + Bytes.toString(kv.getValue()));
//            System.out.println("Timestamp:" + kv.getTimestamp());
//            System.out.println("-------------------------------------------");
//        }
//        return result;
//    }

	public List<Result> getRows(String tableName, String rowKeyLike) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
		
	}


	public void save(Put put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			table.put(put) ;
			System.out.println("��������");
			
		} catch (Exception e) {
			e.printStackTrace() ;
		}finally{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

	public void save(List<Put> Put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			table.put(Put) ;
			//table.
			table.flushCommits();
			System.out.println("����ɹ� ��");
		}
		catch (Exception e) {
			// TODO: handle exception
		}finally
		{
			try {
				table.close() ;
				System.out.println("����ɹ� ��");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public  List<Result> TestgetRows(String tableName, String rowKeyLike){
		
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}


	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		HBaseDAOImp dao = new HBaseDAOImp();
		
		
//		String a=dao.insert("test", "rk3", "cf", "age", "35") ;
//		String b=dao.insert("test", "rk3", "cf", "cardid", "12312312335") ;
//		String c=dao.insert("test", "rk3", "cf", "tel", "13512312345") ;
//		System.out.println("a:"+a+";" +"b:"+b+";" +"c:"+c+";" );
		
/**
 * 		����   ������ rowkey ���� ���ж���
 */
		Result rs = dao.getOneRow("test", "rk6");
		System.out.println("rs.size()=="+rs.size());
		
		if(rs.size()>0){
		
			for(KeyValue keyValue : rs.raw())
			{
				System.out.println("rowkey:"+ new String(keyValue.getRow()));
				System.out.println("Qualifier:"+ new String(keyValue.getQualifierArray()));//.getQualifier()));
				System.out.println("Value:"+ new String(keyValue.getValue()));
				System.out.println("----------------");
				
				
				
			}
		}else{
			System.out.println("δ�鵽���ݣ�");
		}
		
		/**
		 *  ���� ���� ��rowkey ģ����ѯ
		 */
		List<Result> list = dao.getRows("test", "rk");
//		for(Result rs : list)
//		{
//			for(KeyValue keyValue : rs.raw())
//			{
//				System.out.println("rowkey:"+ new String(keyValue.getRow()));
//				System.out.println("Qualifier:"+ new String(keyValue.getQualifier()));
//				System.out.println("Value:"+ new String(keyValue.getValue()));
//				System.out.println("----------------");
//			}
//		}
		/**
		 *   ��������
		 *   
		 */
//		List<Put> list = new ArrayList<Put>();
//		Put put = new Put("rk4".getBytes());
//		put.add("cf".getBytes(), "name".getBytes(), "zhaoliu123".getBytes()) ;
//		list.add(put) ;
//		put.add("cf".getBytes(), "addr".getBytes(), "shanghai".getBytes()) ;
//		list.add(put) ;
//		put.add("cf".getBytes(), "age".getBytes(), "30".getBytes()) ;
//		list.add(put) ;
//		put.add("cf".getBytes(), "tel".getBytes(), "13567882141".getBytes()) ;
//		list.add(put) ;
//		dao.save(list, "test");
		//dao.save(put, "test");
		
	}

	

}
