package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader; 
import java.sql.Connection;
import java.sql.DriverManager; 
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ParseGeneTypeFile {

	final static String aracne_stage_table = "aracne_stg";
	 

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			ParseGeneTypeFiles("C:\\database\\aracne_data",
					"pancancer-tfgenes_2014_04_17.txt",
					"pancancer-cotfgenes_2014_04_17.txt",
					"pancancer-signalinggenes_2014_04_17.txt");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void ParseGeneTypeFiles(String dataDir, String tfFile,
			String cotfFile, String signalingFile) throws Exception {

		System.out.println("Parse geneType files process start at: "
				+ (new Date()).toString());

		List<String> signalGenes = new ArrayList<String>();
		List<String> tfGenes = new ArrayList<String>();
		List<String> cotfGenes = new ArrayList<String>();

		File tfGeneTypeFile = new File(dataDir, tfFile);
		File cotfGeneTypeFile = new File(dataDir, cotfFile);
		File signalGeneTypeFile = new File(dataDir, signalingFile);

		BufferedReader bufferedReader = new BufferedReader(new FileReader(
				tfGeneTypeFile));
		String dataLine = bufferedReader.readLine();

		while (dataLine != null) {
			String[] tokens = dataLine.split("\t");
			tfGenes.add(tokens[0].trim());
			dataLine = bufferedReader.readLine();
		}
		bufferedReader.close();

		bufferedReader = new BufferedReader(new FileReader(cotfGeneTypeFile));
		dataLine = bufferedReader.readLine();

		while (dataLine != null) {
			String[] tokens = dataLine.split("\t");
			cotfGenes.add(tokens[0].trim());
			dataLine = bufferedReader.readLine();
		}
		bufferedReader.close();

		bufferedReader = new BufferedReader(new FileReader(signalGeneTypeFile));
		dataLine = bufferedReader.readLine();

		while (dataLine != null) {
			String[] tokens = dataLine.split("\t");
			signalGenes.add(tokens[0].trim());
			dataLine = bufferedReader.readLine();
		}
		bufferedReader.close();

		String mysql_jdbc_driver = "com.mysql.jdbc.Driver";
		String mysql_user = "user";
		String mysql_passwd = "password";
		String mysql_url = "jdbc:mysql://localhsot:3306/cellnet_kbase?autoReconnect=false";

		try {
			Class.forName(mysql_jdbc_driver);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
			System.exit(1);
		}

		Connection conn = null;
		if (conn == null || conn.isClosed())
			conn = DriverManager.getConnection(mysql_url, mysql_user,
					mysql_passwd);

		conn.setAutoCommit(false);

		Statement stm = conn.createStatement();

		try {

			
			for (int i = 0; i < tfGenes.size(); i++) {
				String aSql = "update physical_entity set gene_type_id = 1 where primary_accession = "
						+ tfGenes.get(i);
				stm.addBatch(aSql);
				if (i % 1000 == 0) {
					stm.executeBatch();
					stm.close();
					stm = conn.createStatement();
				}
			} 
			 
			 stm.executeBatch();
			 stm.close();		
			
			 for (int i = 0; i < cotfGenes.size(); i++) {
				String aSql = "update physical_entity set gene_type_id = 2 where primary_accession = "
						+ cotfGenes.get(i);
				stm.addBatch(aSql);
				if (i % 1000 == 0) {
					stm.executeBatch();
					stm.close();
					stm = conn.createStatement();
				}
			} 
			
			 stm.executeBatch();
			 stm.close();		
			

			for (int i = 0; i < signalGenes.size(); i++) {
				String aSql = "update physical_entity set gene_type_id = 3 where primary_accession = "
						+ signalGenes.get(i);
				stm.addBatch(aSql);
				if (i % 1000 == 0) {
					stm.executeBatch();
					stm.close();
					stm = conn.createStatement();
			    }
			}

			stm.executeBatch();			
			conn.commit();
		 

		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
		} finally {
			stm.close();
			conn.close();
		}
		System.out.println("Parse aracne file process end at: "
				+ (new Date()).toString());
	}

}
