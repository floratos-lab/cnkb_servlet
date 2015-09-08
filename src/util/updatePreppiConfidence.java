package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Date;

public class updatePreppiConfidence {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			updateConfidence();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

   //please update proper value for user/password/database url
	public static void updateConfidence() throws Exception {

		System.out.println("update Preppi Confidence start at: "
				+ (new Date()).toString());

		String mysql_jdbc_driver = "com.mysql.jdbc.Driver";
		String mysql_user = "user";
		String mysql_passwd = "password";
		String mysql_url = "jdbc:mysql://localhost:3306/cellnet_kbase?";

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
 
		Statement stm = conn.createStatement();
				 

		try {

			String aSql = "UPDATE interactions_joined_part_new SET confidence_value = ROUND((confidence_value / (confidence_value + 600))*100)/100, confidence_type=4" 
                                + " WHERE interactome_version_id = 18 AND confidence_type = 1 LIMIT 1000000";
 
			while (true) {
				stm.executeUpdate(aSql);
				System.out.println("executeUpdate() at: "
						+ (new Date()).toString());
				 
			 
				
			  
				System.out.println("executeBatch() at: "
						+ (new Date()).toString());

			}

		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
		} finally {
			if (stm != null)
				stm.close();
			if (conn != null)
				conn.close();
		}
		System.out.println("updatePreppiConfidence end at: "
				+ (new Date()).toString());
	}

}
