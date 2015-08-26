package org.geworkbench.components.interactions.cellularnetwork;

import org.apache.log4j.Logger;

//import java.beans.PropertyVetoException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.sql.SQLException;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//import java.util.Date;

/**
 * The tutorial from
 * http://docs.mongodb.org/ecosystem/tutorial/getting-started-with-java-driver/
 */
public class InteractomeMysqlExportToFile {

	/**
	 * Logger for this class
	 */
	static final Logger logger = Logger
			.getLogger(InteractomeMysqlExportToFile.class);

	private static final String PROPERTIES_FILE = "interactionsweb.properties";
	private static final String MYSQL_JDBC_DRIVER = "mysql.jdbc.Driver";
	private static final String MYSQL_USER = "mysql.user";
	private static final String MYSQL_PASSWD = "mysql.passwd";
	private static final String MYSQL_URL = "mysql.url";

	private static String mysql_jdbc_driver;
	private static String mysql_user;
	private static String mysql_passwd;
	private static String mysql_url;

	private static final String EntrezIDOnly = "Entrez ID Only";
	private static final String GeneSymbolOnly = "Gene Symbol Only";

	private static final String EntrezIDPreferred = "Entrez ID Preferred";
	private static final String GeneSymbolPreferred = "Gene Symbol Preferred";

	private static final String GET_INTERACTIONS_ADJ_FORMAT = "getInteractionsAdjFormat";
	private static final String GET_INTERACTIONS_SIF_FORMAT = "getInteractionsSifFormat";

	private static final String[] presentByList = { EntrezIDOnly,
			GeneSymbolOnly, EntrezIDPreferred, GeneSymbolPreferred };

	public static void main(final String[] args) {

		if (args.length != 2) {
			System.out
					.println("Please enter correct interactome and version as arguments.");
			return;
		}
		String interactome = args[0];
		String version = args[1];
		InteractomeMysqlExportToFile exportToFile = new InteractomeMysqlExportToFile();
		exportToFile.generateFiles(interactome, version);

	}

	void init() throws SQLException, IOException {

		Properties iteractionsWebProp = new Properties();
		InputStream instream = getClass().getResourceAsStream(
				"/" + PROPERTIES_FILE);
		if (instream != null) {
			iteractionsWebProp.load(instream);
			instream.close();
		}

		mysql_jdbc_driver = iteractionsWebProp.getProperty(MYSQL_JDBC_DRIVER)
				.trim();
		mysql_user = iteractionsWebProp.getProperty(MYSQL_USER).trim();
		mysql_passwd = iteractionsWebProp.getProperty(MYSQL_PASSWD).trim();
		mysql_url = iteractionsWebProp.getProperty(MYSQL_URL).trim();

		try {
			Class.forName(mysql_jdbc_driver);
		} catch (ClassNotFoundException e) {
			logger.error(
					"init() - exception  in InteractionsConnectionImplServlet init  ---- ", e); //$NON-NLS-1$
			logger.error("init()", e); //$NON-NLS-1$
			System.exit(1);
		}

	}

	private void generateFiles(String interactome, String version) {
		Connection conn = null; 
		try {
			System.out
			.println("Start exporting: " + interactome);
			init();
			conn = getConnection();
			Integer versionId = getInteractomeVersionId(interactome, version,
					conn);
			if (versionId == null) {
				System.out
						.println("Please enter correct interactome and version as arguments.");
				return;
			}

			
			List<String> interactionTypes = getInteractionTypesByInteractomeVersion(
					versionId, conn);
			InteractionsExport export = new InteractionsExport();
			boolean append_file_flag = false;
			Writer writer = null;
			for (int i = 0; i < presentByList.length; i++) {
				for (int j = 0; j < interactionTypes.size(); j++) {
					String shortName = getInteractionTypeShortName(
							interactionTypes.get(j), conn);
					writer = getFileWriter(interactome, version, shortName,
							presentByList[i], GET_INTERACTIONS_ADJ_FORMAT);
					export.getInteractionsAdjFormat(versionId,
							interactionTypes.get(j), presentByList[i], writer,
							conn);
					writer = getFileWriter(interactome, version, shortName,
							presentByList[i], GET_INTERACTIONS_SIF_FORMAT);
					export.getInteractionsSifFormat(versionId,
							interactionTypes.get(j), shortName,
							presentByList[i], writer, conn);
					if (j == 0)
						append_file_flag = false;
					else
						append_file_flag = true;

					writeToAllFile(interactome, version, shortName,
							presentByList[i], GET_INTERACTIONS_ADJ_FORMAT,
							append_file_flag);
					writeToAllFile(interactome, version, shortName,
							presentByList[i], GET_INTERACTIONS_SIF_FORMAT,
							append_file_flag);
					
				
				}

			}
			
			System.out
			.println("End exporting: " + interactome);
			
		} catch (SQLException sqle) {

			logger.error("get SQLException: ", sqle);

		} catch (IOException ioe) {
			logger.error("get IOException:", ioe);

		} catch (Exception e) {
			logger.error("get Exception:", e);
		} finally {
			try {
				if (conn != null)
				  conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private Connection getConnection() throws SQLException {
		Connection conn = null;
		if (conn == null || conn.isClosed())
			conn = DriverManager.getConnection(mysql_url, mysql_user,
					mysql_passwd);
		return conn;
	}

	private Integer getInteractomeVersionId(String context, String version,
			Connection connection) throws SQLException {

		Integer id = null;
		String aSql = null;
		aSql = "SELECT v.id ";
		aSql += "FROM interactome_version v, interactome i ";
		aSql += "WHERE v.interactome_id=i.id ";
		aSql += "AND i.name='" + context + "' ";
		aSql += "AND v.version='" + version + "'";

		PreparedStatement statement = null;
		statement = connection.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery(aSql);

		while (rs.next()) {

			// Get the data from the row using the column name
			id = rs.getInt("id");
			break;
		}
		rs.close();
		statement.close();
		return id;
	}

	private List<String> getInteractionTypesByInteractomeVersion(int versionId,
			Connection conn) throws SQLException {
		String aSql = null;
		aSql = "SELECT interaction_type FROM interactome_2_interaction_type ";
		aSql += "WHERE interactome_version_id =" + versionId;
		PreparedStatement statement = null;
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		List<String> interactionTypes = new ArrayList<String>();
		while (rs.next()) {
			interactionTypes.add(rs.getString("interaction_type"));
		}
		rs.close();
		statement.close();
		return interactionTypes;
	}

	private String getInteractionTypeShortName(String interactionType,
			Connection conn) throws SQLException {
		String shortName = null;
		String aSql = null;
		aSql = "SELECT short_name FROM interaction_type where name = '"
				+ interactionType + "'";
		;
		PreparedStatement statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		while (rs.next()) {

			// Get the data from the row using the column name
			shortName = rs.getString("short_name");
			break;
		}
		rs.close();
		statement.close();
		return shortName;
	}

	private FileWriter getFileWriter(String context, String version,
			String shortName, String presentBy, String methodName)
			throws IOException {
		String fileName = getExportFileName(context, version, shortName,
				presentBy, methodName);
		File file = new File(fileName);
		return new FileWriter(file);
	}

	private void writeToAllFile(String interactome, String version,
			String shortName, String presentBy, String format,
			boolean append_file_flag) throws IOException {

		String fileName = getExportFileName(interactome, version, shortName,
				presentBy, format);
		File inFile = new File(fileName);
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			InputStream inStream = new FileInputStream(inFile);

			br = new BufferedReader(new InputStreamReader(inStream));

			fileName = getExportFileName(interactome, version, null, presentBy,
					format);
			File outFile = new File(fileName);
			bw = new BufferedWriter(new FileWriter(outFile, append_file_flag));

			String line = br.readLine();
			while (line != null) {
				bw.write(line + "\n");
				line = br.readLine();
			}

		} catch (IOException ie) {
			throw ie;
		} finally {
			br.close();
			bw.close();
		}

	}

	private String getExportFileName(String context, String version,
			String shortName, String presentBy, String methodName) {
		String name = null;
		StringBuffer sb = new StringBuffer(InteractionsExport.datafileDir
				+ "export_" + context + "_" + version);
		if (shortName != null && !shortName.trim().equals(""))
			sb.append("_" + shortName);
		else
			sb.append("_all");
		if (presentBy.equalsIgnoreCase(GeneSymbolOnly))
			sb.append("_GSO");
		else if (presentBy.equalsIgnoreCase(EntrezIDOnly))
			sb.append("_EIO");
		else if (presentBy.equalsIgnoreCase(EntrezIDPreferred))
			sb.append("_EIP");
		else
			sb.append("_GSP");
		if (methodName.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT))
			sb.append(".sif");
		else
			sb.append(".adj");

		name = sb.toString();

		return name;

	}

}