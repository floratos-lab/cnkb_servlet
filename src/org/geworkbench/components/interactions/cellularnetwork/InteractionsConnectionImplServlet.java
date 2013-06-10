package org.geworkbench.components.interactions.cellularnetwork;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource; 
import java.beans.PropertyVetoException;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;  
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement; 
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
 
import javax.xml.bind.DatatypeConverter;
/**
 * @author oleg stheynbuk
 * 
 *         tmp. Servlet that will allow clients execute SQL query over HTTP from
 *         outside the firewall
 * 
 * @param db
 *            and query string.
 * 
 * @return query executed, result set processed and send as a text; for more
 *         info
 * @see ResultSetlUtil
 * 
 */
public class InteractionsConnectionImplServlet extends HttpServlet {

	private static final long serialVersionUID = -5054985364013020840L;
	private static final int MIN_DATA_LENGTH = 1;
	private static final String GET_PAIRWISE_INTERACTION = "getPairWiseInteraction";
	private static final String GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL = "getInteractionsByEntrezIdOrGeneSymbol";
	private static final String GET_INTERACTION_BY_ENTREZID = "getInteractionsByEntrezId";
	private static final String GET_INTERACTION_BY_GENESYMBOL = "getInteractionsByGeneSymbol";
	private static final String GET_INTERACTION_TYPES = "getInteractionTypes";
	private static final String GET_INTERACTION_EVIDENCES = "getInteractionEvidences";
	private static final String GET_DATASET_NAMES = "getDatasetNames";
	private static final String GET_INTERACTOME_NAMES = "getInteractomeNames";
	private static final String GET_INTERACTOME_DESCRIPTION = "getInteractomeDescription";
	private static final String GET_VERSION_DESCRIPTOR = "getVersionDescriptor";
	private static final String GET_INTERACTION_TYPES_BY_INTERACTOMEVERSION = "getInteractionTypesByInteractomeVersion";
	private static final String GET_INTERACTIONS_SIF_FORMAT = "getInteractionsSifFormat";
	private static final String GET_INTERACTIONS_ADJ_FORMAT = "getInteractionsAdjFormat";
	private static final String GET_CONFIDENCE_TYPES = "getConfidenceTypes";
	private static final String CLOSE_DB_CONNECTION = "closeDbConnection";

	private static final String SECONDARY_ACCESSION = "secondary_accession";

	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(InteractionsConnectionImplServlet.class);

	// currently in two files
	static final int SPLIT_ALL = -2;
	static final String DEL = "|";
	private static final String REGEX_DEL = "\\|";
	static final String ORACLE = "oracle";
	static final String MYSQL = "mysql";
	static final String PROPERTIES_FILE = "interactionsweb.properties";
	private static final String MYSQL_JDBC_DRIVER = "mysql.jdbc.Driver";
	private static final String MYSQL_USER = "mysql.user";
	private static final String MYSQL_PASSWD = "mysql.passwd";
	private static final String MYSQL_URL = "mysql.url";
	private static final String DATASET_USER = "dataset.user";
	private static final String DATASET_PASSWD = "dataset.passwd";
	 
	private static String mysql_jdbc_driver;
	private static String mysql_user;
	private static String mysql_passwd;
	private static String mysql_url;
	private static String dataset_passwd;
	private static String dataset_user;
	private ComboPooledDataSource dataSource;

	@Override
	public void init() throws ServletException {
		super.init();

		try {

			Properties iteractionsWebProp = new Properties();
			InputStream instream = getClass().getResourceAsStream(
					"/" + PROPERTIES_FILE);
			if (instream != null) {
				iteractionsWebProp.load(instream);
				instream.close();
			}

			mysql_jdbc_driver = iteractionsWebProp.getProperty(
					MYSQL_JDBC_DRIVER).trim();
			mysql_user = iteractionsWebProp.getProperty(MYSQL_USER).trim();
			mysql_passwd = iteractionsWebProp.getProperty(MYSQL_PASSWD).trim();
			mysql_url = iteractionsWebProp.getProperty(MYSQL_URL).trim();
			dataset_user = iteractionsWebProp.getProperty(DATASET_USER).trim();
			dataset_passwd = iteractionsWebProp.getProperty(DATASET_PASSWD)
					.trim();
			
			try{
		     	Class.forName(mysql_jdbc_driver);
			} catch (ClassNotFoundException e) {
				logger
						.error(
								"init() - exception  in InteractionsConnectionImplServlet init  ---- ", e); //$NON-NLS-1$
				logger.error("init()", e); //$NON-NLS-1$
			} 

			/*dataSource = new ComboPooledDataSource();
			try {
				dataSource.setDriverClass(mysql_jdbc_driver);
			} catch (PropertyVetoException pve) {
				logger.error(pve.getMessage());
			}
			dataSource.setJdbcUrl(mysql_url);
			dataSource.setUser(mysql_user);
			dataSource.setPassword(mysql_passwd);
			dataSource.setAcquireRetryAttempts(10);
			dataSource.setAcquireIncrement(3);
			dataSource.setIdleConnectionTestPeriod(100);
			dataSource.setMaxPoolSize(3);		 
			dataSource.setMaxStatementsPerConnection(100);*/
		 
			
		} catch (IOException ie) {
			logger.error(
					"init() - exception  in InteractionsConnectionImplServlet init  ---- ", ie); //$NON-NLS-1$
			logger.error("init()", ie); //$NON-NLS-1$
		}

	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		doPost(req, resp);
	}

	public void doPost(HttpServletRequest req, HttpServletResponse resp) {

		String methodName = "";

		PreparedStatement statement = null;

		Connection conn = null;
		 
		try {

			PrintWriter out = resp.getWriter();

			if (logger.isDebugEnabled()) {
				logger.debug("doPost(HttpServletRequest, HttpServletResponse) - InteractionsConnectionImplServlet doPost, you got here  ---"); //$NON-NLS-1$
			}
						 
			/*java.lang.management.MemoryMXBean memoryBean = java.lang.management.ManagementFactory.getMemoryMXBean();
		    long l = memoryBean.getHeapMemoryUsage().getMax();
		    logger.info(l);
		    logger.info("test: " + Runtime.getRuntime().maxMemory()); */
		    
			int len = req.getContentLength();
    
			// if there are no input data - return
			if (len < MIN_DATA_LENGTH) {
				if (logger.isInfoEnabled()) {
					logger.info("doPost(HttpServletRequest, HttpServletResponse) - no input data provided"); //$NON-NLS-1$
				}

				return;
			}

			byte[] input = new byte[len];

			ServletInputStream sin = req.getInputStream();
			int c, count = 0;
			while ((c = sin.read(input, count, input.length - count)) != -1) {
				count += c;
			}
			sin.close();

			String inString = new String(input);

			String[] tokens = inString.split(REGEX_DEL, SPLIT_ALL);

			methodName = tokens[0].trim();

			if (methodName.equalsIgnoreCase(CLOSE_DB_CONNECTION))
				return;

			//if (conn == null) 
			  // conn = dataSource.getConnection();
			 
			if (conn == null || conn.isClosed())
				conn = DriverManager.getConnection(mysql_url, mysql_user,
						mysql_passwd);
                
			
			
			if (needAuthentication(tokens, req, conn))
				askForPassword(resp);

			else {
				ResultSet rs = null;
				String geneId = null;
				if (methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION)) {
					geneId = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					rs = this.getPairWiseInteraction(geneId, context, version,
							conn, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)) {
					geneId = tokens[1].trim();
					String geneSymbol = tokens[2].trim();
					String context = tokens[3].trim();
					String version = tokens[4].trim();
					rs = this.getInteractionsByEntrezIdOrGeneSymbol(geneId,
							geneSymbol, context, version, conn, statement);

				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID)) {
					geneId = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					rs = this.getInteractionsByEntrezId(geneId, context,
							version, conn, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL)) {
					String geneSymbol = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					rs = this.getInteractionsByGeneSymbol(geneSymbol, context,
							version, conn, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT)
						|| methodName
								.equalsIgnoreCase(GET_INTERACTIONS_ADJ_FORMAT)) {
					String interactionTypes = "ALL";
					String nodePresentedBy = "GENE_NAME";
					String context = tokens[1].trim();
					String version = tokens[2].trim();
					if (tokens.length == 5) {
						interactionTypes = tokens[3].trim();
						nodePresentedBy = tokens[4].trim();
					}

					int versionId = getInteractomeVersionId(context, version,
							conn, statement);
					InteractionsExport export = new InteractionsExport();
					if (methodName
							.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT))
						export.getInteractionsSifFormat(versionId,
								interactionTypes, nodePresentedBy, out, conn);
					else
						export.getInteractionsAdjFormat(versionId,
								interactionTypes, nodePresentedBy, out, conn);
				} else if (methodName.equalsIgnoreCase(GET_INTERACTION_TYPES)) {
					rs = this.getInteractionTypes(conn, statement);
				} else if (methodName.equalsIgnoreCase(GET_CONFIDENCE_TYPES)) {
					rs = this.getConfidenceTypes(conn, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_EVIDENCES)) {
					rs = this.getInteractionEvidences(conn, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_TYPES_BY_INTERACTOMEVERSION)) {
					String context = tokens[1].trim();
					String version = tokens[2].trim();
					rs = this.getInteractionTypesByInteractomeVersion(context,
							version, conn, statement);
				} else if (methodName.equalsIgnoreCase(GET_DATASET_NAMES)) {
					rs = this.getDatasetNames(conn, statement);
				} else if (methodName.equalsIgnoreCase(GET_INTERACTOME_NAMES)) {
					rs = this.getInteractomeNames(conn, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTOME_DESCRIPTION)) {
					String context = tokens[1].trim();
					rs = this.getInteractomeDescription(context, conn,
							statement);
				} else if (methodName.equalsIgnoreCase(GET_VERSION_DESCRIPTOR)) {
					String context = tokens[1].trim();
					rs = this.getVersionDescriptor(context, conn, statement);
				}

				if (!methodName.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT)
						&& !methodName
								.equalsIgnoreCase(GET_INTERACTIONS_ADJ_FORMAT)) {
					String metaData = new String();

					String gene1Data = "";
					ResultSetMetaData rsmd;
					rsmd = rs.getMetaData();
					int numberOfColumns = rsmd.getColumnCount();

					// get metadata
					if (methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION)) {
						metaData = "ms_id1" + DEL + "db1_xref" + DEL + "gene1"
								+ DEL;
						gene1Data = getGeneDataByEntrezId(geneId, conn,
								statement);
					}

					for (int i = 1; i <= numberOfColumns; i++) {
						if (rsmd.getColumnName(i).trim()
								.equalsIgnoreCase(SECONDARY_ACCESSION)) {

							continue;
						}
						if (i > 1) {
							metaData += DEL;
						}
						metaData += rsmd.getColumnName(i);
					}

					if (methodName
							.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)
							|| methodName
									.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID)
							|| methodName
									.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL)) {

						metaData += DEL + "other_confidence_values" + DEL
								+ "other_confidence_types";

					}

					out.println(metaData);

					// get values

					if (methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION)) {

						while (rs.next()) {
							if (rs.getString("ms_id2").equals(geneId))
								continue;
							String row = gene1Data;
							for (int i = 1; i <= numberOfColumns; i++) {
								row += DEL + rs.getString(i);
							}
							out.println(row);
						}

					} else if (methodName
							.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)
							|| methodName
									.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID)
							|| methodName
									.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL)) {

						int previousInteractionId = 0;

						List<String> rowList = new ArrayList<String>();
						int previousConfidenceType = 0;
						String other_scores = "", other_types = "";
						while (rs.next()) {
							String row = new String();
							int interactionId = rs.getInt("interaction_id");
							int currentConfidenceType = rs
									.getInt("confidence_type");
							if (previousInteractionId == 0
									|| previousInteractionId != interactionId) {
								if (rowList.size() > 0) {
									if (other_scores.trim().equals("")) {
										other_scores = "null";
										other_types = "null";
									}
									for (int i = 0; i < rowList.size(); i++) {
										String r = rowList.get(i);
										r += DEL + other_scores + DEL
												+ other_types;
										out.println(r.trim());
									}
								}

								previousInteractionId = interactionId;
								previousConfidenceType = currentConfidenceType;
								rowList.clear();
								other_scores = "";
								other_types = "";

							}

							if (previousConfidenceType == currentConfidenceType) {
								if (rs.getString(1) == null
										|| rs.getString(1).trim()
												.equals("null")
										|| rs.getString(1).trim().equals("")) {
									for (int i = 2; i <= numberOfColumns; i++) {
										if (i > 2) {
											row += DEL;
										}
										if (rs.getString(i) != null)
										   row += rs.getString(i).trim();
										else
										   row += rs.getString(i);
										
									}

								} else {
									for (int i = 1; i <= numberOfColumns; i++) {
										if (i == 2)
											continue;
										if (i > 1) {
											row += DEL;
										}
										if (rs.getString(i) != null)
										  row += rs.getString(i).trim();
										else
										  row += rs.getString(i);
									}
								}

								rowList.add(row);

							} else {
								other_scores += rs.getFloat("confidence_value")
										+ ";";
								other_types += rs.getFloat("confidence_type")
										+ ";";
							}

						}

						if (rowList.size() > 0) {
							if (other_scores.trim().equals("")) {
								other_scores = "null";
								other_types = "null";
							}
							for (int i = 0; i < rowList.size(); i++) {
								String r = rowList.get(i);
								r += DEL + other_scores + DEL + other_types;
								out.println(r.trim());
							}

							rowList.clear();
						}

					} else {
						while (rs.next()) {
							String row = new String();

							for (int i = 1; i <= numberOfColumns; i++) {
								if (i > 1) {
									row += DEL;
								}
								row += rs.getString(i);
							}

							out.println(row.trim());
						}

					}
					rs.close();

				}

				out.flush();
				out.close();

				resp.setStatus(HttpServletResponse.SC_OK);

			}

		} catch (IOException e) {
			try {
				resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				resp.getWriter().print(e.getMessage());
				resp.getWriter().close();
				logger.error("get IOException: ", e);

			} catch (IOException ioe) {
				logger.error(
						"exception in catch block: doPost(HttpServletRequest, HttpServletResponse)", ioe); //$NON-NLS-1$
			}
		} catch (SQLException e) {
			try {
				resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				resp.getWriter().print(e.getMessage());
				resp.getWriter().close();
				logger.error("get SQLException: ", e);

			} catch (IOException ioe) {
				logger.error(
						"exception in catch block: doPost(HttpServletRequest, HttpServletResponse)", ioe); //$NON-NLS-1$

			}
		} catch (Throwable e) {
			try {
				resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				resp.getWriter().print(e.getMessage());
				resp.getWriter().close();
				logger.error("get exceptions: ", e);
			} catch (IOException ioe) {
				logger.error(
						"exception in catch block: doPost(HttpServletRequest, HttpServletResponse)", ioe); //$NON-NLS-1$
			}

		} finally {

			try {
				if (statement != null)
					statement.close();

				if (conn != null)
					conn.close();
				conn = null;   
				
			} catch (SQLException e) {
			}

		}
	}

	private void askForPassword(HttpServletResponse resp) {
		resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED); // Ie 401
		resp.setHeader("WWW-Authenticate",
				"BASIC realm=\"Requires Authentication \"");
		 
	}

	private boolean needAuthentication(String[] tokens, HttpServletRequest req,
			Connection conn) throws IOException, SQLException {
		String methodName = tokens[0];

		if (!methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION)
				&& !methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)
				&& !methodName.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID)
				&& !methodName.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL)
				&& !methodName.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT)
				&& !methodName.equalsIgnoreCase(GET_INTERACTIONS_ADJ_FORMAT))
			return false;
		else {
			String context = null;
			String version = null;
			if (methodName
					.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)) {
				context = tokens[3];
				version = tokens[4];

			} else if (methodName.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT)
					|| methodName.equalsIgnoreCase(GET_INTERACTIONS_ADJ_FORMAT)) {
				context = tokens[1];
				version = tokens[2];

			} else {
				context = tokens[2];
				version = tokens[3];
			}
			if (getAuthentication(context, version, conn).equalsIgnoreCase("N"))
				return false;			
			String userAndPasswd = null;			 
			String userInfo = req.getHeader("Authorization");			
			if (userInfo != null) {
				userInfo = userInfo.substring(6).trim();			 
				userAndPasswd = new String(DatatypeConverter.parseBase64Binary(userInfo));
				if (userAndPasswd.equals(dataset_user + ":" + dataset_passwd))
					return false;
			}

		}

		return true;
	}

	/*
	 * private ResultSet getPairWiseInteraction_old(String geneId, String
	 * context, String version) throws SQLException { String aSql = null; int
	 * datasetId = getDatasetId(conn, context, version); aSql = "SELECT
	 * pi.ms_id1, pi.ms_id2, pi.gene1, pi.gene2, pi.confidence_value,
	 * pi.db1_xref, pi.db2_xref, it.interaction_type FROM pairwise_interaction
	 * pi, interaction_dataset ids, interaction_type it"; aSql += " WHERE
	 * (ms_id1= ?" + " OR ms_id2= ?"; aSql += ") AND pi.interaction_type=it.id
	 * AND pi.id=ids.interaction_id And ids.dataset_id=" + datasetId; statement
	 * = conn.prepareStatement(aSql); statement.setString(1, geneId);
	 * statement.setString(2, geneId); ResultSet rs = statement.executeQuery();
	 * 
	 * return rs; }
	 */

	private ResultSet getPairWiseInteraction(String geneId, String context,
			String version, Connection conn, PreparedStatement statement)
			throws SQLException {
		String aSql = null;

		String interactionIdList = getInteractionIdsByEntrezId(geneId, context,
				version, conn, statement);

		if (interactionIdList.equals(""))
			interactionIdList = "0";
		aSql = "SELECT pe.primary_accession as ms_id2, ds.name as db2_xref, pe.gene_symbol as gene2, it.name as interaction_type, ie.evidence_id as evidence_id, ic.score as confidence_value, ct.name as confidence_type ";
		aSql += "FROM (physical_entity pe, interaction_participant ip, interaction i, interaction_type it, db_source ds) ";
		aSql += "left join  interaction_evidence ie on (i.id=ie.interaction_id) ";
		aSql += "left join interaction_confidence ic on (i.id=ic.interaction_id) ";
		aSql += "left join confidence_type ct on (ic.confidence_type_id=ct.id) ";
		aSql += "WHERE pe.id=ip.participant_id ";
		aSql += "AND pe.accession_db=ds.id ";
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.interaction_type=it.id ";
		aSql += "AND i.id in (" + interactionIdList + ")";

		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();

		return rs;
	}

	private ResultSet getConfidenceTypes(Connection conn,
			PreparedStatement statement) throws SQLException {
		String aSql = null;
		aSql = "SELECT id as id, name as name FROM confidence_type";
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

	private ResultSet getInteractionTypes(Connection conn,
			PreparedStatement statement) throws SQLException {
		String aSql = null;
		aSql = "SELECT name as interaction_type, short_name as short_name FROM  interaction_type";
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

	private ResultSet getInteractionEvidences(Connection conn,
			PreparedStatement statement) throws SQLException {
		String aSql = null;
		aSql = "SELECT id as id, description as description FROM  evidence_type";
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

 
	
	private ResultSet getDatasetNames(Connection conn,
			PreparedStatement statement) throws SQLException {
		String aSql = null;

		aSql = "SELECT ic.name as name, ic.interaction_count as interaction_count ";
		aSql += "FROM interaction_count ic ";	 
		aSql += "ORDER BY ic.name";
		
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;

	}
	
	
	 

	/*
	 * private ResultSet getVersionDescriptor(String context) throws
	 * SQLException { String aSql = null; aSql = "SELECT DISTINCT version,
	 * authentication_yn FROM interaction_dataset, dataset where dataset.name='"
	 * + context + "' AND interaction_dataset.dataset_id = dataset.id;";
	 * statement = conn.prepareStatement(aSql); ResultSet rs =
	 * statement.executeQuery(); return rs; }
	 */

	private ResultSet getVersionDescriptor(String context, Connection conn,
			PreparedStatement statement) throws SQLException {
		String aSql = null;

		aSql = "SELECT DISTINCT v.version, v.authentication_yn as authentication_yn, v.description as description ";
		aSql += "FROM interactome_version v, interactome i, interaction_interactome_version iv ";
		aSql += "WHERE v.interactome_id=i.id ";
		aSql += " AND v.id=iv.interactome_version_id ";
		aSql += " AND i.name='" + context + "'";

		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

	private ResultSet getInteractomeDescription(String interactomeName,
			Connection conn, PreparedStatement statement) throws SQLException {
		String aSql = null;

		aSql = "SELECT i.description as description ";
		aSql += "FROM interactome i ";
		aSql += "WHERE i.name=?";

		statement = conn.prepareStatement(aSql);
		statement.setString(1, interactomeName);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

	/*
	 * private String getAuthentication(String context, String version) throws
	 * SQLException { Statement statement = conn.createStatement(); String value
	 * = "N"; String sql = "SELECT * FROM dataset where name='" + context + "'";
	 * if (version != null && !version.equals("")) sql = sql + " AND version='"
	 * + version + "'"; ResultSet rs = statement.executeQuery(sql); while
	 * (rs.next()) { // Get the data from the row using the column name value =
	 * rs.getString("authentication_yn"); break; } statement.close(); return
	 * value; }
	 */

	private String getAuthentication(String context, String version,
			Connection conn) throws SQLException {
		Statement statement = conn.createStatement();
		String value = "N";
		String aSql = null;
		aSql = "SELECT v.authentication_yn ";
		aSql += "FROM interactome_version v, interactome i ";
		aSql += "WHERE v.interactome_id=i.id ";
		aSql += "AND i.name='" + context + "'";

		if (version != null && !version.equals(""))
			aSql = aSql + " AND v.version='" + version + "'";
		ResultSet rs = statement.executeQuery(aSql);
		while (rs.next()) {

			// Get the data from the row using the column name
			value = rs.getString("authentication_yn");
			break;
		}
		statement.close();
		return value;
	}

	/*
	 * private int getDatasetId(Connection conn, String context, String version)
	 * throws SQLException { Statement statement = conn.createStatement(); int
	 * id = 0; String sql = "SELECT * FROM dataset where name='" + context +
	 * "'"; if (version != null && !version.equals("")) sql = sql + " AND
	 * version='" + version + "'"; ResultSet rs = statement.executeQuery(sql);
	 * while (rs.next()) { // Get the data from the row using the column name id
	 * = rs.getInt("id"); break; } statement.close(); return id; }
	 */

	private int getInteractomeVersionId(String context, String version,
			Connection connection, PreparedStatement statement)
			throws SQLException {

		int id = 0;
		String aSql = null;
		aSql = "SELECT v.id ";
		aSql += "FROM interactome_version v, interactome i ";
		aSql += "WHERE v.interactome_id=i.id ";
		aSql += "AND i.name='" + context + "' ";
		aSql += "AND v.version='" + version + "'";

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

	private String getGeneDataByEntrezId(String geneId, Connection conn,
			PreparedStatement statement) throws SQLException {

		String aSql = null;
		String str = "";

		aSql = "SELECT pe.primary_accession, ds.name, pe.gene_symbol FROM physical_entity pe, db_source ds ";
		aSql += "WHERE pe.primary_accession =? ";
		aSql += "AND pe.accession_db=ds.id";

		statement = conn.prepareStatement(aSql);
		statement.setInt(1, new Integer(geneId));
		ResultSet rs = statement.executeQuery();

		while (rs.next()) {

			str = rs.getString("primary_accession") + DEL;
			str += rs.getString("name") + DEL;
			str += rs.getString("gene_symbol");
			break;

		}

		statement.close();

		return str;
	}

	private String getInteractionIdsByEntrezId(String geneId, String context,
			String version, Connection conn, PreparedStatement statement)
			throws SQLException {

		String aSql = null;
		String str = "";
		int interactomeVersionId = getInteractomeVersionId(context, version,
				conn, statement);

		logger.debug("getInteractionIdsByEntrezId start query ...");

		aSql = "SELECT idl.interaction_id ";
		aSql += "FROM interactions_joined idl ";		 
		aSql += "WHERE idl.interactome_version_id =? ";
		aSql += "AND idl.primary_accession = ? ";	 	 
		aSql += "ORDER BY idl.interaction_id";
		 
		statement = conn.prepareStatement(aSql);
		statement.setInt(1, interactomeVersionId);
		statement.setInt(2, new Integer(geneId));
		ResultSet rs = statement.executeQuery();

		logger.debug("getInteractionIdsByEntrezId finist query ...");

		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("interaction_id");

		}

		statement.close();

		return str;

	}
	 
	
	private String getInteractionIdsByEntrezIdOrGeneSymbol_orig(String geneId,
			String geneSymbol, String context, String version, Connection conn,
			PreparedStatement statement) throws SQLException {

		String aSql = null;
		String str = "";
		int interactomeVersionId = getInteractomeVersionId(context, version,
				conn, statement);

		logger.info("start query ...");

		aSql = "SELECT idl.interaction_id ";
		aSql += "FROM interactions_joined idl ";		 
		aSql += "WHERE idl.interactome_version_id =? ";
		aSql += "AND ( idl.primary_accession = ? OR (idl.gene_symbol = ? AND idl.secondary_accession is not null ) ) ";	 	 
		aSql += "ORDER BY idl.interaction_id";
		
		logger.info(aSql + "- "+  interactomeVersionId + ":" + geneId + ":" + geneSymbol);
		
		statement = conn.prepareStatement(aSql);
		statement.setInt(1, interactomeVersionId);
		statement.setInt(2, new Integer(geneId));
		statement.setString(3, new String(geneSymbol));
		ResultSet rs = statement.executeQuery();

		logger.info("finist query ...");

		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("interaction_id");

		}

		statement.close();

		return str;

	}
	
	
	 
	
	private String getInteractionIdsByEntrezIdOrGeneSymbol(String geneId,
			String geneSymbol, String context, String version, Connection conn,
			PreparedStatement prepStatement) throws SQLException {

		String aSql = null;
		String str = "";
		int interactomeVersionId = getInteractomeVersionId(context, version,
				conn, prepStatement);

		
		logger.debug("start EntrezId query ...");
	 
		aSql = "SELECT interaction_id ";
		aSql += "FROM interactions_joined_part ";	 
		aSql += "WHERE interactome_version_id = " + interactomeVersionId;
		aSql += " AND primary_accession = " + geneId;		 
		aSql += " UNION ";
		aSql += "SELECT interaction_id ";
		aSql += "FROM interactions_joined_part ";	 
		aSql += "WHERE interactome_version_id = " + interactomeVersionId;
		aSql += " AND (gene_symbol = '" + geneSymbol + "' AND primary_accession is null ) ";		 
	 
		Statement statement = conn.createStatement();
		  
		ResultSet rs = statement.executeQuery(aSql); 		


		logger.debug("finist EntrezId query ...");

		int rowNum = 0;
		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("interaction_id");
			rowNum++;
		}

		statement.close();		
		return str;

	}
	
	
	
	
	private String getInteractionIdsByEntrezIdOrGeneSymbol_test(String geneId,
			String geneSymbol, String context, String version, Connection conn,
			PreparedStatement statement) throws SQLException {

		String aSql = null;
		String str = "";
		int interactomeVersionId = getInteractomeVersionId(context, version,
				conn, statement);

		logger.debug("start EntrezId query ...");

		aSql = "SELECT idl.interaction_id ";
		aSql += "FROM interactions_joined_part idl ";		 
		aSql += "WHERE idl.interactome_version_id =? ";
		aSql += "AND idl.primary_accession = ? ";	 	 
		aSql += "ORDER BY idl.interaction_id";
		statement = conn.prepareStatement(aSql);
		statement.setInt(1, interactomeVersionId);
		statement.setInt(2, new Integer(geneId));	 
		ResultSet rs = statement.executeQuery(); 		

		logger.debug("finish EntrezId query ...");

		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("interaction_id");

		}

		statement.close();
		
		logger.debug("start non EntrezId query ...");

		aSql = "SELECT idl.interaction_id ";
		aSql += "FROM interactions_joined_part idl ";		 
		aSql += "WHERE idl.interactome_version_id =? ";
		aSql += "AND (idl.gene_symbol = ? AND idl.primary_accession is null ) ";	 	 
		aSql += "ORDER BY idl.interaction_id";
		statement = conn.prepareStatement(aSql);
		statement.setInt(1, interactomeVersionId);		 
		statement.setString(2, new String(geneSymbol));
		rs = statement.executeQuery(); 
		
		logger.debug("finished non EntrezId query ...");
		
		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("interaction_id");

		}
		 
		statement.close();

		return str;

	}
	

	private String getInteractionIdsByGeneSymbol(String geneSymbol,
			String context, String version, Connection conn,
			PreparedStatement statement) throws SQLException {

		String aSql = null;
		String str = "";
		int interactomeVersionId = getInteractomeVersionId(context, version,
				conn, statement);

		aSql = "SELECT idl.interaction_id ";
		aSql += "FROM interactions_joined idl ";		 
		aSql += "WHERE idl.interactome_version_id =? ";
		aSql += "AND idl.gene_symbol = ? ) ";	 	 
		aSql += "ORDER BY idl.interaction_id";

		statement = conn.prepareStatement(aSql);
		statement.setInt(1, interactomeVersionId);
		statement.setString(2, geneSymbol);
		ResultSet rs = statement.executeQuery();

		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("interaction_id");

		}

		statement.close();

		return str;

	}

	 

	public ResultSet getInteractomeNames_orig(Connection conn,
			PreparedStatement statement) throws SQLException {
		String aSql = null;
		aSql = "SELECT i.name, COUNT(i.name) as interaction_count ";
		aSql += "FROM interactome i, interactome_version v, interaction_interactome_version iv ";
		aSql += "WHERE i.id=v.interactome_id ";
		aSql += "AND v.id=iv.interactome_version_id ";
		aSql += "GROUP BY i.name";
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}
	
	
	public ResultSet getInteractomeNames(Connection conn,
			PreparedStatement statement) throws SQLException {
		String aSql = null;
		aSql = "SELECT ic.name, ic.interaction_count ";
		aSql += "FROM interaction_count ic ";		 
		aSql += "ORDER BY ic.name";
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

	private ResultSet getInteractionTypesByInteractomeVersion(String context,
			String version, Connection conn, PreparedStatement statement)
			throws SQLException {
		String aSql = null;
		int versionId = getInteractomeVersionId(context, version, conn,
				statement);
		aSql = "SELECT interaction_type FROM interactome_2_interaction_type ";
		aSql += "WHERE interactome_version_id =" + versionId;
		 
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

	private ResultSet getInteractionsByEntrezId(String geneId, String context,
			String version, Connection conn, PreparedStatement statement)
			throws SQLException {
		String aSql = null;

		String interactionIdList = getInteractionIdsByEntrezId(geneId, context,
				version, conn, statement);

		aSql = getInteractionSqlbyIdList(interactionIdList);
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		// Statement statement = conn.createStatement();
		// ResultSet rs = statement.executeQuery(aSql);
		return rs;
	}

	private ResultSet getInteractionsByGeneSymbol(String geneSymbol,
			String context, String version, Connection conn,
			PreparedStatement statement) throws SQLException {
		String aSql = null;

		String interactionIdList = getInteractionIdsByGeneSymbol(geneSymbol,
				context, version, conn, statement);

		aSql = getInteractionSqlbyIdList(interactionIdList);
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();

		return rs;
	}	 
	 
	
	private ResultSet getInteractionsByEntrezIdOrGeneSymbol(String entrezId,
			String geneSymbol, String context, String version, Connection conn,
			PreparedStatement statement) throws SQLException {

		String aSql = null;
		 
		String interactionIdList = getInteractionIdsByEntrezIdOrGeneSymbol(entrezId, geneSymbol, context,
				version, conn, statement);		
		
		aSql = getInteractionSqlbyIdList(interactionIdList);
	 
		logger.debug("start query ...");
		logger.debug(aSql);		
		 
		statement = conn.prepareStatement(aSql);
	 
		ResultSet rs = statement.executeQuery();
 
		logger.debug("after query ...");

		return rs;
	}	
	
 
	
	private String getInteractionSqlbyIdList(String interactionIdList) {
		String aSql = null;

		if (interactionIdList.equals(""))
			interactionIdList = "0";	
		
		aSql = "SELECT idl.* ";
		aSql += "FROM interactions_joined_part idl ";		 
		aSql += "WHERE idl.interaction_id in (" + interactionIdList + ") ";	 
		aSql += "ORDER BY idl.interaction_id";
	 

		return aSql;

	}

	
	

}
