package org.geworkbench.components.interactions.cellularnetwork;

import org.apache.log4j.Logger;

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
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import sun.misc.BASE64Decoder;

/**
 * @author oleg stheynbuk
 * 
 * tmp. Servlet that will allow clients execute SQL query over HTTP from outside
 * the firewall
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
	private static final String GET_DATASET_NAMES = "getDatasetNames";
	private static final String GET_INTERACTOME_NAMES = "getInteractomeNames";
	private static final String GET_INTERACTOME_DESCRIPTION = "getInteractomeDescription";
	private static final String GET_VERSION_DESCRIPTOR = "getVersionDescriptor";
	private static final String GET_INTERACTION_TYPES_BY_INTERACTOMEVERSION = "getInteractionTypesByInteractomeVersion";
	private static final String GET_INTERACTIONS_SIF_FORMAT = "getInteractionsSifFormat";
	private static final String CLOSE_DB_CONNECTION = "closeDbConnection";
	
	private static final String TARGET = "target";
	private static final String MODULATOR = "modulator";

	public static final String GENE_NAME = "gene name";
	public static final String GENE_ID = "gene id";
	
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
	private static final String ENTREZ_GENE = "Entrez Gene";

	private static String mysql_jdbc_driver;
	private static String mysql_user;
	private static String mysql_passwd;
	private static String mysql_url;
	private static String dataset_passwd;
	private static String dataset_user;
	private Connection conn = null;
	//private PreparedStatement statement = null;

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

			Class.forName(mysql_jdbc_driver);

		} catch (ClassNotFoundException e) {
			logger
					.error(
							"init() - exception  in InteractionsConnectionImplServlet init  ---- ", e); //$NON-NLS-1$
			logger.error("init()", e); //$NON-NLS-1$
		} catch (IOException ie) {
			logger
					.error(
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
		boolean isSqlException = false;
 
		try {

			PrintWriter out = resp.getWriter();
			PreparedStatement statement = null;
			
			
			if (logger.isDebugEnabled()) {
				logger
						.debug("doPost(HttpServletRequest, HttpServletResponse) - InteractionsConnectionImplServlet doPost, you got here  ---"); //$NON-NLS-1$
			}

			int len = req.getContentLength();

			// if there are no input data - return
			if (len < MIN_DATA_LENGTH) {
				if (logger.isInfoEnabled()) {
					logger
							.info("doPost(HttpServletRequest, HttpServletResponse) - no input data provided"); //$NON-NLS-1$
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

			if (conn == null || conn.isClosed())
				conn = DriverManager.getConnection(mysql_url, mysql_user,
						mysql_passwd);

			if (needAuthentication(tokens, req))
				askForPassword(resp);

			else {
				ResultSet rs = null;
				String geneId = null;
				if (methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION)) {
					geneId = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					rs = this.getPairWiseInteraction(geneId, context, version, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)) {
					geneId = tokens[1].trim();
					String geneSymbol = tokens[2].trim();
					String context = tokens[3].trim();
					String version = tokens[4].trim();
					rs = this.getInteractionsByEntrezIdOrGeneSymbol(geneId,
							geneSymbol, context, version, statement);
					// rs = this.getInteractionsByEntrezId(geneId, context,
					// version);
					// rs = this.getInteractionsByGeneSymbol(geneSymbol,
					// context,
					// version);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID)) {
					geneId = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					rs = this.getInteractionsByEntrezId(geneId, context,
							version, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL)) {
					String geneSymbol = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					rs = this.getInteractionsByGeneSymbol(geneSymbol, context,
							version, statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT)) {
					String interactionTypes = "ALL";
					String nodePresentedBy = "GENE_NAME";
					String context = tokens[1].trim();
					String version = tokens[2].trim();
					if (tokens.length == 5) {
						interactionTypes = tokens[3].trim();
						nodePresentedBy = tokens[4].trim();
					}
					getInteractionsSifFormat(context, version,
							interactionTypes, nodePresentedBy, out);
				} else if (methodName.equalsIgnoreCase(GET_INTERACTION_TYPES)) {
					rs = this.getInteractionTypes(statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_TYPES_BY_INTERACTOMEVERSION)) {
					String context = tokens[1].trim();
					String version = tokens[2].trim();
					rs = this.getInteractionTypesByInteractomeVersion(context,
							version, statement);
				} else if (methodName.equalsIgnoreCase(GET_DATASET_NAMES)) {
					rs = this.getDatasetNames(statement);
				} else if (methodName.equalsIgnoreCase(GET_INTERACTOME_NAMES)) {
					rs = this.getInteractomeNames(statement);
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTOME_DESCRIPTION)) {
					String context = tokens[1].trim();
					rs = this.getInteractomeDescription(context, statement);
				} else if (methodName.equalsIgnoreCase(GET_VERSION_DESCRIPTOR)) {
					String context = tokens[1].trim();
					rs = this.getVersionDescriptor(context, statement);
				}
				

				if (!methodName.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT)) {
					String metaData = new String();

					String gene1Data = "";
					ResultSetMetaData rsmd;
					rsmd = rs.getMetaData();
					int numberOfColumns = rsmd.getColumnCount();

					// get metadata
					if (methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION)) {
						metaData = "ms_id1" + DEL + "db1_xref" + DEL + "gene1"
								+ DEL;
						gene1Data = getGeneDataByEntrezId(geneId, statement);
					}

					for (int i = 1; i <= numberOfColumns; i++) {
						if (i > 1) {
							metaData += DEL;
						}
						metaData += rsmd.getColumnName(i);
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
					if ( statement != null)
					  statement.close();				 

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
				isSqlException = true;
			} catch (IOException ioe) {
				logger
						.error(
								"exception in catch block: doPost(HttpServletRequest, HttpServletResponse)", ioe); //$NON-NLS-1$
			}
		} catch (SQLException e) {
			try {
				resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				resp.getWriter().print(e.getMessage());
				resp.getWriter().close();
				logger.error("get SQLException: ", e);
				isSqlException = true;

			} catch (IOException ioe) {
				logger
						.error(
								"exception in catch block: doPost(HttpServletRequest, HttpServletResponse)", ioe); //$NON-NLS-1$

			}
		} catch (Throwable e) {
			try {
				resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				resp.getWriter().print(e.getMessage());
				resp.getWriter().close();
				logger.error("get mother of all exceptions: ", e);
			} catch (IOException ioe) {
				logger
						.error(
								"exception in catch block: doPost(HttpServletRequest, HttpServletResponse)", ioe); //$NON-NLS-1$
			}

		} finally {
			if ((!methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION)
					&& !methodName
							.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)
					&& !methodName
							.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID) && !methodName
					.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL))
					|| isSqlException == true) {
				try {
					if (conn != null)
						conn.close();
					conn = null;
				} catch (SQLException e) {
				}

			}

		}

	}

	private void askForPassword(HttpServletResponse resp) {
		resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED); // Ie 401
		resp.setHeader("WWW-Authenticate",
				"BASIC realm=\"Requires Authentication \"");

	}

	private boolean needAuthentication(String[] tokens, HttpServletRequest req)
			throws IOException, SQLException {
		String methodName = tokens[0];

		if (!methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION)
				&& !methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)
				&& !methodName.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID)
				&& !methodName.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL)
				&& !methodName.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT))
			return false;
		else {
			String context = null;
			String version = null;
			if (methodName
					.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)) {
				context = tokens[3];
				version = tokens[4];
			
			}else if (methodName
					.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT)) {
				context = tokens[1];
				version = tokens[2];
			
			} 
			else {
				context = tokens[2];
				version = tokens[3];
			}
			if (getAuthentication(context, version).equalsIgnoreCase("N"))
				return false;
			String userInfo = req.getHeader("Authorization");
			BASE64Decoder decoder = new BASE64Decoder();
			String userAndPasswd = null;
			if (userInfo != null) {
				userInfo = userInfo.substring(6).trim();
				userAndPasswd = new String(decoder.decodeBuffer(userInfo));
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
	 * AND pi.id=ids.interaction_id And ids.dataset_id=" + datasetId; statement =
	 * conn.prepareStatement(aSql); statement.setString(1, geneId);
	 * statement.setString(2, geneId); ResultSet rs = statement.executeQuery();
	 * 
	 * return rs; }
	 */

	private ResultSet getPairWiseInteraction(String geneId, String context,
			String version, PreparedStatement statement) throws SQLException {
		String aSql = null;

		String interactionIdList = getInteractionIdsByEntrezId(geneId, context,
				version, statement);

		if (interactionIdList.equals(""))
			interactionIdList = "0";
		aSql = "SELECT pe.primary_accession as ms_id2, ds.name as db2_xref, pe.gene_symbol as gene2, i.confidence_value as confidence_value, it.name as interaction_type ";
		aSql += "FROM physical_entity pe, interaction_participant ip, interaction i, interaction_type it, db_source ds ";
		aSql += "WHERE pe.id=ip.participant_id ";
		aSql += "AND pe.accession_db=ds.id ";
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.interaction_type=it.id ";
		aSql += "AND i.id in (" + interactionIdList + ")";

		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();

		return rs;
	}

	private ResultSet getInteractionTypes(PreparedStatement statement) throws SQLException {
		String aSql = null;
		aSql = "SELECT name as interaction_type, short_name as short_name FROM  interaction_type";
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

	private ResultSet getDatasetNames(PreparedStatement statement) throws SQLException {
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

	/*
	 * private ResultSet getVersionDescriptor(String context) throws
	 * SQLException { String aSql = null; aSql = "SELECT DISTINCT version,
	 * authentication_yn FROM interaction_dataset, dataset where dataset.name='" +
	 * context + "' AND interaction_dataset.dataset_id = dataset.id;"; statement =
	 * conn.prepareStatement(aSql); ResultSet rs = statement.executeQuery();
	 * return rs; }
	 */

	private ResultSet getVersionDescriptor(String context, PreparedStatement statement) throws SQLException {
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

	private ResultSet getInteractomeDescription(String interactomeName, PreparedStatement statement)
			throws SQLException {
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
	 * SQLException { Statement statement = conn.createStatement(); String value =
	 * "N"; String sql = "SELECT * FROM dataset where name='" + context + "'";
	 * if (version != null && !version.equals("")) sql = sql + " AND version='" +
	 * version + "'"; ResultSet rs = statement.executeQuery(sql); while
	 * (rs.next()) { // Get the data from the row using the column name value =
	 * rs.getString("authentication_yn"); break; } statement.close(); return
	 * value; }
	 */

	private String getAuthentication(String context, String version)
			throws SQLException {
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
	 * while (rs.next()) { // Get the data from the row using the column name id =
	 * rs.getInt("id"); break; } statement.close(); return id; }
	 */

	private int getInteractomeVersionId(String context, String version, Connection connection, PreparedStatement statement)
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

	private String getGeneDataByEntrezId(String geneId, PreparedStatement statement) throws SQLException {

		String aSql = null;
		String str = "";

		aSql = "SELECT pe.primary_accession, ds.name, pe.gene_symbol FROM physical_entity pe, db_source ds ";
		aSql += "WHERE pe.primary_accession =? ";
		aSql += "AND pe.accession_db=ds.id";

		statement = conn.prepareStatement(aSql);
		statement.setString(1, geneId);
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
			String version, PreparedStatement statement) throws SQLException {

		String aSql = null;
		String str = "";
		int interactomeVersionId = getInteractomeVersionId(context, version, conn, statement);

		aSql = "SELECT i.id ";
		aSql += "FROM physical_entity pe, db_source ds, interaction_participant ip, interaction i, interaction_interactome_version  iiv ";
		aSql += "WHERE ip.participant_id = pe.id ";
		aSql += "AND ip.interaction_id = i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND ds.name='" + ENTREZ_GENE + "' ";
		aSql += "AND pe.accession_db = ds.id  ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND iiv.interactome_version_id =? ";
		aSql += "AND pe.primary_accession = ?";

		statement = conn.prepareStatement(aSql);
		statement.setInt(1, interactomeVersionId);
		statement.setString(2, geneId);
		ResultSet rs = statement.executeQuery();

		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("id");

		}

		statement.close();

		return str;

	}

	private String getInteractionIdsByGeneSymbol(String geneSymbol,
			String context, String version, PreparedStatement statement) throws SQLException {

		String aSql = null;
		String str = "";
		int interactomeVersionId = getInteractomeVersionId(context, version, conn, statement);

		aSql = "SELECT i.id ";
		aSql += "FROM physical_entity pe, interaction_participant ip, interaction i, interaction_interactome_version  iiv ";
		aSql += "WHERE ip. participant_id = pe.id ";
		aSql += "AND ip.interaction_id = i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND iiv.interactome_version_id =? ";
		aSql += "AND pe.gene_symbol = ?";

		statement = conn.prepareStatement(aSql);
		statement.setInt(1, interactomeVersionId);
		statement.setString(2, geneSymbol);
		ResultSet rs = statement.executeQuery();

		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("id");

		}

		statement.close();

		return str;

	}

	 

	private String getNonEntrezInteractionIdsByGeneSymbol(String geneSymbol,
			String context, String version, PreparedStatement statement) throws SQLException {

		String aSql = null;
		String str = "";
		int interactomeVersionId = getInteractomeVersionId(context, version, conn, statement);

		aSql = "SELECT i.id ";
		aSql += "FROM physical_entity pe, db_source ds, interaction_participant ip, interaction i, interaction_interactome_version  iiv ";
		aSql += "WHERE ip. participant_id = pe.id ";
		aSql += "AND ip.interaction_id = i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND ds.name <> '" + ENTREZ_GENE + "' ";
		aSql += "AND pe.accession_db = ds.id  ";
		aSql += "AND iiv.interactome_version_id =? ";
		aSql += "AND pe.gene_symbol = ?";

		statement = conn.prepareStatement(aSql);
		statement.setInt(1, interactomeVersionId);
		statement.setString(2, geneSymbol);
		ResultSet rs = statement.executeQuery();

		while (rs.next()) {
			if (!str.trim().equals(""))
				str += ", ";
			str += rs.getString("id");

		}

		statement.close();

		return str;

	}

 
	public ResultSet getInteractomeNames(PreparedStatement statement) throws SQLException {
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

	private ResultSet getInteractionTypesByInteractomeVersion(String context,
			String version, PreparedStatement statement) throws SQLException {
		String aSql = null;
		int versionId = getInteractomeVersionId(context, version, conn, statement);
		aSql = "SELECT DISTINCT t.name as interaction_type FROM interaction_type t, interaction i, interaction_interactome_version iv ";
		aSql += "WHERE iv.interaction_id = i.id ";
		aSql += "AND i.interaction_type = t.id ";
		aSql += "AND iv.interactome_version_id = " + versionId;
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		return rs;
	}

	private ResultSet getInteractionsByEntrezId(String geneId, String context,
			String version, PreparedStatement statement) throws SQLException {
		String aSql = null;

		String interactionIdList = getInteractionIdsByEntrezId(geneId, context,
				version, statement);

		aSql = getInteractionSqlbyIdList(interactionIdList, context, version);
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();
		// Statement statement = conn.createStatement();
		// ResultSet rs = statement.executeQuery(aSql);
		return rs;
	}

	private ResultSet getInteractionsByGeneSymbol(String geneSymbol,
			String context, String version, PreparedStatement statement) throws SQLException {
		String aSql = null;

		String interactionIdList = getInteractionIdsByGeneSymbol(geneSymbol,
				context, version, statement);

		aSql = getInteractionSqlbyIdList(interactionIdList, context, version);
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery();

		return rs;
	}

	private void getInteractionsSifFormat(String context, String version,
			String interactionType, String presentBy, PrintWriter out)
			throws SQLException {
		
		logger.info("Start exporting....");
		String aSql = null;
		Connection exportConn = DriverManager.getConnection(mysql_url, mysql_user,
				mysql_passwd);
		PreparedStatement prepStat = null;
		int versionId = getInteractomeVersionId(context, version, exportConn, prepStat);
		String columnName = null;
		if (presentBy.equalsIgnoreCase(GENE_ID))
			columnName = "primary_accession";
		else
			columnName = "gene_symbol";

		

		List<String> tfList = getRelatedInteractionGenes(versionId,
				interactionType, columnName, exportConn);
		
		out.println("sif format data");
	 
		for (String tfGene : tfList) {
			logger.debug("Start get ...." +  tfGene);
			if (tfGene == null || tfGene.trim().equals("") || tfGene.equalsIgnoreCase("UNKNOWN"))
			{
				logger.info("drop gene ...." +  tfGene);
				continue;
			}
			aSql = "SELECT i.id ";
			aSql += "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it ";
			aSql += "WHERE pe." + columnName + " = ? "; 
			aSql += "AND iiv.interactome_version_id = ? ";
			if (!interactionType.equalsIgnoreCase("ALL")) {
				aSql += "and it.name ='" + interactionType + "' ";
				aSql += "AND it.id = i.interaction_type ";			}	
			aSql += "AND ip.interaction_id=i.id ";
			aSql += "AND i.id = iiv.interaction_id ";		 
			aSql += "AND pe.id=ip.participant_id  ";
			
			prepStat = exportConn.prepareStatement(aSql);
			prepStat.setString(1, tfGene);
			prepStat.setInt(2, versionId);
			 
			ResultSet rs1 = prepStat.executeQuery();
            String idList = "";
			while (rs1.next()) {
				if (!idList.trim().equals(""))
					idList += ", ";
				idList += rs1.getString("id");

			}
            rs1.close();
			prepStat.close();
			
			aSql = "SELECT pe." + columnName + ", it.short_name, r.name ";
			aSql += "FROM physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r ";
			aSql += "WHERE i.id in (" + idList + ") "; 			 
			aSql += "AND pe." + columnName + " <> ? ";
			aSql += "AND pe.id=ip.participant_id ";
			aSql += "AND ip.interaction_id=i.id ";
			aSql += "AND i.interaction_type=it.id ";
			aSql += "AND ip.role_id = r.id ";
			aSql += "order by it.short_name, pe.gene_symbol";

			prepStat = exportConn.prepareStatement(aSql);
			prepStat.setString(1, tfGene);
			 
			
		 
			ResultSet rs2 = prepStat.executeQuery();
		 
			String targetGene = null;
			String shortName = null;
			String previousShortName = null;
			String roleName= null;	
			boolean hasData = false;
			
			while (rs2.next()) {
				targetGene = rs2.getString(columnName);
				shortName = rs2.getString("short_name");
				roleName = rs2.getString("name");
			 
				if (roleName.equals(MODULATOR))
					continue;
				if (previousShortName == null) {	
					hasData = true;
					out.print(tfGene + " " + shortName + " " + targetGene);
				} else if (previousShortName.equalsIgnoreCase(shortName)) {
					out.print(" " + targetGene);
				} else {
					out.println();
					out.print(tfGene + " " + shortName + " " + targetGene);

				}
				previousShortName = shortName;
			}
			if (hasData)
                out.println();
			rs2.close();
			prepStat.close();
		 
		}
		
		exportConn.close();
		logger.info("End exporting....");
	}

	private List<String> getRelatedInteractionGenes(int versionId,
			String interactionType, String presentBy, Connection exportConn ) throws SQLException {
		String aSql = null;
		List<String> tfList = new ArrayList<String>();
		aSql = "SELECT pe." + presentBy + ", + it.short_name ";
		aSql += "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r ";
		aSql += "WHERE pe.id=ip.participant_id ";
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND r.name <> '" + TARGET + "' ";
		aSql += "AND ip.role_id = r.id ";
		aSql += "And iiv.interactome_version_id = " + versionId + " ";
		if (!interactionType.equalsIgnoreCase("ALL")) {
			aSql += "AND it.id = i.interaction_type ";
			aSql += "and it.name ='" + interactionType + "' ";
		}
		aSql += "group by pe.gene_symbol";

		Statement stm = exportConn.createStatement();
		ResultSet rs = stm.executeQuery(aSql);

		while (rs.next()) {
			tfList.add(rs.getString(presentBy));
		}
        stm.close();
        rs.close();
     
		return tfList;
	}

	private ResultSet getInteractionsByEntrezIdOrGeneSymbol(String entrezId,
			String geneSymbol, String context, String version, PreparedStatement statement)
			throws SQLException {
		String aSql = null;

		/*
		 * String interactionIdList = this
		 * .getInteractionIdsByEntrezIdOrGeneSymbol(entrezId, geneSymbol,
		 * context, version);
		 */
		String interactionIdListByEntrezId = getInteractionIdsByEntrezId(
				entrezId, context, version, statement);
		String interactionIdListByName = this
				.getNonEntrezInteractionIdsByGeneSymbol(geneSymbol, context,
						version, statement);
		String interactionIdList = "";
		if (!interactionIdListByEntrezId.equals("")
				&& !interactionIdListByName.equals(""))
			interactionIdList = interactionIdListByEntrezId + ", "
					+ interactionIdListByName;
		else if (!interactionIdListByEntrezId.equals(""))
			interactionIdList = interactionIdListByEntrezId;
		else if (!interactionIdListByName.equals(""))
			interactionIdList = interactionIdListByName;

		aSql = getInteractionSqlbyIdList(interactionIdList, context, version);
		// aSql = getInteractionSymAndTypeSql(context, version);
		statement = conn.prepareStatement(aSql);
		ResultSet rs = statement.executeQuery(aSql);
		return rs;
	}

	private String getInteractionSqlbyIdList(String interactionIdList,
			String context, String version) {
		String aSql = null;

		if (interactionIdList.equals(""))
			interactionIdList = "0";
		aSql = "SELECT pe.primary_accession as primary_accession, ds.name as accession_db, pe.gene_symbol as gene_symbol, i.id as interaction_id,i.confidence_value as confidence_value, it.name as interaction_type ";
		aSql += "FROM physical_entity pe, interaction_participant ip, interaction i, interaction_type it, db_source ds ";
		aSql += "WHERE pe.id=ip.participant_id ";
		aSql += "AND pe.accession_db=ds.id ";
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.interaction_type=it.id ";
		aSql += "AND i.id in (" + interactionIdList + ") ";
		aSql += "ORDER BY i.id";

		return aSql;
	}

	 

}
