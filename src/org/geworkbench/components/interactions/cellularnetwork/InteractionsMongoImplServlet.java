package org.geworkbench.components.interactions.cellularnetwork;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

import java.util.ArrayList; 
import java.util.List;
import java.util.Properties;
//import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.DatatypeConverter;

import java.util.Arrays;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public class InteractionsMongoImplServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	/**
	 * Logger for this class
	 */
	static final Logger logger = Logger
			.getLogger(InteractionsMongoImplServlet.class);
	private static final int MIN_DATA_LENGTH = 1;

	private static final String GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL = "getInteractionsByEntrezIdOrGeneSymbol";
	private static final String GET_INTERACTION_BY_ENTREZID = "getInteractionsByEntrezId";
	private static final String GET_INTERACTION_BY_GENESYMBOL = "getInteractionsByGeneSymbol";
	private static final String GET_INTERACTION_BY_GENESYMBOL_AND_LIMIT = "getInteractionsByGeneSymbolAndLimit";
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
	private static final String GET_NCI_DATASET_NAMES = "getNciDatasetNames";

	private static final String SECONDARY_ACCESSION = "secondary_accession";

	// currently in two files
	private static final int SPLIT_ALL = -2;
	private static final String DEL = "|";
	private static final String REGEX_DEL = "\\|";

	private static final String PROPERTIES_FILE = "interactionsweb.properties";

	private static final String DATASET_USER = "dataset.user";
	private static final String DATASET_PASSWD = "dataset.passwd";
	static String dataset_passwd;
	static String dataset_user;

	private static final String MONGODB_SERVER = "mangodb.server";
	private static final String MONGODB_PORT = "mangodb.port";
	private static final String MONGODB_DATABASE = "mangodb.database";
	private static final String MONGODB_USER = "mangodb.user";
	private static final String MONGODB_PASSWD = "mangodb.passwd";

	private static String mangodb_server;
	private static String mangodb_port;
	private static String mangodb_database;
	private static String mangodb_user;
	private static String mangodb_passwd;

	private MongoClient mongoClient = null;
	DB db = null;

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

			mangodb_server = iteractionsWebProp.getProperty(MONGODB_SERVER)
					.trim();
			mangodb_port = iteractionsWebProp.getProperty(MONGODB_PORT).trim();
			mangodb_database = iteractionsWebProp.getProperty(MONGODB_DATABASE)
					.trim();
			mangodb_user = iteractionsWebProp.getProperty(MONGODB_USER).trim();
			mangodb_passwd = iteractionsWebProp.getProperty(MONGODB_PASSWD)
					.trim();
			int portNum = new Integer(mangodb_port);

			dataset_user = iteractionsWebProp.getProperty(DATASET_USER).trim();
			dataset_passwd = iteractionsWebProp.getProperty(DATASET_PASSWD)
					.trim();

			ServerAddress server = new ServerAddress(mangodb_server, portNum);
			MongoCredential credentialOne = MongoCredential.createCredential(
					mangodb_user, mangodb_database,
					mangodb_passwd.toCharArray());
			mongoClient = new MongoClient(server, Arrays.asList(credentialOne));
			db = mongoClient.getDB(mangodb_database);

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

		Boolean isInteractionData = false;
		PrintWriter out = null;
		try {
			out = resp.getWriter();
			if (logger.isDebugEnabled()) {
				logger.debug("doPost(HttpServletRequest, HttpServletResponse) - InteractionsMongoImplServlet doPost, you got here  ---"); //$NON-NLS-1$
			}

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

			if (needAuthentication(tokens, req))
				askForPassword(resp);

			else {
				AggregationOutput rs = null;
				String geneId = null;
				if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID_OR_GENESYMBOL)) {
					geneId = tokens[1].trim();
					String geneSymbol = tokens[2].trim();
					String context = tokens[3].trim();
					String version = tokens[4].trim();
					getInteractionsByEntrezIdOrGeneSymbol(new Integer(geneId),
							geneSymbol, context, version, out);
					isInteractionData = true;
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_ENTREZID)) {
					geneId = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					getInteractionsByEntrezId(new Integer(geneId), context,
							version, out);
					isInteractionData = true;
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL)) {
					String geneSymbol = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					getInteractionsByGeneSymbol(geneSymbol, context, version,
							out);
					isInteractionData = true;
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_BY_GENESYMBOL_AND_LIMIT)) {
					String geneSymbol = tokens[1].trim();
					String context = tokens[2].trim();
					String version = tokens[3].trim();
					Integer limit = null;
					if (tokens.length >= 5)
						limit = new Integer(tokens[4].trim());

					getInteractionsByGeneSymbolAndLimit(geneSymbol, context,
							version, limit, out);
					isInteractionData = true;
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

					int versionId = getInteractomeVersionId(context, version);
					String shortName = getInteractionTypeShortName(interactionTypes, db);
					InteractionsMongoExport export = new InteractionsMongoExport();
					String existFileName = export.getExportExistFileName(context, version, shortName, nodePresentedBy, methodName);
					if (existFileName != null)
						export.exportExistFile(existFileName, out);
					else if (methodName
							.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT))			 
						export.getInteractionsSifFormat(versionId,
								interactionTypes, shortName, nodePresentedBy, out, db);
					else
						export.getInteractionsAdjFormat(versionId,
								interactionTypes, nodePresentedBy, out, db);				 
					isInteractionData = true;
				} else if (methodName.equalsIgnoreCase(GET_INTERACTION_TYPES)) {
					rs = this.getInteractionTypes();
				} else if (methodName.equalsIgnoreCase(GET_CONFIDENCE_TYPES)) {
					rs = this.getConfidenceTypes();
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_EVIDENCES)) {
					rs = this.getInteractionEvidences();
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTION_TYPES_BY_INTERACTOMEVERSION)) {
					String context = tokens[1].trim();
					String version = tokens[2].trim();
					rs = getInteractionTypesByInteractomeVersion(context,
							version);
				} else if (methodName.equalsIgnoreCase(GET_DATASET_NAMES)) {
					rs = this.getDatasetNames();
				} else if (methodName.equalsIgnoreCase(GET_NCI_DATASET_NAMES)) {
					rs = this.getNciDatasetNames();
				} else if (methodName.equalsIgnoreCase(GET_INTERACTOME_NAMES)) {
					rs = this.getInteractomeNames();
				} else if (methodName
						.equalsIgnoreCase(GET_INTERACTOME_DESCRIPTION)) {
					String context = tokens[1].trim();
					rs = this.getInteractomeDescription(context);
				} else if (methodName.equalsIgnoreCase(GET_VERSION_DESCRIPTOR)) {
					String context = tokens[1].trim();
					rs = this.getVersionDescriptor(context);
				}

				if (isInteractionData == false) {
					Object[] columns = null;
					boolean hasMetaData = false;
					for (DBObject result : rs.results()) {
						if (hasMetaData == false) {
							columns = result.keySet().toArray();
							out.println(getMetaData(columns, false));
							hasMetaData = true;
						}
						String row = new String();

						for (int i = 0; i < columns.length; i++) {
							if (i > 0) {
								row += DEL;
							}
							row += result.get(columns[i].toString()).toString();
						}

						out.println(row.trim());
					}

				}

				out.flush();
				
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
		} catch (MongoException e) {
			try {
				resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				resp.getWriter().print(e.getMessage());
				resp.getWriter().close();
				logger.error("get MongoException: ", e);

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

		}
		finally{			 
			if (out != null)
			   out.close();

		}
	}

	private void askForPassword(HttpServletResponse resp) {
		resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED); // Ie 401
		resp.setHeader("WWW-Authenticate",
				"BASIC realm=\"Requires Authentication \"");

	}

	private boolean needAuthentication(String[] tokens, HttpServletRequest req)
			throws IOException, MongoException {
		String methodName = tokens[0];

		if (!methodName
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
			if (getAuthentication(context, version).equalsIgnoreCase("N"))
				return false;
			String userAndPasswd = null;
			String userInfo = req.getHeader("Authorization");
			if (userInfo != null) {
				userInfo = userInfo.substring(6).trim();
				userAndPasswd = new String(
						DatatypeConverter.parseBase64Binary(userInfo));
				if (userAndPasswd.equals(dataset_user + ":" + dataset_passwd))
					return false;
			}

		}

		return true;
	}

	private AggregationOutput getConfidenceTypes() throws MongoException {

		// aSql = "SELECT id as id, name as name FROM confidence_type";
		DBCollection collection = db.getCollection("confidence_type");
		DBObject fields = new BasicDBObject();
		fields.put("id", 1);
		fields.put("name", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(project);
		AggregationOutput output = collection.aggregate(pipeline);

		return output;
	}

	private AggregationOutput getInteractionTypes() throws MongoException {

		DBCollection collection = db.getCollection("interaction_type");
		DBObject fields = new BasicDBObject();
		fields.put("interaction_type", "$name");
		fields.put("short_name", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(project);
		AggregationOutput output = collection.aggregate(pipeline);

		return output;
	}

	private AggregationOutput getInteractionEvidences() throws MongoException {

		// aSql =
		// "SELECT id as id, description as description FROM  evidence_type";
		DBCollection collection = db.getCollection("evidence_type");
		DBObject fields = new BasicDBObject();
		fields.put("id", 1);
		fields.put("description", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(project);
		AggregationOutput output = collection.aggregate(pipeline);

		return output;
	}

	private AggregationOutput getDatasetNames() throws MongoException {

		/*
		 * aSql =
		 * "SELECT ic.name as name, ic.interaction_count as interaction_count ";
		 * aSql += "FROM interaction_count ic "; aSql += "ORDER BY ic.name";
		 */

		DBCollection collection = db.getCollection("interaction_count");
		DBObject fields = new BasicDBObject();
		fields.put("name", 1);
		fields.put("interaction_count", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("name", 1));
		List<DBObject> pipeline = Arrays.asList(project, sort);
		AggregationOutput output = collection.aggregate(pipeline);

		return output;

	}

	private AggregationOutput getNciDatasetNames() throws MongoException {

		/*
		 * aSql =
		 * "SELECT DISTINCT ic.name as name, ic.interaction_count as interaction_count "
		 * ; aSql +=
		 * "FROM interactome_version v, interaction_count ic,  interactome i ";
		 * aSql += "WHERE v.interactome_id=i.id "; aSql +=
		 * "AND i.name=ic.name "; aSql += "AND v.authentication_yn='N' "; aSql
		 * += "AND v.nci_yn='Y' "; aSql += "ORDER BY ic.name";
		 */

		int interactomeId = 0;
		DBCollection collection = db.getCollection("interactome_version");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("authentication_yn", "N");
		whereQuery.put("nci_yn", "Y");
		DBObject match = new BasicDBObject("$match", whereQuery);
		DBObject fields = new BasicDBObject();
		fields.put("interactome_id", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);
		List<Integer> list = new ArrayList<Integer>();
		for (DBObject result : output.results()) {
			interactomeId = new Integer(result.get("interactome_id").toString());
			list.add(interactomeId);
		}

		collection = db.getCollection("interactome");
		BasicDBObject inQuery = new BasicDBObject();
		inQuery.put("id", new BasicDBObject("$in", list));
		match = new BasicDBObject("$match", inQuery);
		fields = new BasicDBObject();
		fields.put("name", 1);
		fields.put("_id", 0);
		project = new BasicDBObject("$project", fields);
		pipeline = Arrays.asList(match, project);
		collection.aggregate(pipeline);
		output = collection.aggregate(pipeline);
		List<String> nameList = new ArrayList<String>();
		for (DBObject result : output.results()) {
			String name = result.get("name").toString();
			nameList.add(name);
		}
		collection = db.getCollection("interaction_count");
		inQuery = new BasicDBObject();
		inQuery.put("name", new BasicDBObject("$in", nameList));
		match = new BasicDBObject("$match", inQuery);
		fields = new BasicDBObject();
		fields.put("name", 1);
		fields.put("interaction_count", 1);
		fields.put("_id", 0);
		project = new BasicDBObject("$project", fields);
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("name", 1));
		pipeline = Arrays.asList(match, project, sort);
		collection.aggregate(pipeline);
		output = collection.aggregate(pipeline);

		return output;
	}

	private AggregationOutput getVersionDescriptor(String context)
			throws MongoException {

		/*
		 * aSql =
		 * "SELECT DISTINCT v.version, v.authentication_yn as authentication_yn, v.description as description "
		 * ; aSql += "FROM interactome_version v, interactome i "; aSql +=
		 * "WHERE v.interactome_id=i.id "; aSql += " AND i.name='" + context +
		 * "'";
		 */

		int interactomeId = 0;
		DBCollection collection = db.getCollection("interactome");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("name", context);
		DBObject match = new BasicDBObject("$match", whereQuery);
		DBObject fields = new BasicDBObject();
		fields.put("id", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);
		for (DBObject result : output.results()) {
			interactomeId = new Integer(result.get("id").toString());
			break;
		}

		collection = db.getCollection("interactome_version");
		whereQuery = new BasicDBObject();
		whereQuery.put("interactome_id", interactomeId);
		match = new BasicDBObject("$match", whereQuery);
		fields = new BasicDBObject();
		fields.put("version", 1);
		fields.put("authentication_yn", 1);
		fields.put("description", 1);
		fields.put("_id", 0);
		project = new BasicDBObject("$project", fields);
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("version",
				1));
		pipeline = Arrays.asList(match, project, sort);
		output = collection.aggregate(pipeline);

		return output;

	}

	private AggregationOutput getInteractomeDescription(String interactomeName)
			throws MongoException {

		/*
		 * aSql = "SELECT i.description as description "; aSql +=
		 * "FROM interactome i "; aSql += "WHERE i.name=?";
		 */

		DBCollection collection = db.getCollection("interactome");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("name", interactomeName);
		DBObject match = new BasicDBObject("$match", whereQuery);
		DBObject fields = new BasicDBObject();
		fields.put("description", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);
		return output;
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

	private String getAuthentication(String context, String version)
			throws MongoException {

		String value = "N";
		DBCollection collection = db.getCollection("interactome");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("name", context);
		DBCursor cursor = collection.find(whereQuery);
		int id = 0;
		while (cursor.hasNext()) {
			id = new Integer(cursor.next().get("id").toString());
			break;
		}
		collection = db.getCollection("interactome_version");
		whereQuery = new BasicDBObject();
		whereQuery.put("interactome_id", id);
		whereQuery.put("version", version);
		cursor = collection.find(whereQuery);
		while (cursor.hasNext()) {
			value = cursor.next().get("authentication_yn").toString();
			break;
		}
		return value;
	}

	private int getInteractomeVersionId(String context, String version)
			throws MongoException {
		int interactomeId = 0;
		DBCollection collection = db.getCollection("interactome");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("name", context);
		DBObject match = new BasicDBObject("$match", whereQuery);
		DBObject fields = new BasicDBObject();
		fields.put("id", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);
		for (DBObject result : output.results()) {
			interactomeId = new Integer(result.get("id").toString());
			break;
		}

		collection = db.getCollection("interactome_version");
		whereQuery = new BasicDBObject();
		whereQuery.put("version", version);
		whereQuery.put("interactome_id", interactomeId);
		match = new BasicDBObject("$match", whereQuery);

		fields = new BasicDBObject();
		fields.put("id", 1);
		fields.put("_id", 0);
		project = new BasicDBObject("$project", fields);
		pipeline = Arrays.asList(match, project);
		output = collection.aggregate(pipeline);
		int interactomeVersionId = 0;
		for (DBObject result : output.results()) {
			interactomeVersionId = new Integer(result.get("id").toString());
			break;
		}

		return interactomeVersionId;
	}

	private List<Integer> getInteractionIdsByEntrezId(int geneId,
			int interactomeVersionId) throws MongoException {
		logger.debug("getInteractionIdsByEntrezId start query ...");

		/*
		 * aSql = "SELECT idl.interaction_id "; aSql +=
		 * "FROM interactions_joined_part idl "; aSql +=
		 * "WHERE idl.interactome_version_id =? "; aSql +=
		 * "AND idl.primary_accession = ? "; aSql +=
		 * "ORDER BY idl.interaction_id";
		 */

		DBCollection collection = db.getCollection("interactions_denorm");

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", interactomeVersionId);
		whereQuery.put("primary_accession", geneId);
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);

		List<Integer> idList = new ArrayList<Integer>();
		for (DBObject result : output.results()) {
			Integer id = new Integer(result.get("interaction_id").toString());
			idList.add(id);

		}

		return idList;
	}

	private List<Integer> getInteractionIdsByEntrezIdOrGeneSymbol(int geneId,
			String geneSymbol, int interactomeVersionId) throws MongoException {

		/*
		 * aSql = "SELECT interaction_id "; aSql +=
		 * "FROM interactions_joined_part "; aSql +=
		 * "WHERE interactome_version_id = " + interactomeVersionId; aSql +=
		 * " AND primary_accession = " + geneId; aSql += " UNION "; aSql +=
		 * "SELECT interaction_id "; aSql += "FROM interactions_joined_part ";
		 * aSql += "WHERE (gene_symbol = '" + geneSymbol +
		 * "' AND primary_accession is null ) "; aSql +=
		 * "AND interactome_version_id = " + interactomeVersionId;
		 */

		DBCollection collection = db.getCollection("interactions_denorm");		 
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", interactomeVersionId);
	    whereQuery.put("primary_accession", geneId);		 
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);		 
		if (output.results().iterator().hasNext() == false) {
			logger.info("search by symbol");
			whereQuery = new BasicDBObject();
			whereQuery.put("gene_symbol", geneSymbol);
			whereQuery.put("interactome_version_id", interactomeVersionId);			
			match = new BasicDBObject("$match", whereQuery);
			pipeline = Arrays.asList(match, project);
			output = collection.aggregate(pipeline);
		}
	 
		List<Integer> idList = new ArrayList<Integer>();
		for (DBObject result : output.results()) {
			Integer id = new Integer(result.get("interaction_id").toString());
			idList.add(id);

		}
	 
		return idList;

	}

	private List<Integer> getInteractionIdsByGeneSymbol(String geneSymbol,
			int interactomeVersionId) throws MongoException {

		/*
		 * aSql = "SELECT idl.interaction_id "; aSql +=
		 * "FROM interactions_joined_part idl "; aSql +=
		 * "WHERE idl.interactome_version_id =" + interactomeVersionId + " ";
		 * aSql += "AND idl.gene_symbol =  '" + geneSymbol + "' "; aSql +=
		 * "ORDER BY idl.interaction_id";
		 */

		DBCollection collection = db.getCollection("interactions_denorm");

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", interactomeVersionId);
		whereQuery.put("gene_symbol", geneSymbol);
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);

		List<Integer> idList = new ArrayList<Integer>();
		for (DBObject result : output.results()) {
			Integer id = new Integer(result.get("interaction_id").toString());
			idList.add(id);

		}

		return idList;

	}

	private List<Integer> getInteractionIdsByGeneSymbolAndLimit(
			String geneSymbol, int interactomeVersionId, Integer rowLimit)
			throws MongoException {

		/*
		 * aSql =
		 * "SELECT ijp.interaction_id as interaction_id FROM interactions_joined_part ijp "
		 * ;
		 * 
		 * aSql += "WHERE ijp.gene_symbol = '" + geneSymbol + "' "; aSql +=
		 * "AND ijp.interactome_version_id = " + interactomeVersionId + " "; if
		 * (rowLimit != null) aSql +=
		 * "ORDER BY ijp.confidence_value desc limit " + rowLimit;
		 */

		DBCollection collection = db.getCollection("interactions_denorm");

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", interactomeVersionId);
		whereQuery.put("gene_symbol", geneSymbol);
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);
		fields.put("confidence_value", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = null;
		if (rowLimit != null) {
			DBObject sort = new BasicDBObject("$sort", new BasicDBObject(
					"confidence_value", -1));
			DBObject limit = new BasicDBObject("$limit", rowLimit);
			pipeline = Arrays.asList(match, project, sort, limit);
		} else
			pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);
		List<Integer> idList = new ArrayList<Integer>();
		for (DBObject result : output.results()) {
			Integer id = new Integer(result.get("interaction_id").toString());
			idList.add(id);
		}
		return idList;

	}

	public AggregationOutput getInteractomeNames() throws MongoException {
		/*
		 * String aSql = null; aSql = "SELECT ic.name, ic.interaction_count ";
		 * aSql += "FROM interaction_count ic "; aSql += "ORDER BY ic.name";
		 */

		DBCollection collection = db.getCollection("interaction_count");
		DBObject fields = new BasicDBObject();
		fields.put("name", 1);
		fields.put("interaction_count", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("name", 1));
		List<DBObject> pipeline = Arrays.asList(project, sort);
		AggregationOutput output = collection.aggregate(pipeline);

		return output;
	}

	private AggregationOutput getInteractionTypesByInteractomeVersion(
			String context, String version) throws MongoException {
		int versionId = getInteractomeVersionId(context, version);
		DBCollection collection = db
				.getCollection("interactome_2_interaction_type");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);
		DBObject match = new BasicDBObject("$match", whereQuery);
		DBObject fields = new BasicDBObject();
		fields.put("interaction_type", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);
		return output;
	}

	private void getInteractionsByEntrezId(int geneId, String context,
			String version, PrintWriter out) throws MongoException {

		int interactomeVersionId = getInteractomeVersionId(context, version);
		List<Integer> idList = getInteractionIdsByEntrezId(geneId,
				interactomeVersionId);

		getInteractionsByIdList(idList, interactomeVersionId, out);

	}

	private void getInteractionsByGeneSymbol(String geneSymbol, String context,
			String version, PrintWriter out) throws MongoException {

		int interactomeVersionId = getInteractomeVersionId(context, version);

		logger.info("start id query ...");
		List<Integer> interactionIdList = getInteractionIdsByGeneSymbol(
				geneSymbol, interactomeVersionId);
		logger.info("start interaction query ...");
		getInteractionsByIdList(interactionIdList, interactomeVersionId, out);
		logger.info("end interaction query ...");
	}

	private void getInteractionsByGeneSymbolAndLimit(String geneSymbol,
			String context, String version, Integer rowLimit, PrintWriter out)
			throws MongoException {

		int interactomeVersionId = getInteractomeVersionId(context, version);
		logger.info("start id query ...");
		List<Integer> interactionIdList = getInteractionIdsByGeneSymbolAndLimit(
				geneSymbol, interactomeVersionId, rowLimit);
		logger.info("start interaction query ...");
		getInteractionsByIdList(interactionIdList, interactomeVersionId, out);
		logger.info("end interaction query ...");
	}

	private void getInteractionsByEntrezIdOrGeneSymbol(int entrezId,
			String geneSymbol, String context, String version, PrintWriter out)
			throws MongoException {

		int interactomeVersionId = getInteractomeVersionId(context, version);
		List<Integer> interactionIdList = null;
		logger.info("start id query ...");
		/*if (context.contains("Preppi") || context.contains("MINT")
				|| context.contains("Reactome"))
			interactionIdList = getInteractionIdsByGeneSymbol(geneSymbol,
					interactomeVersionId);
		else*/
			interactionIdList = getInteractionIdsByEntrezIdOrGeneSymbol(
					entrezId, geneSymbol, interactomeVersionId);
		logger.info("start interaction query ...");
		getInteractionsByIdList(interactionIdList, interactomeVersionId, out);

		logger.info("end interaction query ...");

	}

	private void getInteractionsByIdList(List<Integer> idList,
			int interactomeVersionId, PrintWriter out) throws MongoException {

		if (idList == null || idList.size() == 0)
			return;

		/*
		 * aSql =
		 * "SELECT ijp.primary_accession as primary_accession, ijp.secondary_accession as secondary_accession, ijp.interaction_id as interaction_id, ijp.accession_db as accession_db, ijp.gene_symbol as gene_symbol, ijp.interaction_type as interaction_type, ijp.evidence_id as evidence_id, ijp.confidence_value as confidence_value, ijp.confidence_type as confidence_type "
		 * ; aSql += "FROM interactions_joined_part ijp "; aSql +=
		 * "WHERE ijp.interaction_id in (" + interactionIdList + ") "; aSql +=
		 * "AND ijp.interactome_version_id = " + interactomeVersionId + " ";
		 * aSql += "ORDER BY ijp.interaction_id";
		 */

		DBCollection collection = db.getCollection("interactions_denorm");
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);
		fields.put("primary_accession", 1);
		fields.put("secondary_accession", 1);
		fields.put("accession_db", 1);
		fields.put("gene_symbol", 1);
		fields.put("interaction_type", 1);
		fields.put("evidence_id", 1);
		fields.put("confidence_value", 1);
		fields.put("confidence_type", 1);
		fields.put("role_name", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		String[] columns = { "primary_accession", "secondary_accession",
				"interaction_id", "accession_db", "gene_symbol",
				"interaction_type", "evidence_id", "confidence_value",
				"confidence_type", "role_name" };
		boolean hasMetaData = false;
		for (int id : idList) {
			DBObject whereQuery = new BasicDBObject();
			whereQuery.put("interaction_id", id);
			DBObject match = new BasicDBObject("$match", whereQuery);
			DBObject sort = new BasicDBObject("$sort", new BasicDBObject(
					"confidence_type", 1));
			List<DBObject> pipeline = Arrays.asList(match, project, sort);
			AggregationOutput output = collection.aggregate(pipeline);

			List<String> rowList = new ArrayList<String>();
			int previousConfidenceType = 0;
			String other_scores = "", other_types = "";
			for (DBObject result : output.results()) {
				if (hasMetaData == false) {
					out.println(getMetaData(columns, true));
					hasMetaData = true;
				}
				String row = new String();
				int currentConfidenceType = new Integer(result.get(
						"confidence_type").toString());
				previousConfidenceType = currentConfidenceType;
				if (previousConfidenceType == currentConfidenceType) {
					if (result.get(columns[0]) == null
							|| result.get(columns[0]).toString().trim()
									.equals("null")
							|| result.get(columns[0]).toString().trim()
									.equals("")) {
						for (int i = 1; i < columns.length; i++) {
							if (i > 1) {
								row += DEL;
							}
							if (result.get(columns[i]) != null)
								row += result.get(columns[i]).toString().trim();
							else
								row += result.get(columns[i]);

						}

					} else {
						for (int i = 0; i < columns.length; i++) {
							if (i == 1)
								continue;
							if (i > 0) {
								row += DEL;
							}
							if (result.get(columns[i]) != null)
								row += result.get(columns[i]).toString().trim();
							else
								row += result.get(columns[i]);
						}
					}

					rowList.add(row);

				} else {
					other_scores += (Float) result.get("confidence_value")
							+ ";";
					other_types += (Float) result.get("confidence_type") + ";";
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
		}

		logger.debug("End query cnkb ....");

	}

	private String getMetaData(Object[] columns, boolean isInteractionData) {
		String metaData = "";
		for (int i = 0; i < columns.length; i++) {
			if (columns[i].toString().trim()
					.equalsIgnoreCase(SECONDARY_ACCESSION)) {

				continue;
			}
			if (!metaData.equals("")) {
				metaData += DEL;
			}
			metaData += columns[i].toString();
		}

		if (isInteractionData)
			metaData += DEL + "other_confidence_values" + DEL
					+ "other_confidence_types";

		return metaData;
	}
	
	private String getInteractionTypeShortName(String interactionType, DB db) throws MongoException {
 
		String shortName = null;
		DBCollection collection = db.getCollection("interaction_type");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("name", interactionType);
		DBObject match = new BasicDBObject("$match", whereQuery);
		DBObject fields = new BasicDBObject();		 
		fields.put("short_name", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);
		for (DBObject result : output.results()) {			 
			  shortName = result.get("short_name").toString().trim();
			  if (shortName != null && !shortName.trim().equals(""))
				  return shortName;
		}
		
		return interactionType;
	}
	
	

	static void printMemoryUsage() {
		java.lang.management.MemoryMXBean memoryBean = java.lang.management.ManagementFactory
				.getMemoryMXBean();

		logger.info("init memory: " + memoryBean.getHeapMemoryUsage().getInit());
		logger.info("used memory: " + memoryBean.getHeapMemoryUsage().getUsed());
		logger.info("max memory: " + memoryBean.getHeapMemoryUsage().getMax());
	}

}
