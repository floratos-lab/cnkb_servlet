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
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse; 
import javax.servlet.http.Cookie;

import sun.misc.BASE64Decoder;


/**
 * @author oleg stheynbuk
 *
 *         tmp. Servlet that will allow clients execute SQL query
 *         over HTTP from outside the firewall
 *
 * @param  db and query string.
 *
 * @return  query executed, result set processed
 *         and send as a text; for more info @see ResultSetlUtil
 *
 */
public class InteractionsConnectionImplServlet extends HttpServlet {
	private static final int MIN_DATA_LENGTH = 1;
	private static final String GET_PAIRWISE_INTERACTION = "getPairWiseInteraction";
    private static final String GET_INTERACTION_TYPES = "getInteractionTypes";
    private static final String GET_DATASET_NAMES = "getDatasetNames";
    private static final String GET_VERSION_DESCRIPTOR = "getVersionDescriptor";
    
    /**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(InteractionsConnectionImplServlet.class);

	/*
	 * private static final String PSWD_ORACLE_LINKT0CELLNET = "linkt0cellnet";
	 * private static final String USER_INTERACTION_RO = "interaction_ro";
	 * private static final String JDBC_ORACLE_THIN_URL =
	 * "jdbc:oracle:thin:@adora.cgc.cpmc.columbia.edu:1521:BIODB2";
	 */
	// currently in two files
	static final int SPLIT_ALL = -2;
	static final String DEL = "|";
	private static final String REGEX_DEL = "\\|";
	static final String ORACLE = "oracle";
	static final String MYSQL = "mysql";
	static final String PROPERTIES_FILE = "interactionsweb.properties";
	private static final String MYSQL_JDBC_DRIVER  = "mysql.jdbc.Driver";
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

	private static String message = "Error processing SQL query";

	@Override
	public void init() throws ServletException {
		super.init();

		try {
			
			Properties iteractionsWebProp = new Properties();
			 InputStream instream = getClass().getResourceAsStream("/" + PROPERTIES_FILE);
	            if (instream != null) {
	            	iteractionsWebProp.load(instream);
	                instream.close();
	            }
		 
			mysql_jdbc_driver = iteractionsWebProp.getProperty(MYSQL_JDBC_DRIVER).trim();
			mysql_user = iteractionsWebProp.getProperty(MYSQL_USER).trim();
			mysql_passwd = iteractionsWebProp.getProperty(MYSQL_PASSWD).trim();
			mysql_url = iteractionsWebProp.getProperty(MYSQL_URL).trim();
			dataset_user = iteractionsWebProp.getProperty(DATASET_USER).trim();
			dataset_passwd = iteractionsWebProp.getProperty(DATASET_PASSWD).trim();
			 
			Class.forName(mysql_jdbc_driver);
		} catch (ClassNotFoundException e) {
			logger
					.error(
							"init() - exception  in InteractionsConnectionImplServlet init  ---- ", e); //$NON-NLS-1$
			logger.error("init()", e); //$NON-NLS-1$
		}
		catch(IOException ie)
		{
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
		try { 
			
			String methodName = null;
			String db = null;

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
			db = tokens[0];
			methodName = tokens[1].trim();
			 

			if (db.equals(ORACLE)) {

				/*
				 * jdbcURL = JDBC_ORACLE_THIN_URL; user = USER_INTERACTION_RO;
				 * pswd = PSWD_ORACLE_LINKT0CELLNET;
				 */
				logger
						.error(
								"doPost(HttpServletRequest, HttpServletResponse) - This should never happens as Oracle is not used any more....", null); //$NON-NLS-1$
			}  

			Connection conn = DriverManager.getConnection(mysql_url, mysql_user, mysql_passwd);
			if (needAuthentication(tokens, conn, req))
				askForPassword(resp);
			else
			{		  
			    String sql = getSqlString(tokens, conn);
			    if (logger.isDebugEnabled()) {
					logger
							.debug("doPost(HttpServletRequest, HttpServletResponse) - InteractionsConnectionImplServlet doPost, sql string is " + sql  ); //$NON-NLS-1$
				}
			    
			    Statement statement = conn.createStatement();
			    ResultSet rs = statement.executeQuery(sql);
			    PrintWriter out = resp.getWriter();

			    String metaData = new String();
			    ResultSetMetaData rsmd;
			    rsmd = rs.getMetaData();
		    	int numberOfColumns = rsmd.getColumnCount();

		     	// get metadata
			    for (int i = 1; i <= numberOfColumns; i++) {
				   if (i > 1) {
					   metaData += DEL;
				   }
				   metaData += rsmd.getColumnName(i);
			    }
			    out.println(metaData);

			    // get values
		        while (rs.next()) {
				    String row = new String();

				    for (int i = 1; i <= numberOfColumns; i++) {
				   	   if (i > 1) {
						   row += DEL;
					    }
					     row += rs.getString(i);
				     }
				     out.println(row);
			    }
			
			  
	
 
			    out.flush();
			    out.close();

			    rs.close();
			    statement.close();
			    resp.setStatus(HttpServletResponse.SC_OK);
			    
			}    
			    
			    
			conn.close();
			
	

			// set the response code and write the response data
			

		} catch (IOException e) {
			try {
				resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				resp.getWriter().print(e.getMessage());
				resp.getWriter().close();
				logger.error("get IOException: ", e);
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

		}
		 

	}
	
	
	private void askForPassword(HttpServletResponse resp)
	{
		resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED); // Ie 401
		 resp.setHeader("WWW-Authenticate",
         "BASIC realm=\"Requires Authentication \"");

	}
	
	private boolean needAuthentication(String[] tokens, Connection conn, HttpServletRequest req) throws IOException, SQLException 
	{
		String methodName = tokens[1];

		if (!methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION))
		    return false;
		else
		{
			String context = tokens[3];
			String version = tokens[4];
			if (getAuthentication(conn,context,version).equalsIgnoreCase("N"))
				return false;			
			String userInfo = req.getHeader("Authorization");			
			BASE64Decoder decoder = new BASE64Decoder();			 
			String userAndPasswd=null;
			if ( userInfo != null)
			{
		    	 userInfo = userInfo.substring(6).trim();
		         userAndPasswd = new String(decoder.decodeBuffer(userInfo));
		         if (userAndPasswd.equals(dataset_user + ":" + dataset_passwd))
		        	 return false;
			}
			 
			 
		}
		
		return true;
	}
	
	private String getSqlString(String[] tokens, Connection conn) throws SQLException
	{
		String aSql = null;
		String methodName = tokens[1];
		if (methodName.equalsIgnoreCase(GET_PAIRWISE_INTERACTION))
		{
			String geneId = tokens[2].trim();
			String context = tokens[3].trim();
			String version = tokens[4].trim();
			int datasetId = getDatasetId(conn, context, version);
			aSql = "SELECT pi.ms_id1, pi.ms_id2, pi.gene1, pi.gene2, pi.confidence_value, pi.is_modulated, pi.source, it.interaction_type, it.description FROM pairwise_interaction pi, interaction_dataset ids, interaction_type it";
			aSql += " WHERE (ms_id1=" + geneId + " OR ms_id2=" + geneId;
			aSql += ") AND pi.interaction_type=it.id AND pi.id=ids.interaction_id And ids.dataset_id=" + datasetId;
		}
		else if (methodName.equalsIgnoreCase(GET_INTERACTION_TYPES))
		{
			aSql = "SELECT  interaction_type FROM  interaction_type";
		}
		else if (methodName.equalsIgnoreCase(GET_DATASET_NAMES))
		{
			aSql= "SELECT name FROM dataset";
		}
		else if (methodName.equalsIgnoreCase(GET_VERSION_DESCRIPTOR))
		{
			String context = tokens[2].trim();
			aSql = "SELECT version FROM dataset where name='" + context + "'";
		}
	   
		return aSql;
	}
	 
	private String getAuthentication(Connection conn, String context,String version) throws SQLException
	{
		    Statement statement = conn.createStatement();
		    String value = "N";
		    String sql = "SELECT * FROM dataset where name='" + context + "'";
		    if ( version != null && !version.equals(""))
		    	sql = sql + " AND version='" + version + "'" ;
		    ResultSet rs = statement.executeQuery(sql);
		    while (rs.next()) {
	             
	            // Get the data from the row using the column name
		    	value = rs.getString("authentication_yn");
		    	break;
		    }
		    statement.close();
		    return value;
	}
	
	private int getDatasetId(Connection conn, String context,String version) throws SQLException
	{
		    Statement statement = conn.createStatement();
		    int id = 0;
		    String sql = "SELECT * FROM dataset where name='" + context + "'";
		    if ( version != null && !version.equals(""))
		    	sql = sql + " AND version='" + version + "'" ;
		    ResultSet rs = statement.executeQuery(sql);
		    while (rs.next()) {
	             
	            // Get the data from the row using the column name
		    	id = rs.getInt("id");
		    	break;
		    }
		    statement.close();
		    return id;
	}
	
}
