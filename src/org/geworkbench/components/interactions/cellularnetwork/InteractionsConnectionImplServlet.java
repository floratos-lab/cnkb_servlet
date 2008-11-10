package org.geworkbench.components.interactions.cellularnetwork;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

	private static final String COM_MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String MYSQL_PSWD = "zhangc2b2";
	private static final String MYSQL_USER = "xiaoqing";
	private static final String MYSQL_URL = "jdbc:mysql://afdev.cgc.cpmc.columbia.edu:3306/cellnet_kbase?autoReconnect=true";

	private static String message = "Error processing SQL query";

	@Override
	public void init() throws ServletException {
		super.init();

		try {
			Class.forName(COM_MYSQL_JDBC_DRIVER);
			// DriverManager.registerDriver(new
			// oracle.jdbc.driver.OracleDriver());

		} catch (ClassNotFoundException e) {
			logger
					.error(
							"init() - exception  in InteractionsConnectionImplServlet init  ---- ", e); //$NON-NLS-1$
			logger.error("init()", e); //$NON-NLS-1$
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		doPost(req, resp);
	}

	public void doPost(HttpServletRequest req, HttpServletResponse resp) {
		try {
			String jdbcURL = null;
			String user = null;
			String pswd = null;
			String sql = null;
			String db = null;

			if (logger.isDebugEnabled()) {
				logger
						.debug("doPost(HttpServletRequest, HttpServletResponse) - InteractionsConnectionImplServlet doPost, you got here  ---- "); //$NON-NLS-1$
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
			sql = tokens[1];

			if (db.equals(ORACLE)) {

				/*
				 * jdbcURL = JDBC_ORACLE_THIN_URL; user = USER_INTERACTION_RO;
				 * pswd = PSWD_ORACLE_LINKT0CELLNET;
				 */
				logger
						.error(
								"doPost(HttpServletRequest, HttpServletResponse) - This should never happens as Oracle is not used any more....", null); //$NON-NLS-1$
			} else if (db.equals(MYSQL)) {
				jdbcURL = MYSQL_URL;
				user = MYSQL_USER;
				pswd = MYSQL_PSWD;
			}

			Connection conn = DriverManager.getConnection(jdbcURL, user, pswd);
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
			conn.close();

			// set the response code and write the response data
			resp.setStatus(HttpServletResponse.SC_OK);

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

}
