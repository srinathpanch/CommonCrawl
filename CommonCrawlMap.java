package org.commoncrawl.examples.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class CommonCrawlMap {
	private static final Logger LOG = Logger.getLogger(CommonCrawlMap.class);

	protected static enum MAPPERCOUNTER {
		RECORDS_IN, EXCEPTIONS
	}

	protected static class CommonCrawlMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException, InterruptedException {
			int i = 1;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				Connection con = DriverManager.getConnection("jdbc:mysql://ccinstance.c1mcasvph2xx.us-east-1.rds.amazonaws.com/commoncrawl", "srinath", "uppal143");

				for (ArchiveRecord r : value) {
					try {
						LOG.debug(r.getHeader().getUrl() + " -- " + r.available());
						
						// Convenience function that reads the full message into a raw byte array
						byte[] rawData = IOUtils.toByteArray(r, r.available());
						String content = new String(rawData);
						
						// The HTTP header gives us valuable information about what was received during the request
						String headerText = content.substring(0, content.indexOf("\r\n\r\n"));
						
						// We're only interested in processing the responses, not requests or metadata
						if (r.getHeader().getMimetype().equals("application/http; msgtype=response")) {
							// In our task, we're only interested in text/html,so we can be a little lax
							// TODO: Proper HTTP header parsing + don't trust headers
							context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
							
							// Only extract the body of the HTTP response when necessary
							// Due to the way strings work in Java, we don't use any more memory than before
							 String body = content.substring(content.indexOf("\r\n\r\n") +4);

							 String serverName = "";
//							 try {
								 serverName = headerText.substring(headerText.indexOf("Server"), headerText.indexOf("Server") + 40);
//							 } catch (StringIndexOutOfBoundsException e) {
//								 serverName = "";
//							}
							String setCookieHeader = "";
//							try {
								 setCookieHeader = headerText.substring(headerText.indexOf("Set-Cookie:"), headerText.indexOf("Set-Cookie:") + 160);
//							} catch (StringIndexOutOfBoundsException e) {
//								 setCookieHeader = "";
//							}

							boolean isDrupal	 = headerText.contains("X-Generator");
							boolean isJoomla	 = body.contains("content=\"Joomla!");
							boolean isWordpress	 = body.contains("WordPress");
							boolean hasJQuery 	 = body.contains("code.jquery.com");
							String drup 	 = "" + isDrupal;
							String wp 		 = "" + isWordpress;
							String jm		 = "" + isJoomla;
							String jquery	 = "" + hasJQuery;
							String drupalVersion	 = "";
							String wordpressVersion	 = "";
							String joomlaVersion	 = "";
							String jQueryVersion	 = "";
							
							String isXframe = "" + headerText.contains("X-Frame-Options");
							String isCSP 	= "" + headerText.contains("Content-Security-Policy");
							String isPKP 	= "" + headerText.contains("Public-Key-Pins");
							String isSTS 	= "" + headerText.contains("Strict-Transport-Security");
							String isXCType = "" + headerText.contains("X-Content-Type-Options");
							String isXXSS 	= "" + headerText.contains("X-Content-Type-Options");
							String isXDown 	= "" + headerText.contains("X-Download-Options");
							String isCDP 	= "" + headerText.contains("X-Permitted-Cross-Domain-Policies");
							
							String arrayOfVulnerabilities[] = new String[19];
							
							arrayOfVulnerabilities[0] 	 = r.getHeader().getUrl();
							arrayOfVulnerabilities[1]	 = serverName;
							arrayOfVulnerabilities[2]	 = setCookieHeader;
							arrayOfVulnerabilities[3]	 = "X-Frame-Options set? - " + isXframe;
							arrayOfVulnerabilities[4]	 = "CSP set? - " + isCSP;
							arrayOfVulnerabilities[5] 	 = "PKP set? - " + isPKP;
							arrayOfVulnerabilities[6] 	 = "STS set? - " + isSTS;
							arrayOfVulnerabilities[7] 	 = "X-Content Type set? - " + isXCType;
							arrayOfVulnerabilities[8] 	 = "X-XSS set? - " + isXXSS;
							arrayOfVulnerabilities[9]	 = "X Download set? - " + isXDown;
							arrayOfVulnerabilities[10] 	 = "CDP set? - " + isCDP;
							arrayOfVulnerabilities[11]	 = "is Drupal? - " + isDrupal;
							arrayOfVulnerabilities[12] 	 = "is Joomla? - " + jm;
							arrayOfVulnerabilities[13]	 = "is Wordpress? - " + wp;
							
							if(isJoomla) {
								joomlaVersion =  body.substring(body.indexOf("Joomla!") , body.indexOf("Joomla!") + 30);
								arrayOfVulnerabilities[14]	 = "Joomla Version - " + joomlaVersion;
							} else if(isDrupal) {
								drupalVersion = headerText.substring(headerText.indexOf("X-Generator:") + 12, headerText.indexOf("X-Generator:") + 50);
								arrayOfVulnerabilities[15] 	 = "Drupal version" + drupalVersion;
							} else if(isWordpress) {
								wordpressVersion = body.substring(body.indexOf("WordPress"), body.indexOf("WordPress") + 30);
								arrayOfVulnerabilities[16]	 = "WP Version - " + wordpressVersion;
							}
							
							arrayOfVulnerabilities[17] 	= "Has jQuery? - " + jquery;
							
							if(hasJQuery) {
								jQueryVersion = body.substring(body.indexOf("src=\"https://code.jquery.com"), body.indexOf("src=\"https://code.jquery.com") + 50);
								arrayOfVulnerabilities[18] = "jQuery Version - " + jQueryVersion;
							}
							
							String query = "insert into march18 (Sno, URL, server, setcookie, xfo, csp, pkp, sts, XCT, xxss, xdown, cdp, drupal, drupalver, joomla, wordpress, wordpressver, joomlaver, jquery, jqueryver) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
							
							// create the mysql insert preparedstatement
							PreparedStatement preparedStmt = con.prepareStatement(query);
							preparedStmt.setInt(1, i++);
							preparedStmt.setString(2, r.getHeader().getUrl());
							preparedStmt.setString(3, serverName);
							preparedStmt.setString(4, setCookieHeader);
							preparedStmt.setString(5, isXframe);
							preparedStmt.setString(6, isCSP);
							preparedStmt.setString(7, isPKP);
							preparedStmt.setString(8, isSTS);
							preparedStmt.setString(9, isXCType);
							preparedStmt.setString(10, isXXSS);
							preparedStmt.setString(11, isXDown);
							preparedStmt.setString(12, isCDP);
							preparedStmt.setString(13, drup);
							preparedStmt.setString(14, drupalVersion);
							preparedStmt.setString(15, jm);
							preparedStmt.setString(16, wp);
							preparedStmt.setString(17, wordpressVersion);
							preparedStmt.setString(18, joomlaVersion);
							preparedStmt.setString(19, jquery);
							preparedStmt.setString(20, jQueryVersion);
//							
//							// execute the preparedstatement
							preparedStmt.executeUpdate();
							
							String outputText = "";
							for (int j = 0; j < 19; j++) {
								outputText = outputText + arrayOfVulnerabilities[j] + " \n ";
							}
							outKey.set(outputText);
						}
						context.write(outKey, outVal);
					} catch (Exception ex) {
						LOG.error("Caught Exception", ex);
						outKey.set(ex.toString());
						context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
						context.write(outKey, outVal);
					}
				}
				
				con.close();

			} catch (Exception e) {
				outKey.set(e.toString());
				context.write(outKey, outVal);
			}
		}
	}
}
