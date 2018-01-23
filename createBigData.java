j/*
 * Get each day's page views for topics mentioned in main method
 */
package wiki.wiki;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONException;
import org.json.JSONObject;

public class createBigData {

	private final String USER_AGENT = "Mozilla/5.0";

	public static void main(String[] args) throws Exception {
		prepURL();
	}

	// HTTP GET request
	private String sendGet(String url) throws Exception {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		
		// optional default is GET
		con.setRequestMethod("GET");

		//add request header
		con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		//System.out.println("\nSending 'GET' request to URL : " + url);

		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		
		return response.toString();
	}
	
	public static void parseFromJSONResponse(String respo) 
    {
        JSONObject myjson;
        try 
        {
            myjson = new JSONObject(respo);
            for(int i = 0 ; i< 30 ; i++){
            	JSONObject res = myjson.getJSONArray("items").getJSONObject(i);
            	String result = res.getString("article") + "," + res.getString("timestamp").substring(0, 8) + "," + res.getLong("views");
            	
            	try(FileWriter fw = new FileWriter("wikicounts", true);
            		    BufferedWriter bw = new BufferedWriter(fw);
            		    PrintWriter out = new PrintWriter(bw))
            		{
            		    out.println(result);
            		} catch (IOException e) {
            			e.printStackTrace();
            		}
            }
        	
        } 
        catch (JSONException e) {
            e.printStackTrace();
        }
    }
	
	public static void prepURL() throws Exception{
		String[] articles = { "Isaac_Newton","Albert_Einstein" , "Thomas_Edison" , "Galileo_Galilei" , "Archimedes"} ;
		String[] startDates = {"2015100100" , "2015090100" , "2015080100"  };
		String[] endDates = {"2015103100" , "2015093000" , "2015083100"};
		createBigData http = new createBigData();
		for(int i = 0 ; i < articles.length ; i++){
			for(int j = 0 ; j < startDates.length ; j++){
				String url1 = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/";
				String url2 = articles[i];	 //Article name
				String url3 = "/daily/" ; 
				String url4 = startDates[j]; //Start Date
				String url5 = "/";
				String url6 = endDates[j]; //End Date
				StringBuilder sb = new StringBuilder();
				sb.append(url1).append(url2).append(url3).append(url4).append(url5).append(url6);
				String res = sb.toString();
				String response = http.sendGet(res);
				parseFromJSONResponse(response);
			}
		}
		
	}

}