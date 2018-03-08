package com.websystique.springmvc.service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String str = "campaignName:,6035489913829";
		Pattern p = Pattern.compile("(campaignName:)(.*)(,)", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(str);
		String result = m.replaceAll('"'+"campaignName"+'"'+":"+'"'+"$2"+'"'+","+'"');
	    System.out.println(result);
	
	    ReportDAOImpl repDAO = ReportDAOImpl.getInstance();
	    repDAO.FetchDeviceImpData("impressions", "2016-01-01","2017-01-31", "12721","all","true", "true");
	    repDAO.FetchOSImpData("impressions", "2016-01-01","2017-01-31", "12721","all","true", "true");
	}

}
