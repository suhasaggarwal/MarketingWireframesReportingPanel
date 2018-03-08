package com.websystique.springmvc.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.websystique.springmvc.model.CampaignData;
import com.websystique.springmvc.model.Reports;
import com.websystique.springmvc.service.ReportService;

//Application code - b7

@RestController
public class ReportRestController {

	
	//Controller receives data from Service API and generates a JSON feed to feed into visualisation component.
	//API format <Report/Reportcode/Daterange/CampaignId, as campaignId is specified after daterange, selected campaign is picked and campaign report is shown channel wise.	
	
	
	@Autowired
	ReportService reportService;  //Service which will do all data retrieval/manipulation work

	//-------------------Retrieve Report with Id--------------------------------------------------------
	@RequestMapping(value = "/report/{QueryField}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@CrossOrigin
	public ResponseEntity<String> getReport(@PathVariable("QueryField") String queryfield,
			@RequestParam("dateRange") String dateRange,
			@RequestParam(value = "metric", required = false) String metric,
			@RequestParam(value = "campaign_id", required = false) String campaignId,
			@RequestParam(value = "channel", required = false) String channel,
			@RequestParam(value= "aggregated") String aggregate,
			@RequestParam(value= "ratiometric",required = false) String ratiometric
			) {
		
		System.out.println("Fetching Report with"+queryfield);
		String report = reportService.extractReports(queryfield,metric,dateRange,campaignId,channel,aggregate,ratiometric,"false");
		if (report == null) {
			System.out.println("Report with with"+queryfield+ " not found");
			return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<String>(report, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/report/{QueryField}/downloadCSV", method = RequestMethod.GET, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
	@CrossOrigin
	public void getCSVReport(@PathVariable("QueryField") String queryfield,
			@RequestParam("dateRange") String dateRange,
			@RequestParam(value = "metric", required = false) String metric,
			@RequestParam(value = "campaign_id", required = false) String campaignId,
			@RequestParam(value = "channel", required = false) String channel,
			@RequestParam(value= "aggregated") String aggregate,
			@RequestParam(value= "ratiometric",required = false) String ratiometric,
			HttpServletResponse response
			) {
		
		System.out.println("Fetching CSV with"+queryfield);
		 
		String filename = "";
		try {
		      // get your file as InputStream
			 
			if(queryfield.equals("gender"))
			{
				filename = "/root/gender.csv";
			}
				
			if(queryfield.equals("os"))
			{
				filename = "/root/os.csv";
			}
			
			
			if(queryfield.equals("metricdata"))
			{
				filename = "/root/metricdata.csv";
			}
			
			
			
			
			if(queryfield.equals("agegroup"))	
			{
				
				filename = "/root/agegroup.csv";
			}
			
			if(queryfield.equals("income"))	
			{
				filename = "/root/income.csv";
			
			}
			
			if(queryfield.equals("device"))	
			{
				
				filename = "/root/device.csv";
			}
			
			
			if(queryfield.equals("audience_segment"))	
			{
				filename = "/root/audience_segment.csv";
			
			}
			
			if(queryfield.equals("screensize"))	
			{
				
				filename = "/root/screensize.csv";
			}
			
			if(queryfield.equals("brand"))	
			{
				
				filename = "/root/brand.csv";
			}
			
			

			if(queryfield.equals("cityOthers"))	
			{
				
				filename = "/root/city.csv";
			}
			
			
			if(queryfield.equals("isp"))	
			{
				
				filename = "/root/ISP.csv";
			}
			
			
			response.setContentType("text/csv");      
			response.setHeader("Content-Disposition", "attachment; filename="+filename+'"'); 
			 String report = reportService.extractReports(queryfield,metric,dateRange,campaignId,channel,aggregate,ratiometric,"true");
			 File initialFile = new File(filename);
			 InputStream is = new FileInputStream(initialFile);
		      // copy it to response's OutputStream
		      IOUtils.copy(is, response.getOutputStream());
		      response.flushBuffer();
		    } catch (IOException ex) {
		     
		    }
	}
	

	@RequestMapping(value = "/report/getCuberootCampaignIds", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@CrossOrigin
	public ResponseEntity<List<CampaignData>> getReport(@RequestParam("dateRange") String dateRange) {
	
		List<CampaignData> report = reportService.extractCuberootCampaignIds(dateRange);
		if (report == null) {
			
			return new ResponseEntity<List<CampaignData>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<CampaignData>>(report, HttpStatus.OK);
	}

	@RequestMapping(value = "/report/getCampaignIds", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@CrossOrigin
	public ResponseEntity<List<CampaignData>> getReport(@RequestParam("dateRange") String dateRange,@RequestParam("cuberootCampaignId") String cuberootCampaignId) {
	
		List<CampaignData> report = reportService.extractCampaignIds(dateRange,cuberootCampaignId);
		if (report == null) {
			
			return new ResponseEntity<List<CampaignData>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<CampaignData>>(report, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/report/getCampaignName", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@CrossOrigin
	public ResponseEntity<String> getReportCampaign(@RequestParam("campaignId") String campaignId) {
	
		String campaignName = reportService.extractCampaignName(campaignId);
        if (campaignName == null) {
			
			return new ResponseEntity<String>("",HttpStatus.OK);
		}
			
			return new ResponseEntity<String>(campaignName, HttpStatus.OK);
		
	}
	

	@RequestMapping(value = "/report/getCampaignCurrency", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@CrossOrigin
	public ResponseEntity<String> getCampaignCurrency(@RequestParam("campaignId") String campaignId) {
	
		String currency = reportService.extractCampaignCurrency(campaignId);
        currency = currency.replace("{\"share\":\"0\",\"scaledshare\":\"0\"}","").replace(',',' ').replace("\"share\":\"0\"","").replace("\"scaledshare\":\"0\"","");
		if (currency == null) {
			
			return new ResponseEntity<String>("",HttpStatus.OK);
		}
			
			return new ResponseEntity<String>(currency, HttpStatus.OK);
		
	}
	





}
