/*
 * Copyright 2012 ETH Zurich, Computer Engineering and Networks Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ethz.vizzly;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.annotation.WebServlet;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.cache.CacheManager;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.VizzlyInvalidSignalException;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.datatype.VizzlyView;
import ch.ethz.vizzly.performance.UserRequestPerformanceMeasurement;
import ch.ethz.vizzly.util.VizzlySignalValidatorUtil;

import com.google.gson.Gson;

/**
 * This class implements the servlet to which all HTTP requests are routed.
 * @author Matthias Keller
 *
 */
@WebServlet(urlPatterns={"/vizzly"})
public class VizzlyServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    
    private final int DEFAULT_CANVAS_WIDTH = 600;
    
    private final int DEFAULT_CANVAS_HEIGHT = 400;
    
    private final int MAX_CANVAS_WIDTH = 10000;
    
    private final int MAX_CANVAS_HEIGHT = 6000;

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(VizzlyServlet.class);

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        processRequest(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        processRequest(req, resp);
    }

    private void processRequest(HttpServletRequest req, 
            HttpServletResponse resp) throws IOException {

        UserRequestPerformanceMeasurement reqMeas = new UserRequestPerformanceMeasurement();
        Long timeFilterStart = null;
        Long timeFilterEnd = null;
        Double latSW = null;
        Double lngSW = null;
        Double latNE = null;
        Double lngNE = null;
        int signalIdx = -1;
        int canvasWidth = 0;
        int canvasHeight = 0;
        // HTTP request parameters
        // Parameters that correspond to actions
        String statsParam = req.getParameter("stats");
        String aggMapParam = req.getParameter("aggMap");
        String heatMapParam = req.getParameter("heatMap");
        // Signal selection 
        String viewConfigParam = req.getParameter("viewConfig");
        // Time selection
        String timeStartParam = req.getParameter("timeStart");
        String timeEndParam = req.getParameter("timeEnd");
        // Map area of interest
        String mapBoundsParam = req.getParameter("mapBounds");
        // Signal to be displayed on the map (one at a time)
        String signalIdxParam = req.getParameter("signalIdx");
        // Dimension of canvas object used for displaying requested contents
        String canvasWidthParam = req.getParameter("canvasWidth");
        String canvasHeightParam = req.getParameter("canvasHeight");
        
        // JSON string from HTTP request
        StringBuffer jsonReq = new StringBuffer();
        String line = null;
        try {
            BufferedReader reader = req.getReader();
            while ((line = reader.readLine()) != null)
                jsonReq.append(line);
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }

        // Convert JSON into Java object
        Gson gson = new Gson();
        VizzlyView viewConfig = null;
        if(viewConfigParam != null) {
            // From GET variable
            viewConfig = gson.fromJson(viewConfigParam, VizzlyView.class);
        } else {
            // From POST contents
            viewConfig = gson.fromJson(jsonReq.toString(), VizzlyView.class);
        }

        if(viewConfig != null) {
            try {
                VizzlyStateContainer vizzlyState = 
                        (VizzlyStateContainer)getServletContext().getAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY);
                for(VizzlySignal s : viewConfig.getVisibleSignals()) {
                    VizzlySignalValidatorUtil.validateSignal(s, vizzlyState.getDataReaderRegistry());
                }
            } catch(VizzlyInvalidSignalException e) {
                returnErrorMessage(e.getLocalizedMessage(), resp);
                return;
            }
        }

        if(timeStartParam != null) {
            timeFilterStart = Long.parseLong(timeStartParam);
        }
        if(timeEndParam != null) {
            timeFilterEnd = Long.parseLong(timeEndParam);
        }
        
        if(mapBoundsParam != null) {
            String[] s = mapBoundsParam.split(",");
            latSW = Double.parseDouble(s[0]);
            lngSW = Double.parseDouble(s[1]);
            latNE = Double.parseDouble(s[2]);
            lngNE = Double.parseDouble(s[3]);
        }
        
        if(signalIdxParam != null) {
            signalIdx = Integer.parseInt(signalIdxParam);
        }
        
        canvasWidth = DEFAULT_CANVAS_WIDTH;
        canvasHeight = DEFAULT_CANVAS_HEIGHT;
        if(canvasWidthParam != null) {
            canvasWidth = Integer.parseInt(canvasWidthParam);
            if(canvasWidth > MAX_CANVAS_WIDTH) {
                canvasWidth = MAX_CANVAS_WIDTH;
            }
        }
        if(canvasHeightParam != null) {
            canvasHeight = Integer.parseInt(canvasHeightParam);
            if(canvasHeight > MAX_CANVAS_HEIGHT) {
                canvasHeight = MAX_CANVAS_HEIGHT;
            }
        }

        if(aggMapParam != null) {
            // Respond with map grid data
            getAggregationMapCSV(viewConfig, timeFilterStart, timeFilterEnd, latSW, lngSW, latNE, lngNE, signalIdx, canvasWidth, canvasHeight, resp, reqMeas);
        } else if(heatMapParam != null) {
            // Respond with a transparent PNG that displays a heat map
            getHeatMap(resp);
        } else if(statsParam != null) {
            // Respond with performance statistics
            showPerformanceStats(resp);
        } else if(viewConfig != null) { 
            // Respond with a time series
            getTimedDataCSV(viewConfig, timeFilterStart, timeFilterEnd, latSW, lngSW, latNE, lngNE, signalIdx, canvasWidth, resp, reqMeas);
        } else {
            returnErrorMessage("Invalid request parameters.", resp);
        }
    }

    private void getTimedDataCSV(VizzlyView viewConfig, Long timeFilterStart, Long timeFilterEnd, Double latSW, 
            Double lngSW, Double latNE, Double lngNE, int signalIdx, int canvasWidth, HttpServletResponse resp, 
            UserRequestPerformanceMeasurement reqMeas)
            throws IOException
            {
        VizzlyStateContainer vizzlyState = 
                (VizzlyStateContainer)getServletContext().getAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY);
        // Respond with CSV
        String output = "";
        try {
            output = CsvOutputGenerator.getTimedDataCSV(viewConfig, timeFilterStart, timeFilterEnd, latSW, 
                    lngSW, latNE, lngNE, signalIdx, canvasWidth, reqMeas, vizzlyState.getCacheManager(),
                    vizzlyState.getPerformanceTracker(), vizzlyState.getDataReaderRegistry());
        } catch(VizzlyException e) {
            returnErrorMessage(e.getLocalizedMessage(), resp);
            return;
        }
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setContentType("text/csv; charset=UTF-8");
        ServletOutputStream outputStream = resp.getOutputStream();
        outputStream.write(output.getBytes("UTF-8"));
        reqMeas.setEnd();
        vizzlyState.getPerformanceTracker().addUserRequestMeasurement(reqMeas);
        vizzlyState.incrNumberOfRequests();
            }

    private void getAggregationMapCSV(VizzlyView viewConfig, Long timeFilterStart, Long timeFilterEnd, Double latSW, 
            Double lngSW, Double latNE, Double lngNE, int signalIdx, int canvasWidth, int canvasHeight, HttpServletResponse resp,
            UserRequestPerformanceMeasurement reqMeas)
                    throws IOException
                    {
        VizzlyStateContainer vizzlyState = 
                (VizzlyStateContainer)getServletContext().getAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY);
        // Respond with CSV
        String output = "";
        try {
            output = CsvOutputGenerator.getAggregationMapCSV(viewConfig, timeFilterStart, timeFilterEnd, 
                    latSW, lngSW, latNE, lngNE, signalIdx, canvasWidth, canvasHeight, reqMeas, vizzlyState.getCacheManager(),
                    vizzlyState.getPerformanceTracker(), vizzlyState.getDataReaderRegistry());
        } catch(VizzlyException e) {
            returnErrorMessage(e.getLocalizedMessage(), resp);
            return;
        }
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setContentType("text/csv; charset=UTF-8");
        ServletOutputStream outputStream = resp.getOutputStream();
        outputStream.write(output.getBytes("UTF-8"));
        reqMeas.setEnd();
        vizzlyState.getPerformanceTracker().addUserRequestMeasurement(reqMeas);
        vizzlyState.incrNumberOfRequests();
                    }

    private void showPerformanceStats(HttpServletResponse resp) throws IOException
    {
        StringBuffer sb = new StringBuffer();
        VizzlyStateContainer vizzlyState = 
                (VizzlyStateContainer)getServletContext().getAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY);
        CacheManager cache = vizzlyState.getCacheManager();
        
        sb.append("uptime="+cache.getUptime(0));
        sb.append(",");
        sb.append("requests="+vizzlyState.getNumberOfRequests());
        sb.append(",");
        sb.append("cacheFileSize="+cache.getCacheSize(0));
        sb.append(",");
        sb.append("seenSignals="+cache.getNumberOfSeenSignals(0));
        sb.append(",");
        sb.append("cacheEntries="+cache.getNumberOfCacheEntries(0));
        sb.append(",");
        sb.append("cacheRequests="+cache.getNumberOfCacheRequests(0));
        sb.append(",");
        sb.append("cacheHits="+cache.getNumberOfCacheHits(0));
        sb.append(",");
        sb.append("cacheMisses="+cache.getNumberOfCacheMisses(0));

        resp.setContentType("text/plain; charset=UTF-8");
        ServletOutputStream outputStream = resp.getOutputStream();
        outputStream.write(sb.toString().getBytes("UTF-8"));
    }

    private void getHeatMap(HttpServletResponse resp) throws IOException {
        OverlayHeatMap heatMap = new OverlayHeatMap();
        BufferedImage bufferedImage = heatMap.getImage();
        resp.setContentType("image/png");
        ServletOutputStream outputStream = resp.getOutputStream();
        ImageIO.write(bufferedImage, "png", outputStream);
    }
    
    private void returnErrorMessage(String errorMsg, HttpServletResponse resp) throws IOException
    {
        resp.setContentType("text/plain; charset=UTF-8");
        ServletOutputStream outputStream = resp.getOutputStream();
        String error = "# ERROR: " + errorMsg + "\n";
        outputStream.write(error.toString().getBytes("UTF-8"));
    }

}
