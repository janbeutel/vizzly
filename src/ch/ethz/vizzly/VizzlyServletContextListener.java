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

import java.util.Calendar;
import java.util.TimeZone;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import ch.ethz.vizzly.cache.CacheUpdateWorkerSynchronization;
import ch.ethz.vizzly.datatype.VizzlyException;

/**
 * This class takes care of background cache updater threads and the initialization
 * of state that must be persisted during the lifetime of the servlet
 * @author Matthias Keller
 *
 */
@WebListener 
public class VizzlyServletContextListener implements ServletContextListener {

    private final String CONFIG_FILE_NAME = "vizzly.xml";

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(VizzlyServletContextListener.class);
    
    public static final String INIT_ERROR_ATTRIB_KEY = "initError";

    public void contextInitialized(ServletContextEvent sce) {
        // Enable DNS caching, very useful when GSN data sources are used
        java.security.Security.setProperty("networkaddress.cache.ttl", "14400");
        
        // Set the default time zone to UTC
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        
        Boolean initError = false;
        try{
            
            // Open config XML
            String configPath = sce.getServletContext().getRealPath("/WEB-INF");
            VizzlyConfiguration vizzlyConfig = VizzlyConfiguration.fromXmlFile(configPath+"/"+CONFIG_FILE_NAME);
            
            // Setup of (pooled) database connection
            if(vizzlyConfig.useSqlDatabase()) {
                BasicDataSource ds = new BasicDataSource();
                ds.setDriverClassName(vizzlyConfig.getJdbcDriver());
                ds.setUsername(vizzlyConfig.getJdbcUser());
                ds.setPassword(vizzlyConfig.getJdbcPassword());
                ds.setUrl(vizzlyConfig.getJdbcUrl());
                ds.setTestOnBorrow(true);
                ds.setValidationQuery("SELECT 1");
                Context ctx = new InitialContext();
                ctx.bind("VizzlyDS", ds);
            }
     
            // Initialize aggregation level lookup - happens before caches are created
            AggregationLevelLookup.getInstance().init(vizzlyConfig.useSqlDatabase());
            
            // Initialize state object
            VizzlyStateContainer vizzlyState = new VizzlyStateContainer(vizzlyConfig);
            sce.getServletContext().setAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY, vizzlyState);
            
            // Initialize cache update workers
            CacheUpdateWorkerSynchronization workerSync = new CacheUpdateWorkerSynchronization(vizzlyState);
            sce.getServletContext().setAttribute(CacheUpdateWorkerSynchronization.SERVLET_ATTRIB_KEY, workerSync);
            workerSync.startUpdaterThreads(vizzlyConfig.getNumWorkerThreads());
           
            log.info("Vizzly started successfully.");

        } catch(VizzlyException e) {
            log.error(e.getLocalizedMessage());
            initError = true;
        } catch(NamingException e) {
            log.error(e.getLocalizedMessage());
            initError = true;
        } finally {
            // Tell the servlet to show an error 500
            sce.getServletContext().setAttribute(INIT_ERROR_ATTRIB_KEY, initError);
        }

    }

    public void contextDestroyed(ServletContextEvent sce){
        try {
            // Properly shutdown performance tracker persistence thread
            VizzlyStateContainer vizzlyState = (VizzlyStateContainer)sce.getServletContext()
                    .getAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY);
            vizzlyState.getPerformanceTracker().stopApplication();
            // Stop updater threads
            CacheUpdateWorkerSynchronization workerSync = (CacheUpdateWorkerSynchronization)sce
                    .getServletContext().getAttribute(CacheUpdateWorkerSynchronization.SERVLET_ATTRIB_KEY);
            workerSync.terminateThreads();
        } catch (Exception ex) {
        }
    }

}
