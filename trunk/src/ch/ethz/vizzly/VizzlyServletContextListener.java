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

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.cache.CacheUpdateWorkerSynchronization;

/**
 * This class takes care of background cache updater threads and the initialization
 * of state that must be persisted during the lifetime of the servlet
 * @author Matthias Keller
 *
 */
@WebListener 
public class VizzlyServletContextListener implements ServletContextListener {

    private final int NUM_UPDATE_WORKERS = 1;
    
    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(VizzlyServletContextListener.class);

    public void contextInitialized(ServletContextEvent sce) {
        // Initialize state object
        VizzlyStateContainer vizzlyState = new VizzlyStateContainer();
        sce.getServletContext().setAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY, vizzlyState);
        
        // Initialize cache
        CacheUpdateWorkerSynchronization workerSync = new CacheUpdateWorkerSynchronization(vizzlyState);
        sce.getServletContext().setAttribute("workerSync", workerSync);
        workerSync.startUpdaterThreads(NUM_UPDATE_WORKERS);
        
        log.info("Vizzly started successfully.");
    }

    public void contextDestroyed(ServletContextEvent sce){
        try {
            // Properly shutdown performance tracker persistence thread
            VizzlyStateContainer vizzlyState = (VizzlyStateContainer)sce.getServletContext().getAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY);
            vizzlyState.getPerformanceTracker().stopApplication();
            // Stop updater threads
            CacheUpdateWorkerSynchronization workerSync = (CacheUpdateWorkerSynchronization)sce.getServletContext().getAttribute("workerSync");
            workerSync.terminateThreads();
        } catch (Exception ex) {
        }
    }

}
