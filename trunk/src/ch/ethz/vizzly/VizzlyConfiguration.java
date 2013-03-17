/*
 * Copyright 2013 ETH Zurich, Computer Engineering and Networks Laboratory
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

import java.io.File;
import java.io.IOException;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import ch.ethz.vizzly.datatype.CacheSpec;
import ch.ethz.vizzly.datatype.VizzlyException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * This class is used for loading a Vizzly configuration from an XML file and to 
 * make parameters set accessible to other objects.
 * @author Matthias Keller
 *
 */
public class VizzlyConfiguration {

    public static final String SERVLET_ATTRIB_KEY = "vizzlyConfig";
    
    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(VizzlyConfiguration.class);

    /**
     * Number of background threads that continuously poll data sources
     */
    private int numWorkerThreads = 1;

    /**
     * Enable/disable the performance tracker module that logs performance
     * data to a SQL database.
     */
    private Boolean enablePerformanceTracker = false;

    private String databaseJdbcDriver = null;

    private String databaseJdbcUrl = null;

    private String databaseJdbcUser = null;
    
    private String databaseJdbcPassword = null;

    private Vector<CacheSpec> cacheList = null;
    
    private static final String TAG_NAME_CACHE_LIST = "caches";
    
    private static final String TAG_NAME_SQL_CACHE = "sqlDbCache";
    
    private static final String TAG_NAME_MEM_CACHE = "memoryCache";
    
    private static final String TAG_NAME_DATABASE = "database";
    
    private static final String ATTR_NAME_NUM_WORKERS = "numWorkerThreads";
    
    private static final String ATTR_NAME_EN_PERF_TRACKER = "enablePerformanceTracker";
    
    private static final String ATTR_NAME_JDBC_DRIVER = "jdbcDriver";
    
    private static final String ATTR_NAME_JDBC_URL = "jdbcUrl";
    
    private static final String ATTR_NAME_JDBC_USER = "jdbcUser";
    
    private static final String ATTR_NAME_JDBC_PASSWORD = "jdbcPassword";
    
    private static final String ATTR_NAME_CACHE_WINDOW_LENGTH = "windowLengthSec";
    
    private VizzlyConfiguration() {
        cacheList = new Vector<CacheSpec>();
    }

    public Boolean isPerformanceTrackerEnabled() {
        return enablePerformanceTracker;
    }

    public int getNumWorkerThreads() {
        return numWorkerThreads;
    }
 
    public Boolean useSqlDatabase() {
        return !databaseJdbcUrl.isEmpty();
    }
    
    public String getJdbcDriver() {
        return databaseJdbcDriver;
    }

    public String getJdbcUrl() {
        return databaseJdbcUrl;
    }

    public String getJdbcUser() {
        return databaseJdbcUser;
    }
    
    public String getJdbcPassword() {
        return databaseJdbcPassword;
    }
    
    public Vector<CacheSpec> getCacheList() {
        return cacheList;
    }

    public static VizzlyConfiguration fromXmlFile(String configFileUri) throws VizzlyException {
        VizzlyConfiguration config = new VizzlyConfiguration();

        try {

            File configFile = new File(configFileUri);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(configFile);
            doc.getDocumentElement().normalize();

            Element e = doc.getDocumentElement();

            // Parse general properties
            config.numWorkerThreads = 
                    Integer.parseInt(e.getAttribute(VizzlyConfiguration.ATTR_NAME_NUM_WORKERS));

            config.enablePerformanceTracker = 
                    Boolean.parseBoolean(e.getAttribute(VizzlyConfiguration.ATTR_NAME_EN_PERF_TRACKER));

            // Parse database properties
            NodeList database = e.getElementsByTagName(VizzlyConfiguration.TAG_NAME_DATABASE);
            Node dbElement = null;
            if((dbElement = database.item(0)) != null) {
                if(dbElement instanceof Element) {
                    config.databaseJdbcDriver = ((Element)dbElement).getAttribute(VizzlyConfiguration.ATTR_NAME_JDBC_DRIVER);
                    config.databaseJdbcUrl = ((Element)dbElement).getAttribute(VizzlyConfiguration.ATTR_NAME_JDBC_URL);
                    config.databaseJdbcUser = ((Element)dbElement).getAttribute(VizzlyConfiguration.ATTR_NAME_JDBC_USER);
                    config.databaseJdbcPassword = ((Element)dbElement).getAttribute(VizzlyConfiguration.ATTR_NAME_JDBC_PASSWORD);
                }
            }

            // Parse caches used
            NodeList caches = e.getElementsByTagName(VizzlyConfiguration.TAG_NAME_CACHE_LIST);
            Node cachesElement = null;
            if((cachesElement = caches.item(0)) != null) {
                NodeList cacheNodeList = cachesElement.getChildNodes();
                for(int i=0; i<cacheNodeList.getLength(); i++) {
                    CacheSpec c = new CacheSpec();
                    if(cacheNodeList.item(i) instanceof Element) {
                        Element cacheElement = ((Element)cacheNodeList.item(i));
                        if(cacheElement.getTagName().equals(VizzlyConfiguration.TAG_NAME_MEM_CACHE)) {
                            c.type = CacheSpec.CACHE_TYPE_MEM;
                        } else if(cacheElement.getTagName().equals(VizzlyConfiguration.TAG_NAME_SQL_CACHE)) {
                            c.type = CacheSpec.CACHE_TYPE_SQL;
                        } else {
                            // Should never happen
                            throw new VizzlyException("Unknown cache type in configuration file.");
                        }
                        c.windowLength = Integer.parseInt(cacheElement.getAttribute(VizzlyConfiguration.ATTR_NAME_CACHE_WINDOW_LENGTH));
                    }
                    config.cacheList.add(c);
                }
            }

            return config;

        } catch(IOException e) {
            log.error(e.getLocalizedMessage());
            throw new VizzlyException(e.getLocalizedMessage());
        } catch(ParserConfigurationException e) {
            log.error(e.getLocalizedMessage());
            throw new VizzlyException(e.getLocalizedMessage());
        } catch(SAXException e) {
            log.error(e.getLocalizedMessage());
            throw new VizzlyException(e.getLocalizedMessage());
        } catch(NumberFormatException e) {
            throw new VizzlyException(e.getLocalizedMessage());
        }

    }

}
