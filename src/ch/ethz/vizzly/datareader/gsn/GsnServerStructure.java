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

package ch.ethz.vizzly.datareader.gsn;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import ch.ethz.vizzly.datatype.ServerSpec;

/**
 * This class implements a data type that represents the structure of a GSN
 * server instance. This structure includes the list of available virtual sensors
 * including the fields that each of those sensors provides.
 * @author Matthias Keller
 *
 */
public class GsnServerStructure {

    private static Logger log = Logger.getLogger(GsnServerStructure.class);
    
    private HashMap<String,Vector<String>> _structure = null;
    
    public GsnServerStructure() {
        _structure = new HashMap<String,Vector<String>>();
    }
    
    public Set<String> getVirtualSensors() {
        return _structure.keySet();
    }
    
    public Vector<String> getFields(String virtualSensor) {
        return _structure.get(virtualSensor);
    }
    
    public void addVirtualSensor(String virtualSensor, Vector<String> fields) {
        _structure.put(virtualSensor, fields);
    }
    
    public static GsnServerStructure fromGsnServer(ServerSpec serverSpec) {
        GsnServerStructure structure = new GsnServerStructure();
        try {

            log.debug("Fetching GSN structure from " + serverSpec.serverIp.getHostName() + ":" 
                    + Integer.valueOf(serverSpec.serverPort).toString());
            
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();

            Document doc = docBuilder.parse("http://" + serverSpec.serverIp.getHostName() + ":" 
                    + Integer.valueOf(serverSpec.serverPort).toString() + "/gsn?structure");

            // normalize text representation
            doc.getDocumentElement ().normalize();
            NodeList virtualSensors = doc.getElementsByTagName("virtual-sensor");

            for(int i = 0; i < virtualSensors.getLength(); i++) {

                String sensorName = null;
                Vector<String> sensorFields = new Vector<String>();
                NamedNodeMap sensorAttrib = virtualSensors.item(i).getAttributes();
                for(int j = 0; j < sensorAttrib.getLength(); j++) {
                    Node n = sensorAttrib.item(j);
                    if(n.getNodeName().equals("name")) {
                        sensorName = n.getNodeValue();
                        break;
                    }
                }
                NodeList fields = virtualSensors.item(i).getChildNodes();
                for(int k = 0; k < fields.getLength(); k++) {
                    if(fields.item(k).getNodeName().equals("field")) {
                        NamedNodeMap fieldAttrib = fields.item(k).getAttributes();
                        for(int j = 0; j < fieldAttrib.getLength(); j++) {
                            Node n = fieldAttrib.item(j);
                            if(n.getNodeName().equals("name")) {
                                sensorFields.add(n.getNodeValue());
                                break;
                            }
                        }
                    }
                }
                structure.addVirtualSensor(sensorName, sensorFields);
            }
        } catch (SAXParseException err) {
            log.error("Parsing error", err);
        } catch (SAXException e) {
            log.error(e);
        } catch (Throwable t) {
            log.error(t);
        }

        return structure;
    }
    
}
