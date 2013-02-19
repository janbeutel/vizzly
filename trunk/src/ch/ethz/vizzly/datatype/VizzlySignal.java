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

package ch.ethz.vizzly.datatype;

import java.io.Serializable;

import org.apache.log4j.Logger;

/**
 * This class implements a data type that represents a requested signal. In contrast to
 * JsonVizzlyView, this data type implements equals() etc. functions.
 * @author Matthias Keller
 *
 */
public class VizzlySignal implements Serializable {

    /**
     * This class implements a data type that represents a data source.
     * @author Matthias Keller
     *
     */
    public static class DataSource implements Serializable {
        private static final long serialVersionUID = 1L;
        public String type;
        public String name;
        public String serverAddress; /** optional */
        
        /**
         * Log.
         */
        private static Logger log = Logger.getLogger(VizzlySignal.DataSource.class);
        
        public DataSource(String type, String name, String serverAddress) {
            this.type = type;
            this.name = name;
            this.serverAddress = serverAddress;
        }
        
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof DataSource)) return false;
            DataSource otherDs = (DataSource)other;

            if(!type.equals(otherDs.type)) return false;
            if(!name.equals(otherDs.name)) return false;
            if(serverAddress != null && otherDs.serverAddress == null) return false;
            if(serverAddress == null && otherDs.serverAddress != null) return false;

            if(serverAddress != null) {
                try {
                    ServerSpec thisServer = ServerSpec.fromAddress(serverAddress);
                    ServerSpec otherServer = ServerSpec.fromAddress(otherDs.serverAddress);
                    if(!thisServer.equals(otherServer)) return false;
                } catch(VizzlyException e) {
                    return false;
                }
            }
            return true;
        }
        
        public int hashCode() {
            int hash = type.hashCode() + 10*name.hashCode();
            if(serverAddress != null) {
                try {
                    hash += 40*ServerSpec.fromAddress(serverAddress).hashCode();
                } catch(VizzlyException e) {
                    // Cannot help it here
                    log.error(e.getLocalizedMessage(), e);
                }
            }
            return hash;
        }

    }

    /**
     * This class implements a data type that represents a device selection.
     * @author Matthias Keller
     *
     */
    public static class DeviceSelect implements Serializable {
        private static final long serialVersionUID = 1L;
        public String type; /** either 'single' or 'all' */
        public String field; /** empty if 'all' */
        public String value; /** empty if 'all' */

        public DeviceSelect(String type, String field, String value) {
            this.type = type;
            this.field = field;
            this.value = value;
        }
        
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof DeviceSelect)) return false;
            DeviceSelect otherSelect = (DeviceSelect)other;
            if (!type.equals(otherSelect.type)) return false;
            if (field == null && otherSelect.field != null) return false;
            if (field != null && otherSelect.field == null) return false;
            if (field != null && !field.equals(otherSelect.field)) return false;
            if (value == null && otherSelect.value != null) return false;
            if (value != null && otherSelect.value == null) return false;
            if (value != null && !value.equals(otherSelect.value)) return false;

            return true;
        }

        public int hashCode() {
            int hash = type.hashCode();
            if(field != null) hash += field.hashCode()*10;
            if(value != null) hash += value.hashCode()*100;
            return hash;
        }
    }

    private static final long serialVersionUID = 1L;

    public String displayName;
    public DataSource dataSource;
    public String dataField;
    public DeviceSelect deviceSelect;
    public String timeField;
    public String locationLatField;
    public String locationLngField;
    public String aggFunction; // not implemented yet
    public Double scaling;
    public Boolean visible;

    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VizzlySignal)) return false;

        VizzlySignal otherSignal = (VizzlySignal)other;
        if(!dataField.equals(otherSignal.dataField)) return false;
        if(!deviceSelect.equals(otherSignal.deviceSelect)) return false;
        if(!timeField.equals(otherSignal.timeField)) return false;
        if(!dataSource.equals(otherSignal.dataSource)) return false;
        
        if(locationLatField == null && otherSignal.locationLatField != null) return false;
        if(locationLatField != null && otherSignal.locationLatField == null) return false;
        if(locationLatField != null && !locationLatField.equals(otherSignal.locationLatField)) return false;
        
        if(locationLngField == null && otherSignal.locationLngField != null) return false;
        if(locationLngField != null && otherSignal.locationLngField == null) return false;
        if(locationLngField != null && !locationLngField.equals(otherSignal.locationLngField)) return false;
        
        return true;
    }

    public int hashCode() {
        int hash = deviceSelect.hashCode()+dataSource.hashCode()*10+dataField.hashCode()*5+timeField.hashCode();
        if(locationLatField != null) {
            hash += locationLatField.hashCode()*2;
        }
        if(locationLngField != null) {
            hash += locationLngField.hashCode()*3;
        }
        return hash;
    }

    public String getUniqueIdentifier() {
        String dataSourceStr = dataSource.type+":/";
        if(dataSource.serverAddress != null && !dataSource.serverAddress.equals("")) {
            dataSourceStr += dataSource.serverAddress+"/";
        }
        dataSourceStr += dataSource.name;
        
        String selectStr = deviceSelect.type;
        if(deviceSelect.type.equals("single")) {
            selectStr += ","+deviceSelect.field;
            selectStr += ","+deviceSelect.value;
        }
        
        String locationStr = (hasLocation()) ? ";loc" : "";
        
        return dataSourceStr+";"+dataField+";"+selectStr+";"+timeField+locationStr;
    }
    
    public boolean hasLocation() {
        return (locationLatField != null && !locationLatField.equals(""));
    }

}
