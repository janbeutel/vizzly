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

package ch.ethz.vizzly.util;

import ch.ethz.vizzly.datareader.DataReaderRegistry;
import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class provides functions for validating Vizzly signals that are specified by
 * users and that may potentially contain errors.
 * @author Matthias Keller
 *
 */
public class VizzlySignalValidatorUtil {

    public static boolean validateSignal(VizzlySignal signal, DataReaderRegistry readerRegistry) {

        if(signal.dataSource == null) return false;
        if(signal.deviceSelect == null) return false;
        if(signal.dataSource.type == null) return false;
        
        if(signal.dataField == null || signal.dataField.equals("")) return false;
        if(signal.timeField == null || signal.timeField.equals("")) return false;
        
        if(readerRegistry.getDataReader(signal.dataSource.type) == null) return false;
        if(!readerRegistry.getDataReader(signal.dataSource.type).validateSignal(signal)) return false;
        
        if(signal.dataSource.name == null || signal.dataSource.name.equals("")) return false;
        
        if(signal.deviceSelect.type == null) return false;
        if(!signal.deviceSelect.type.equals("single") && !signal.deviceSelect.type.equals("all")) return false;
        if(signal.deviceSelect.type.equals("single")) {
            if(signal.deviceSelect.field == null || signal.deviceSelect.field.equals("")) return false;
            if(signal.deviceSelect.value == null || signal.deviceSelect.value.equals("")) return false;
        }
        
        return true;
    }

}
