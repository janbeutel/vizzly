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
import ch.ethz.vizzly.datatype.VizzlyInvalidSignalException;
import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class provides functions for validating Vizzly signals that are specified by
 * users and that may potentially contain errors.
 * @author Matthias Keller
 *
 */
public class VizzlySignalValidatorUtil {

    public static void validateSignal(VizzlySignal signal, DataReaderRegistry readerRegistry) throws VizzlyInvalidSignalException {

        if(signal.dataSource == null) throw new VizzlyInvalidSignalException("Could not parse signal: Data source is missing.");
        if(signal.deviceSelect == null) throw new VizzlyInvalidSignalException("Could not parse signal: Device selection is missing.");
        if(signal.dataSource.type == null) throw new VizzlyInvalidSignalException("Could not parse signal: Data source spec is incomplete.");
        if(signal.dataSource.name == null || signal.dataSource.name.equals("")) throw new VizzlyInvalidSignalException("Could not parse signal: Data source spec is incomplete.");
        
        if(signal.dataField == null || signal.dataField.equals("")) throw new VizzlyInvalidSignalException("Could not parse signal: dataField is missing.");
        if(signal.timeField == null || signal.timeField.equals("")) throw new VizzlyInvalidSignalException("Could not parse signal: timeField is missing.");
        
        if(readerRegistry.getDataReader(signal.dataSource.type) == null) throw new VizzlyInvalidSignalException("Could not parse signal: Unknown data source type.");
        // Also throws an exception on error
        readerRegistry.getDataReader(signal.dataSource.type).validateSignal(signal);
        
        if(signal.deviceSelect.type == null) throw new VizzlyInvalidSignalException("Could not parse signal: Device selection is incomplete.");
        if(!signal.deviceSelect.type.equals("single") && !signal.deviceSelect.type.equals("all")) throw new VizzlyInvalidSignalException("Could not parse signal: Device selection is invalid.");
        if(signal.deviceSelect.type.equals("single")) {
            if(signal.deviceSelect.field == null || signal.deviceSelect.field.equals("")) throw new VizzlyInvalidSignalException("Could not parse signal: Device selection is incomplete.");
            if(signal.deviceSelect.value == null || signal.deviceSelect.value.equals("")) throw new VizzlyInvalidSignalException("Could not parse signal: Device selection is incomplete.");
        }
        
        if(signal.locationLatField != null && signal.locationLngField == null) throw new VizzlyInvalidSignalException("Could not parse signal: Location field spec is incomplete.");
        if(signal.locationLngField != null && signal.locationLatField == null) throw new VizzlyInvalidSignalException("Could not parse signal: Location field spec is incomplete.");
        
        return;
    }

}
