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

package ch.ethz.vizzly.datareader;

import java.util.Vector;

import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlyInvalidSignalException;
import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class defines the interface of a data reader.
 * @author Matthias Keller
 *
 */
public abstract class AbstractDataReader {

    abstract public void validateSignal(VizzlySignal signal) throws VizzlyInvalidSignalException;
    
    /**
     * This function is called for reading data from a data source.
     * @param signal The desired signal.
     * @param timeFilterStart Start time, null if not specified
     * @param timeFilterEnd End time, null if not specified
     * @param rowLimit Maximum number of rows to be returned, 0 if unlimited
     * @return Data from data source
     * @throws VizzlyException
     */
    abstract public Vector<TimedLocationValue> getSignalData(VizzlySignal signal, Long timeFilterStart, Long timeFilterEnd, int rowLimit)
            throws VizzlyException;
    
    public Boolean isPrivate() {
        return false;
    }
    
    public Boolean authenticate(String username, String password) {
        return true;
    }
    
    
}
