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

import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class implements a data reader that uses CSV files as input. Right now there
 * are only some stubs that need to be filled.
 * @author Matthias Keller
 *
 */
public class CsvDataReader extends AbstractDataReader {

    @Override
    public Boolean validateSignal(VizzlySignal signal) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Vector<TimedLocationValue> getSignalData(VizzlySignal signal,
            Long timeFilterStart, Long timeFilterEnd, int rowLimit)
            throws VizzlyException {
        // TODO Auto-generated method stub
        // Interpret signal
        // Read corresponding CSV file
        // Return data
        
        return null;
    }

}
