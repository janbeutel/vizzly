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

import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.ethz.vizzly.datareader.AbstractDataReader;
import ch.ethz.vizzly.datatype.ServerSpec;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class distributed GSN read requests to the respective GSN servers.
 * @author Matthias Keller
 *
 */
public class GsnDataReader extends AbstractDataReader {

    private GsnFetcherInstances fetcherList = null;

    public GsnDataReader() {
        fetcherList = new GsnFetcherInstances();
    }

    public Boolean validateSignal(VizzlySignal signal) {
        if(signal.dataSource.serverAddress == null) return false;
        Pattern p = Pattern.compile("[a-zA-Z0-9\\.\\-:]+"); 
        Matcher m = p.matcher(signal.dataSource.serverAddress); 
        return m.matches();
    }

    public Vector<TimedLocationValue> getSignalData(VizzlySignal signal, Long timeFilterStart, Long timeFilterEnd, int rowLimit)
            throws VizzlyException {
        // Find fetcher for server on IP:port
        GsnMultiDataFetcher fetcher = fetcherList.getMultiDataFetcher(ServerSpec.fromAddress(signal.dataSource.serverAddress));
        return fetcher.getDataFromSource(signal, timeFilterStart, timeFilterEnd, rowLimit);
    }

}
