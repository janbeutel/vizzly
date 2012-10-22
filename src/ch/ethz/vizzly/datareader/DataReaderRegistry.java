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

import java.util.HashMap;

/**
 * This class implements a mapping table that returns the corresponding
 * instance of a data reader for a given key, e.g. "gsn"
 * @author Matthias Keller
 *
 */
public class DataReaderRegistry {

    private HashMap<String,AbstractDataReader> dataReaders = null;
    
    public DataReaderRegistry() {
        dataReaders = new HashMap<String,AbstractDataReader>();
    }
    
    public void addDataReader(String key, AbstractDataReader dataReader) {
        dataReaders.put(key, dataReader);
    }
    
    public AbstractDataReader getDataReader(String key) {
        AbstractDataReader dataReader = dataReaders.get(key);
        return dataReader;
    }
    
    
}
