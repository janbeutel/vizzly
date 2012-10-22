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

package ch.ethz.vizzly.cache.mysql;

import java.util.Date;

import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class implements a data type that is used by the MySQL cache for internally
 * managing its meta data.
 * @author Matthias Keller
 *
 */
public class MySqlDbCacheMetaEntry {
    
    public VizzlySignal signal = null;
    public int windowLengthSec = 0;
    public Long startTime = null;
    public Long endTime = null;
    public Long firstPacketTimestamp = null;
    public Long lastPacketTimestamp = null;
    public int hits = 0;
    public Date lastUpdate = null;
    public int numElements = 0;
    public Boolean hasLocationData = false;;

}
