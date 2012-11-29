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


/**
 * This class implements a data type that is used for determining the next signal that is
 * going to be updated by the cache updaters.
 * @author Matthias Keller
 *
 */
public class VizzlySignalCurrentness implements Comparable<VizzlySignalCurrentness> {

    public VizzlySignal signal = null;
    
    public Long lastUpdateAttempt = null;
    
    public VizzlySignalCurrentness(VizzlySignal signal, Long lastTriedUpdate) {
        this.signal = signal;
        this.lastUpdateAttempt = lastTriedUpdate;
    }

    public int compareTo(VizzlySignalCurrentness o) {
        return lastUpdateAttempt.compareTo(o.lastUpdateAttempt);
    }

    
}
