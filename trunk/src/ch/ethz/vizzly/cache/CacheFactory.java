/*
 * Copyright 2013 ETH Zurich, Computer Engineering and Networks Laboratory
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

package ch.ethz.vizzly.cache;

import ch.ethz.vizzly.cache.memory.MemCache;
import ch.ethz.vizzly.cache.sqldb.SqlDbCache;
import ch.ethz.vizzly.datatype.CacheSpec;
import ch.ethz.vizzly.datatype.VizzlyException;

/**
 * Class for generating cache instances.
 * @author Matthias Keller
 *
 */
public class CacheFactory {
    
    public static AbstractCache createCache(CacheSpec s) throws VizzlyException {
        if(s.type.equals(CacheSpec.CACHE_TYPE_SQL)) {
            return new SqlDbCache();
        } else if(s.type.equals(CacheSpec.CACHE_TYPE_MEM)) {
            return new MemCache();
        }
        throw new VizzlyException("Unknown cache type.");
    }    
    
}
