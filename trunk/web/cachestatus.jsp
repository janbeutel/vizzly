<%@ page import="ch.ethz.vizzly.VizzlyStateContainer,ch.ethz.vizzly.cache.*,ch.ethz.vizzly.datatype.*,java.util.*,java.text.SimpleDateFormat,java.text.DecimalFormat,java.lang.StringBuffer" %>
<html><head>
<title>Vizzly Cache Status</title>
<style type="text/css">
h3 { font-family: Verdana, Helvetica, sans-serif; font-size:14px; }
body, p, td { font-family: Verdana, Helvetica, sans-serif; font-size:12px; }
a.white { color:#ffffff; }
#footer { font-style: italic; }
</style>
</head>
<body bgcolor="#FFFFFF">
<%
VizzlyStateContainer vizzlyState = 
        (VizzlyStateContainer)application.getAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY);
CacheManager cacheManager = vizzlyState.getCacheManager();

// Generate text representation of current uptime
long uptime = cacheManager.getUptime(cacheManager.getNumberOfCaches()-1);
int SECOND = 1000;
int MINUTE = 60 * SECOND;
int HOUR = 60 * MINUTE;
int DAY = 24 * HOUR;
StringBuffer uptimeText = new StringBuffer("");
uptimeText.append(uptime / DAY).append(" days ");
uptime %= DAY;
uptimeText.append(uptime / HOUR).append(" hours ");
uptime %= HOUR;
uptimeText.append(uptime / MINUTE).append(" minutes ");
uptime %= MINUTE;
uptimeText.append(uptime / SECOND).append(" seconds ");
uptime %= SECOND;

%>
<h3>Cache Status</h3>
<p>
<table style="border: 0px;">
<tr><td width="250">Uptime:</td><td><%=uptimeText.toString()%></tr>
<%
for(int i = 0; i < cacheManager.getNumberOfCaches(); i++) {
    double cacheSizeMb = (double)cacheManager.getCacheSize(i)/1024/1024;
    DecimalFormat df = new DecimalFormat("#.###");
%>
<tr><td><%=cacheManager.getCacheDescription(i)%> Cache Size:</td><td><%=df.format(cacheSizeMb)%> MByte</tr>
<tr><td><%=cacheManager.getCacheDescription(i)%> # of Cache Entries:</td><td><%=cacheManager.getNumberOfCacheEntries(i)%></tr>
<tr><td><%=cacheManager.getCacheDescription(i)%> # of Seen Signals:</td><td><%=cacheManager.getNumberOfSeenSignals(i)%></tr>
<tr><td><%=cacheManager.getCacheDescription(i)%> # of Requests:</td><td><%=cacheManager.getNumberOfCacheRequests(i)%></tr>
<tr><td><%=cacheManager.getCacheDescription(i)%> # of Hits:</td><td><%=cacheManager.getNumberOfCacheHits(i)%></tr>
<tr><td><%=cacheManager.getCacheDescription(i)%> # of Misses:</td><td><%=cacheManager.getNumberOfCacheMisses(i)%></tr>
<%
}
%>
</table>
</p>
<%
// Process signal removal requests
if(request.getParameter("remove") != null) {
    for(VizzlySignal s : cacheManager.getSignals(cacheManager.getNumberOfCaches()-1)) {
        if(s.getUniqueIdentifier().equals(request.getParameter("remove"))) {
            cacheManager.scheduleSignalForRemoval(s);
        }   
    }
}

if(cacheManager.getSignalsToRemove().size() > 0) {
%>
<h3>Pending Signals Waiting for Removal</h3>
<p>
<%
    for(VizzlySignal s : cacheManager.getSignalsToRemove()) {
%>
<%=s.getUniqueIdentifier()%><br/>
<%
    }
%>
</p>
<%
}

// Sorting
Comparator<CachedDataInfo> comp = null;
if(request.getParameter("s") == null || request.getParameter("s").equals("NAME")) {
    comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.NAME_ASCENDING);
} else if(request.getParameter("s").equals("WINDOW_LENGTH")) {
    comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.WINDOW_LENGTH_DESCENDING);
} else if(request.getParameter("s").equals("NUM_ELEMENTS")) {
    comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.NUM_ELEMENTS_DESCENDING);
} else if(request.getParameter("s").equals("HAS_LOCATION_DATA")) {
    comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.HAS_LOCATION_DATA_DESCENDING);
} else if(request.getParameter("s").equals("LAST_PACKET_TIMESTAMP")) {
    comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.LAST_PACKET_TIMESTAMP_DESCENDING);
} else if(request.getParameter("s").equals("LAST_UPDATE")) {
    comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.LAST_UPDATE_DESCENDING);
} else if(request.getParameter("s").equals("HITS")) {
    comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.HITS_DESCENDING);
} else {
    // Should actually never happen, but users can be funny ...
    comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.NAME_ASCENDING);
}

SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

for(int i = 0; i < cacheManager.getNumberOfCaches(); i++) {
%>
<h3><%=cacheManager.getCacheDescription(i)%> Cache Contents</h3>
<table style="border: 1px solid #000000;">
<tr style="background-color: #000000; color: #ffffff; font-weight: bold">
    <td><a class="white" href="?s=NAME">SIGNAL NAME</a></td>
    <td><a class="white" href="?s=WINDOW_LENGTH">WIN LEN.</a></td>
    <td><a class="white" href="?s=NUM_ELEMENTS"># ROWS</a></td>
    <td><a class="white" href="?s=HAS_LOCATION_DATA">LOC</a></td>
    <td width="160"><a class="white" href="?s=LAST_PACKET_TIMESTAMP">LAST TIMESTAMP</a></td>
    <td width="160"><a class="white" href="?s=LAST_UPDATE">LAST UPDATE</a></td>
    <td><a class="white" href="?s=HITS">HITS</a></td>
    <td>DEL</td>
</tr>
<%
    Vector<CachedDataInfo> cacheInfo = cacheManager.getCachedDataInfo(i);
    long totalElements = 0;
    Collections.sort(cacheInfo, comp);

    for (CachedDataInfo d : cacheInfo) {
        totalElements += (d.hasLocationData) ? d.numElements*24 : d.numElements*8;
        String removeLink = "?remove=" + d.signal.getUniqueIdentifier();
        if(request.getParameter("s") != null) {
            removeLink += "&s=" + request.getParameter("s");
        }           
%>
<tr>
    <td><%=d.signal.getUniqueIdentifier()%></td>
    <td align="right"><%=d.windowLength%></td>
    <td align="right"><%=d.numElements%></td>
    <td align="center"><%=((d.hasLocationData) ? "Y" : "N")%></td>
    <td align="right"><%=((d.lastPacketTimestamp != null) ? dateFormatter.format(d.lastPacketTimestamp) : "")%></td>
    <td align="right"><%=((d.lastUpdate != null) ? dateFormatter.format(d.lastUpdate) : "")%></td>
    <td align="right"><%=d.hits%></td>
<%
    if(!cacheManager.getSignalsToRemove().contains(d.signal)) {
%>
    <td align="right"><a href="<%=removeLink%>" title="Remove signal (all resolutions)"><img src="./images/drop.png" width="16" height="16" alt="Remove signal (all resolutions)" /></a></td>
<% } else { %>
    <td align="right"><img src="./images/drop-locked.png" width="16" height="16" alt="Removal pending" title="Removal pending" /></td>
<% } %>
</tr>
<%  } 
    double cacheSizeMb = (double)totalElements/1024/1024;
    DecimalFormat df = new DecimalFormat("#.###");
%>
    <tr style="background-color: #000000; color: #ffffff; font-weight: bold">
        <td colspan="8">Number of entries: <%=cacheInfo.size()%> / Size of cached data: <%=df.format(cacheSizeMb)%> MByte</td>
    </tr>
</table>
<p>&nbsp;</p>
<% } %>
<hr />
<div id="footer">
&copy; 2012 Computer Engineering and Networks Laboratory, ETH Zurich<br/>
Vizzly is free open-source software: <a href="https://code.google.com/p/vizzly">https://code.google.com/p/vizzly</a><br/>
</div>   
</body>
</html>