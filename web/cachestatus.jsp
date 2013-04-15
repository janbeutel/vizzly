<%@ page import="ch.ethz.vizzly.VizzlyStateContainer,ch.ethz.vizzly.cache.*,ch.ethz.vizzly.datatype.*,
    java.util.*,java.text.SimpleDateFormat,java.text.DecimalFormat,java.lang.StringBuffer" %>
<html><head>
<title>Vizzly Cache Status</title>
<script type='text/javascript' src='extlib/jquery-1.7.2.min.js'></script>
<style type="text/css">
h3 { font-family: Verdana, Helvetica, sans-serif; font-size:14px; }
body, p, td { font-family: Verdana, Helvetica, sans-serif; font-size:12px; }
a.white { color:#ffffff; }
#footer, td.it { font-style: italic; }
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
<h3>Update Worker Status</h3>
<%
CacheUpdateWorkerSynchronization workerSync = (CacheUpdateWorkerSynchronization)application
    .getAttribute(CacheUpdateWorkerSynchronization.SERVLET_ATTRIB_KEY);
VizzlySignal[] workerSignals = workerSync.getWorkerSignals();
%>
<p>
<table style="border: 1px solid #000000;">
    <tr style="background-color: #000000; color: #ffffff; font-weight: bold">
        <td width="30">#</td>
        <td width="300">PROCESSED SIGNAL</td>
    </tr>
<%
for(int i = 0; i < workerSignals.length; i++) {
%>
    <tr>
        <td><%=i%></td>
<%
    if(workerSignals[i] != null) {
%>
        <td><%=workerSignals[i].getUniqueIdentifier()%></td>
<%
    } else {
%>
        <td class="it">Sleeping</td>
<%
    }
%>
    </tr>
<%
}
%>    
</table>
</p>


<%
if ("POST".equalsIgnoreCase(request.getMethod())) {
    // Process signal removal requests
    Map<String, String[]> parameters = request.getParameterMap();
    for(String parameter : parameters.keySet()) {
        // Quite inefficient right now, but should not be used so often anyways
        for(VizzlySignal s : cacheManager.getSignals(cacheManager.getNumberOfCaches()-1)) {
            if(s.getUniqueIdentifier().equals(parameter)) {
                cacheManager.scheduleSignalForRemoval(s);
            }   
        }
    }
    response.sendRedirect(request.getRequestURI());
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
<form method="POST">
<table style="border: 1px solid #000000;">
<tr style="background-color: #000000; color: #ffffff; font-weight: bold">
    <td>DEL</td>
    <td><a class="white" href="?s=NAME">SIGNAL NAME</a></td>
    <td><a class="white" href="?s=WINDOW_LENGTH">WIN LEN.</a></td>
    <td><a class="white" href="?s=NUM_ELEMENTS"># ROWS</a></td>
    <td><a class="white" href="?s=HAS_LOCATION_DATA">LOC</a></td>
    <td width="160"><a class="white" href="?s=LAST_PACKET_TIMESTAMP">LAST TIMESTAMP</a></td>
    <td width="160"><a class="white" href="?s=LAST_UPDATE">LAST UPDATE</a></td>
    <td><a class="white" href="?s=HITS">HITS</a></td>
</tr>
<%
    Vector<CachedDataInfo> cacheInfo = cacheManager.getCachedDataInfo(i);
    long totalElements = 0;
    int signalIdx = 0;
    Collections.sort(cacheInfo, comp);

    for (CachedDataInfo d : cacheInfo) {
        totalElements += (d.hasLocationData) ? d.numElements*24 : d.numElements*8;
        signalIdx++;
            
%>
<tr class="signalrow" id="row_<%=i%>_<%=signalIdx%>">
<%
    if(!cacheManager.getSignalsToRemove().contains(d.signal)) {
%>
    <td><input type="checkbox" id="check_<%=i%>_<%=signalIdx%>" name="<%=d.signal.getUniqueIdentifier()%>" /></td>
<% } else { %>
    <td><img src="./images/drop-locked.png" width="16" height="16" alt="Removal pending" title="Removal pending" /></td>
<% } %>
    <td><%=d.signal.getUniqueIdentifier()%></td>
    <td align="right"><%=d.windowLength%></td>
    <td align="right"><%=d.numElements%></td>
    <td align="center"><%=((d.hasLocationData) ? "Y" : "N")%></td>
    <td align="right"><%=((d.lastPacketTimestamp != null) ? dateFormatter.format(d.lastPacketTimestamp) : "")%></td>
    <td align="right"><%=((d.lastUpdate != null) ? dateFormatter.format(d.lastUpdate) : "")%></td>
    <td align="right"><%=d.hits%></td>
</tr>
<%  } 
    double cacheSizeMb = (double)totalElements/1024/1024;
    DecimalFormat df = new DecimalFormat("#.###");
%>
    <tr style="background-color: #000000; color: #ffffff; font-weight: bold">
        <td colspan="8">Number of entries: <%=cacheInfo.size()%> / Size of cached data: <%=df.format(cacheSizeMb)%> MByte</td>
    </tr>
</table>
<input type="submit" value="Submit">
</form>
<p>&nbsp;</p>
<% } %>
<hr />
<div id="footer">
&copy; 2013 Computer Engineering and Networks Laboratory, ETH Zurich<br/>
Vizzly is free open-source software: <a href="https://code.google.com/p/vizzly">https://code.google.com/p/vizzly</a><br/>
</div>
<script type='text/javascript'>
$("tr.signalrow").mouseover(function() {
    $(this).css('background-color', '#77FF99');
}).mouseout(function() {
    var rowName = 'check_' + $(this).attr('id').substr(4, $(this).attr('id').len);
    if(!$("#" + rowName).prop('checked')) {
          $(this).css('background-color', 'transparent');
    }
}).click(function(e) {
    var rowName = 'check_' + $(this).attr('id').substr(4, $(this).attr('id').len);
    // The following line checks if the checkbox itself or the table row has been clicked
    if($(e.target).closest('input[type="checkbox"]').length == 0) {
        $("#" + rowName).prop('checked', !$("#" + rowName).prop('checked'));
}});
</script>
</body>
</html>