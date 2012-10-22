<%@ page import="ch.ethz.vizzly.VizzlyStateContainer,ch.ethz.vizzly.cache.*,ch.ethz.vizzly.datatype.*,java.util.*,java.text.SimpleDateFormat,java.text.DecimalFormat" %>
<html><head>
<title>Vizzly Cache Status</title>
<style type="text/css">
h3 { font-family: Verdana, Helvetica, sans-serif; font-size:14px; }
p, td { font-family: Verdana, Helvetica, sans-serif; font-size:12px; }
a { color:#ffffff; }
</style>
</head>
<body bgcolor="#FFFFFF">
<%

VizzlyStateContainer vizzlyState = 
        (VizzlyStateContainer)application.getAttribute(VizzlyStateContainer.SERVLET_ATTRIB_KEY);
CacheManager cacheManager = vizzlyState.getCacheManager();

// Process signal removal requests
if(request.getParameter("remove") != null) {
    for(VizzlySignal s : cacheManager.getSignals(0)) {
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
<h3><%=cacheManager.getCacheDescription(i)%></h3>
<table style="border: 1px solid #000000;">
<tr style="background-color: #000000; color: #ffffff; font-weight: bold">
    <td><a href="?s=NAME">SIGNAL NAME</a></td>
    <td><a href="?s=WINDOW_LENGTH">WIN LEN.</a></td>
    <td><a href="?s=NUM_ELEMENTS"># ROWS</a></td>
    <td><a href="?s=HAS_LOCATION_DATA">LOC</a></td>
    <td width="160"><a href="?s=LAST_PACKET_TIMESTAMP">LAST TIMESTAMP</a></td>
    <td width="160"><a href="?s=LAST_UPDATE">LAST UPDATE</a></td>
    <td><a href="?s=HITS">HITS</a></td>
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
</body>
</html>