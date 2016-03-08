# Introduction #

This page describes the JSON notation used for specifying visualization widgets. The definition of the canvas and the signals that should be plotted is independent of the type of the widget, i.e., time series or map widget. The type of widget is then determined by the chosen function call used for populating the canvas area.

## General graph/canvas properties ##

```
// Definition of a graph, same format for time series and map widgets
g1Config = {
    // Properties of viewing canvas
    "graph": {
        // ID of the <div> element into which the graph is embedded
        "div": "g1_div",
        // Width of the graph in pixels
        "width": 600,
        // Height of the graph in pixels
        "height": 250,
        // Enable/disable signal selection form in time series plot
        "showForm": false
    },
    // URL of Java Servlet, e.g. http://vizzly.server:8080/vizzly?
    "appUrl": "vizzly?",
    // Base URL for images
    "imageUrl" : "./images/",
    // Initial center position of map widget
    "mapCenter": new google.maps.LatLng(47.379022, 8.541001),
    // Array of signals that should be shown in this plot
    "signals": []
}
```

## Specification of a Vizzly Signal ##

A graph config (see previous section) requires one or more signals to be specified.

```
{
    // Name of the signal, this name is used in generated drop-down menus etc.
    "displayName": "Particular matter",
    // Data source config
    // - type: "gsn" or "csv", more readers can be registered when extending Vizzly
    // - serverAddress: Only relevant when type is "gsn" - Address of GSN server
    // - name: If type is "gsn", name of the virtual sensor. If type is "csv", name of the CSV file to read from.
    "dataSource": { "type": "gsn", "serverAddress": "data.opensense.ethz.ch", "name": "opensense_minidisc_emb__mapped" },
    // Name of the data field/column that holds the actual measurements
    "dataField": "number",
    // Selection of a subset of the input file by adding a selector
    // - type: "all" or "single"
    // - field: Only relevant when type is "single" - Name of the data field/column used for selecting a subset
    // - value: Only relevant when type is "single" - Only entries whose "field" has the value "value" are displayed
    "deviceSelect": { "type": "all" },
    // Name of the data field/column that holds time information
    "timeField": "generation_time",
    // Only relevant for line plots - Measurement values are multiplied with this value
    "scaling": 1.0,
    // Only relevant for line plots - decide if this signal is visible from the start
    "visible": true,
    // Only relevant for map plots - name of the data field/column that holds latitude information
    "locationLatField": "latitude",
    // Only relevant for map plots - name of the data field/column that holds longitude information
    "locationLngField": "longitude",
    // Only relevant for map plots - thresholds used for painting markers green, yellow or red
    "thresholds": [ 18.0, 8.0 ]
}
```


## JavaScript Library Objects ##

Creation of a line plot widget:

```
var g1 = new LinePlotWidget(g1Config);
```

Creation of a map plot widget:

```
var aggMap = new MapWidget(g1Config);
```