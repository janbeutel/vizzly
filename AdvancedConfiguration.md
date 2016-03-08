# Vizzly Configuration File #

A template for the configuration file that Vizzly uses is provided in _web/WEB-INF/vizzly.xml.example_

This file will automatically be copied into _./jetty-deploy/webapp/WEB-INF/vizzly.xml_ when running `ant jetty-deploy` for the first time. Changes are ideally done by directly editing  _./jetty-deploy/webapp/WEB-INF/vizzly.xml_. Vizzly needs to be reloaded for changes to take effect.

```
<?xml version="1.0" encoding="ISO-8859-1"?>

<!-- Vizzly will start a number of background threads that are used for continuously fetching the most recent data from all data sources known.
        In combination with a SQL database, the performance tracker can be used to collect runtime statistics. -->
<vizzlyConfig numWorkerThreads="2" enablePerformanceTracker="false">

	<!-- Uncomment the following line to setup a database connection. A database connection is required for the performance tracker
            as well as for a SQL cache to work -->
	<!-- <database jdbcDriver="com.mysql.jdbc.Driver" jdbcUrl="jdbc:mysql://host:3306/vizzly" jdbcUser="USER" jdbcPassword="PASS" /> -->
	
	<!-- 
	       In principle you can define as many caches as you want. In reality something will break right now if there is more than one
	       SQL DB cache configured. This is going to be fixed in one of the next versions.
	
	       Ideally, the window lengths of different caches should be multiples of the smallest window length used. The configuration we use
	       is to have a SQL cache with 240sec window length and a memory cache with 960sec window length. The larger the window length over which
	       data is aggregated, the less space for storing aggregates is needed.
	-->
	<caches>
		<memoryCache windowLengthSec="240" />

		<!-- Uncomment the following line to enable the SQL cache. The database configuration must be configured as well for this to work.
		      Please not that changing windowLengthSec requires you to drop all existing data tables - otherwise strange things will happen -->
		<!-- <sqlDbCache windowLengthSec="240" /> -->

	</caches>
</vizzlyConfig>
```

# Other Issues #

## How can I change the TCP port on which Vizzly listens? ##

The TCP port (default: 8080) on which the Jetty server that runs Vizzly listens on can be changed by editing _./jetty-deploy/etc/jetty.xml_.