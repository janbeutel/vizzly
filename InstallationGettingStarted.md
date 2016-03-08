# Introduction #

Vizzly is a Java EE 6 Web application that can basically be deployed into any Servlet container (Apache Tomcat, JBoss, Jetty, ...). The project repository includes a fully configured runtime that is based on Jetty.

# Using the included Jetty runtime #

We use Ant for building Vizzly. While there is also a target that creates a generic WAR file that can then be deployed anywhere, the Ant target _jetty-deploy_ also adds Jetty-specific configuration parameters (enables GZip compression and disables Cross-Origin checking for testing).

  1. Checkout Vizzly from the SVN repository hosted on Google Code
  1. In the main folder, run _ant jetty-deploy_  for building the WAR file that is then deployed in Jetty
  1. Go to _./jetty-runtime_, create your own copy of _start\_vizzly.sh_ by copying the provided _start\_vizzly.sh.example_
  1. Start Vizzly using _./start\_vizzly.sh_

# Default URLs & Examples #

  * http://localhost:8080/ - First example of a line plot widget, data is loaded from a CSV file
  * http://localhost:8080/lineplot-gsn-example.html - Another line plot example, but this time data is loaded from a GSN server
  * http://localhost:8080/map-gsn-example.html - An example of the Vizzly map widget
  * http://localhost:8080/cachestatus.jsp - Cache status page