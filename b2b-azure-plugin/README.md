## Azure Blob Storage plugin with Application pickup, and Delivery
* Download PluggableTransport here ![tar](distributions/b2b-azure-plugin.tar), ![zip](distributions/b2b-azure-plugin.zip) and extract the contents to ```{B2BI_INSTALL_HOME}/Interchange/site/jars``` to each of the B2Bi cluster nodes.
  *NOTE: All the libraries referenced by the plugin are included in the tar/zip file.
* Adjust pluggabletransports.xml under ```{B2BI_INSTALL_HOME}/Interchange/conf``` with the xml content from ![Pluggable-Transport-Configuration](distributions/azure-pluggabletransport.xml)
  *NOTE: A constant setting with the name 'Exchange type' is used to determine whether the plugin is used for Pickup/Delivery.
* Bounce Trading Engine on all cluster nodes
* Source can be viewed ![here](../)
  * If you are building on your local laptop, add a lib folder to the project and include ```interchange-server.jar``` and other dependencies. Refer gradle build script for more details
* The plugin is tested minimally, and is built as an exercise. Please use it with proper discretion. Feel free to submit a pull request for any issues/bug fixes/suggestions.

### Sample screenshots

#### Sample Application Pickup Configuration
![Sample Application Pickup Configuration](distributions/images/Sample_ApplicationPickup.JPG)


#### Sample Application Delivery Configuration
![Sample Application Delivery Configuration](distributions/images/Sample_ApplicationDelivery.JPG)
