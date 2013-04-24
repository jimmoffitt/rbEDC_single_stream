Introduction
============

This is a Ruby script written to stream data from a Enterprise Data Collector (EDC).  This version can stream from a single EDC data feed and is based on CURL, as implemented by the Curl::Easy gem (curb).  (There will also be example code for streaming from multiple streams, based on event-machine, in the near future).

The target EDC data stream can be configured in a script configuration file. If you want to use this script to stream from more than one stream at a time, in theory, multiple instances opt this code could be ran (with different configuration files passed in).  

Once the script is started, it creates a streaming connection (using Curl) to the configured EDC data feed. 

* Important note: this script is designed to process normalized Activity Streams (atom) data in XML. 
 

Usage
=====

One file is passed in at the command-line if you are running this code as a script (and not building some wrapper
around the EDC_client class):

1) A configuration file with account/username/password details and processing options (see below, or the sample project
file, for details):  -c "./EDC_Config.yaml"

The EDC configuration file needs to have an "account" section, an "edc" section, and a "stream" section.  If you specify that
you are using database (edc --> storage: database) you will need to have a "database" section as well.

So, if you were running from a directory with this source file in it, with the configuration file in that folder too,
the command-line would look like this:

        $ruby ./edc_client_single_stream.rb -c "./EDC_Config.yaml"


Configuration
=============

See the sample EDC_config.yaml file for an example of a EDC client configuration file.  

Here are some important points:

<p>
+ In the "account" section, you specify the "machine name" used in the URL for your EDC.  EDCs have the following URL pattern:
    https://machine_name.gnip.com

<p>
+ In the "edc" section, you can specify the following processing options:
	+ storage: "files" or "database".  How do you plan on storing the data? In flat files or in a database.
		If you are storing as files, the filename is based on the native activity "id" and the extension indicates the 
		markup format (xml or json, although only xml is currently supported). 
	+ out_box: If storing data in files, where do you want them written to?

<p>
+ In the "stream" section you specify the EDC data feed you want to stream from. For this stream you need to specify its numeric "ID" and provide a stream name:
	
	+ ID: the numeric ID assigned to the stream.  This ID can be referenced by navigating to the data stream with the EDC dashboard and noting the numeric ID in the URL, as in "https://myEDC.gnip.com/data_collectors/**5**.  Note that these stream IDs are not always consecutive, and there will be gaps in the ID sequence if you have deleted any streams during the life of your EDC. 
		
	+ Name: a label given to the stream to help you identify the stream in the configuration file.  The first word of this 'name' is used to specify the *publisher* if you are writing to the database.  

<p>
* Example "stream" configuration section:

<code>

	stream:	
	  - ID 	  : 1
	    Name  : Facebook Keyword Search  
</code>