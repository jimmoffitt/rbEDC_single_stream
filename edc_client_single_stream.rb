
'''
A simple script to stream a single Enterprise Data Collector (EDC) feed.

Streaming is managed using the curb gem (Curl::Easy).

Currently, this script only handles normalized Activity Stream (ATOM) data formatted in XML.

TODO:
    [] Add connection timeout handling.  Curl::Easy does not seem to provide such a parameter.  So use a timer.
    [] Add support for original format (JSON).

'''

require 'curb'
require "base64"    #Used for basic password encryption.
require "yaml"      #Used for configuration file management.
require "nokogiri"  #Used for parsing activity XML.
require "cgi"       #Used only for retrieving HTTP GET parameters and converting to a hash.
#require "nori"     #I tried to use this gem to transform XML to a Hash, but the IDE kept blowing up!

class EDC_Client

    ACT_OPEN = "<entry"
    ACT_CLOSE = "</entry>"

    attr_accessor :http, :datastore, :machine_name, :user_name, :password_encoded, :url,
                  :storage, :out_box,
                  :data,      #Used to manage data streaming in... streaming loop adds content to this string, as we chomp off the data.
                  :publisher  #Parsed from configuration file name of data stream.

    def initialize(config_file = nil)

        @data = ""

        if not config_file.nil? then
            getCollectorConfig(config_file)
        end

        if not @machine_name.nil? then
            @url = "https://" + @machine_name + ".gnip.com/data_collectors"
        end

        if not config_file.nil? then
            getStreamConfig(config_file)
        end

    end

    def getPassword
        #You may want to implement a more secure password handler.  Or not.
        @password = Base64.decode64(@password_encoded)  #Decrypt password.
    end

    def getCollectorConfig(config_file)

        config = YAML.load_file(config_file)

        #Account details.
        @machine_name = config["account"]["machine_name"]
        @user_name  = config["account"]["user_name"]
        @password_encoded = config["account"]["password_encoded"]

        if @password_encoded.nil? then  #User is passing in plain-text password...
            @password = config["account"]["password"]
            @password_encoded = Base64.encode64(@password)
        else
            @password = getPassword
        end

        #EDC configuration details.
        @storage = config["edc"]["storage"]
        @out_box = config["edc"]["out_box"]

        if @storage == "database" then #Get database connection details...
            db_host = config["database"]["host"]
            db_port = config["database"]["port"]
            db_schema = config["database"]["schema"]
            db_user_name = config["database"]["user_name"]
            db_password  = config["database"]["password"]
            #... and create a database object...
            @datastore = PtDatabase.new(db_host, db_port, db_schema, db_user_name, db_password)

            begin
                @datastore.connect
            rescue
                #No database connection, put your error handling here...
                p "Could not connect to database @ #{db_host}!"
            end
        end
    end

    #Load our target stream from configuration file.
    def getStreamConfig(config_file)

        config = YAML.load_file(config_file)

        #Load any configured streams.
        @stream = config["stream"]

        if @stream.nil? then
            p "No stream is configured. "
        else #Parse and set Publisher.
            @publisher = @stream[0]['Name'].split(' ')[0]
        end
    end

    '''
    Parses normalized Activity Stream XML.
    Parsing details here are driven by the current database schema used to store activities.
    If writing files, then just write out the entire activity payload to the file.
    '''
    def processResponseXML(docXML)
        content = ""
        id = ""
        posted_time = ""
        body = ""
        tags = Array.new
        values = Array.new

        node = docXML.root

        #Grab this activity content
        content = node.to_s
        #p content

        #Storing as a file?  Then we are writing the entire activity payload with no need to parse out details.
        if @storage == "files" then #Write to the file.

            node.children.each do |sub_node|
                #TODO: make this short-circuit after finding "id" tag...
                id = sub_node.inner_text if sub_node.name == "id"
            end

            #Create file name
            filename = id + ".xml"
            File.open(@out_box + "/" + filename, "w") do |new_file|

                begin
                    new_file.write(content)
                    p "Writing #{filename}"
                rescue
                    p "Error writing file: #{filename}"
                end
            end
        else #Storing in database, so do more parsing for payload elements that have been promoted to db fields.
            node.children.each do |sub_node|
                id = sub_node.inner_text if sub_node.name == "id"
                posted_time = sub_node.inner_text if sub_node.name == "created"

                if sub_node.name == "object" then
                    sub_node.children.each do |content_node|
                        body = content_node.inner_text if content_node.name == "content"
                    end
                end

                if sub_node.name == "matching_rules" then
                    sub_node.children.each do |rules_node|
                        values << rules_node.inner_text if rules_node.name == "matching_rule"
                        tags << rules_node.attr('tag') if rules_node.name == "matching_rule" unless tags.include?(rules_node.attr('tag'))
                    end
                end
            end

            @datastore.storeActivityData(id, posted_time, content, body, @publisher, values, tags)
        end
    end

    def checkForActivity(data)

        fullActivity = false

        if data.include?(ACT_OPEN) and data.include?(ACT_CLOSE) then
            if data.index(ACT_OPEN) < data.index(ACT_CLOSE) then
                fullActivity = true
            end
        end
        fullActivity
    end

    #Streamed data are directed to here...
    #This method monitors the @data structure, looks for complete activities and send them off for processing.
    #Several cases to handle for @data:
    #<entry @@@@@@@@@@@@@@ </entry>
    #<entry @@@@@@@@@@@@@@@@@@@@@@@
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    #@@@@@@@<entry @@@@@@@@</entry>
    #@@<entry @@@@@</entry>@@@@@@@@

    def handleData(data)

        incoming = Array.new  #Activity array of what we are taking off the incoming buffer.
        activities = Array.new #Activity array of what we are cleaning up, passing on for processing.

        @data = @data + data  #Add the incoming buffer data to our managed data string.
        #p "length of data: #{@data.length}"

        #loop until we no longer have a full activity to work on.
        #A full activity is defined by matching <entry> tags.

        fullActivity = true

        while fullActivity
            fullActivity = checkForActivity(@data)

            if fullActivity then
                #Consume first activity.
                activity = @data[@data.index(ACT_OPEN)..(@data.index(ACT_CLOSE) + ACT_CLOSE.length)-1]
                #p activity

                #Insert into activities array to be further processed.
                activities << activity

                #Remove activity from @data
                @data.slice!(activity)
            end
        end

        #p "length of data: #{@data.length}"

        activities.each do |activity|
            #p activity
            docXML = Nokogiri::XML.parse(activity)  {|config| config.noblanks}
            processResponseXML(docXML)
        end
    end

    '''
    Establishes a data stream for a single EDC stream.
    '''
    def streamData

        #Build streaming end-point.
        url = "https://#{@machine_name}.gnip.com/data_collectors/#{@stream[0]["ID"]}/stream.xml"

        #Create streaming HTTP connection, and start streaming data...
        Curl::Easy.http_get url do |c|
            c.http_auth_types = :basic
            c.username = @user_name
            c.password = @password

            c.encoding = "gzip"
            c.verbose = true

            c.on_body do |data|
                if data.length > 1 then
                    #p data
                    handleData(data)
                else
                    p "Received heartbeat @ #{Time.now}.  data = #{data}"
                end
                data.size # required by curl's api.
            end
        end
    end
end



#=======================================================================================================================
#Database class.

'''
This class is meant to demonstrate basic code for building a "database" class for use with the
PowerTrack set of example code.  It is written in Ruby, but in its present form hopefully will
read like pseudo-code for other languages.

One option would be to use (Rails) ActiveRecord for data management, but it seems that may abstract away more than
desired.

Having said that, the database was created (and maintained/migrated) with Rails ActiveRecord.
It is just a great way to create databases.

ActiveRecord::Schema.define(:version => 20130306234839) do

  create_table "activities", :force => true do |t|
      t.string   "native_id"
      t.text     "content"
      t.text     "body"
      t.string   "rule_value"
      t.string   "rule_tag"
      t.string   "publisher"
      t.datetime "created_at",               :null => false
      t.datetime "updated_at",               :null => false
      t.datetime "posted_at"
  end

end

The above table fields are a bit arbitrary.  I cherry picked some Tweet details and promoted them to be table fields.
Meanwhile the entire tweet is stored, in case other parsing is needed downstream.
'''
class PtDatabase
    require "mysql2"
    require "time"
    require "json"
    require "base64"

    attr_accessor :client, :host, :port, :user_name, :password, :database, :sql

    def initialize(host=nil, port=nil, database=nil, user_name=nil, password=nil)
        #local database for storing activity data...

        if host.nil? then
            @host = "127.0.0.1" #Local host is default.
        else
            @host = host
        end

        if port.nil? then
            @port = 3306 #MySQL post is default.
        else
            @port = port
        end

        if not user_name.nil?  #No default for this setting.
            @user_name = user_name
        end

        if not password.nil? #No default for this setting.
            @password = password
        end

        if not database.nil? #No default for this setting.
            @database = database
        end
    end

    #You can pass in a PowerTrack configuration file and load details from that.
    def config=(config_file)
        @config = config_file
        getSystemConfig(@config)
    end


    #Load in the configuration file details, setting many object attributes.
    def getSystemConfig(config)

        config = YAML.load_file(config_file)

        #Config details.
        @host = config["database"]["host"]
        @port = config["database"]["port"]

        @user_name = config["database"]["user_name"]
        @password_encoded = config["database"]["password_encoded"]

        if @password_encoded.nil? then  #User is passing in plain-text password...
            @password = config["database"]["password"]
            @password_encoded = Base64.encode64(@password)
        end

        @database = config["database"]["schema"]
    end


    def to_s
        "EDC object => " + @host + ":" + @port.to_s + "@" + @user_name + " schema:" + @database
    end

    def connect
        #TODO: need support for password!
        @client = Mysql2::Client.new(:host => @host, :port => @port, :username => @user_name, :database => @database )
    end

    def disconnect
        @client.close
    end

    def SELECT(sql = nil)

        if sql.nil? then
            sql = @sql
        end

        result = @client.query(sql)

        result

    end

    def UPDATE(sql)
    end

    def REPLACE(sql)
        begin
            result = @client.query(sql)
            true
        rescue
            false
        end
    end

    #Tweet "id" field has this form: "tag:search.twitter.com,2005:198308769506136064"
    #This function parses out the numeric ID at end.
    def getTwitterNativeID(id)
        native_id = Integer(id.split(":")[-1])
    end

    #Most publishers use UTC. Thankfully!
    def getPostedTime(time_stamp)
        time_stamp = Time.parse(time_stamp).strftime("%Y-%m-%d %H:%M:%S")
    end

    #Replace some special characters with an _.
    #(Or, for Ruby, use ActiveRecord for all db interaction!)
    def handleSpecialCharacters(text)

        if text.include?("'") then
            text.gsub!("'","_")
        end
        if text.include?("\\") then
            text.gsub!("\\","_")
        end

        text
    end

    '''
    storeActivity
    Receives an Activity Stream data. Writes to an Activities table.

    t.string   "native_id"
    t.text     "content"
    t.text     "body"
    t.string   "rule_value"
    t.string   "rule_tag"
    t.string   "publisher"
    t.string   "job_uuid"  #Used for Historical PowerTrack.
    t.datetime "posted_at"
    '''

    def storeActivityData(native_id, posted_at, content, body, publisher, rule_values, rule_tags)

        content = handleSpecialCharacters(content)
        body = handleSpecialCharacters(body)

        #Build SQL.
        sql = "REPLACE INTO activities (native_id, posted_at, content, body, rule_value, rule_tag, publisher, created_at, updated_at ) " +
            "VALUES ('#{native_id}', '#{posted_at}', '#{content}', '#{body}', '#{rule_values}','#{rule_tags}','#{publisher}', UTC_TIMESTAMP(), UTC_TIMESTAMP());"

        if not REPLACE(sql) then
            p "Activity not written to database: " + publisher + " | " + native_id
        else
            p "Activity WRITTEN to database: " + publisher + " | " + native_id
        end
    end

end #PtDB class.

#=======================================================================================================================
if __FILE__ == $0  #This script code is executed when running this file.

    OptionParser.new do |o|
        o.on('-c CONFIG') { |config| $config = config}
        o.parse!
    end

    if $config.nil? then
        $config = "./EDC_Config_private.yaml"  #Default
    end

    p "Creating EDC Client object with config file: " + $config

    edc = EDC_Client.new($config)
    edc.streamData

    p "Exiting"


end

