# EXAMPLE #6: JetStream management: streams and consumers
# remember to start nats-server with -js to enable JetStream and -sd to set the storage directory

package require nats
package require fileutil

set conn [nats::connection new "MyNats"]
$conn configure -servers nats://localhost:4222
$conn connect
set js [$conn jet_stream]

# create a stream collecting messages sent to the foo.* and bar.* subjects
$js add_stream MY_STREAM -subjects [list foo.* bar.*] -retention limits -max_msgs 100 -discard old
# Create another stream with configuration from a JSON file (compatible with NATS CLI)
set json_config [fileutil::cat [file join [file dirname [info script]] stream_config.json]]
set response [$js add_stream_from_json $json_config]
puts "Added a stream: $response" ;# unlike add_stream that returns a Tcl dict, add_stream_from_json returns JSON exactly as received from NATS

puts "List all streams: [$js stream_names]"
puts "Find the stream that listens to foo.* : [$js stream_names -subject foo.*]"
puts "Info about MY_STREAM: [$js stream_info MY_STREAM]"

# create a push consumer using a config from Tcl dict
# note that dashes in option names are optional, i.e. you can use 'description' or '-description' etc
set push_consumer_config [dict create \
                         description "dummy push consumer" \
                         filter_subject bar.*]

set response [$js add_push_consumer MY_STREAM PUSH_CONSUMER my_delivery_subj {*}$push_consumer_config]
puts "Added a push consumer: $response"

# Create a pull consumer with configuration from a JSON file (compatible with NATS CLI)
set json_config [fileutil::cat [file join [file dirname [info script]] pull_consumer_config.json]]
set response [$js add_consumer_from_json MY_STREAM PULL_CONSUMER $json_config]
puts "Added a pull consumer: $response"
# Note: while you can use the universal method 'add_consumer' that can create both push and pull consumers,
# using explicit 'add_push_consumer' and 'add_pull_consumer' allows cleaner code

puts "List all consumers defined on MY_STREAM: [$js consumer_names MY_STREAM]"

set response [$js delete_consumer MY_STREAM PUSH_CONSUMER]
puts "Deleted PUSH_CONSUMER: $response"

puts "Check that there is only one consumer left: [$js consumer_names MY_STREAM]"

$js delete_stream MY_STREAM
$js delete_stream MY_STREAM2

$js destroy
$conn destroy

# not all JetStream management functions are shown in this example! refer to the API docs and tests for more details
