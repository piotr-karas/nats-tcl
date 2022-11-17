# Copyright (c) 2021-2022 Petro Kazmirchuk https://github.com/Kazmirchuk
# Copyright (c) 2021 ANT Solutions https://antsolutions.eu/

# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and  limitations under the License.

oo::class create ::nats::jet_stream {
    variable conn timeout
    
    # do NOT call directly! instead use connection::jet_stream
    constructor {c t} {
        set conn $c
        set timeout $t
    }

    # SUMMARY https://docs.nats.io/reference/reference-protocols/nats_api_reference

    # nats schema info --yaml io.nats.jetstream.api.v1.stream_msg_get_request
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_msg_get_response
    method stream_msg_get {stream args} {
        set subject "\$JS.API.STREAM.MSG.GET.$stream"

        set containSeq [dict exists $args "-seq"]
        set containLastBySubj [dict exists $args "-last_by_subj"]
        if {($containSeq && $containLastBySubj) || (!$containSeq && !$containLastBySubj)} {
            # XOR: exacly one of this option should be provided
            throw {NATS ErrInvalidArg} "Options should contain -seq or -last_by_subj"
        }

        set msg {}
        set common_arguments [dict create]
        
        foreach {opt val} $args {
            switch -- $opt {
                -last_by_subj {
                    set msg [::nats::_json_write_object "last_by_subj" [json::write::string $val]]
                }
                -seq {
                    set msg [::nats::_json_write_object "seq" $val]
                }
                default {
                    throw {NATS ErrInvalidArg} "Unknown option $opt"
                }
            }
        }

        return [my SimpleRequest $subject $common_arguments "Getting message from stream $stream timed out" $msg]
    }
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_msg_delete_request
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_msg_delete_response
    method stream_msg_delete {stream args} {
        set subject "\$JS.API.STREAM.MSG.DELETE.$stream"

        if {![dict exists $args "-seq"]} {
            throw {NATS ErrInvalidArg} "Options should contain -seq"
        }

        set msg {}
        set common_arguments [dict create]
        
        foreach {opt val} $args {
            switch -- $opt {
                -seq {
                    set msg [::nats::_json_write_object "seq" $val]
                }
                default {
                    throw {NATS ErrInvalidArg} "Unknown option $opt"
                }
            }
        }

        return [my SimpleRequest $subject $common_arguments "Deleting message from stream $stream timed out" $msg]
    }

    # see also https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-13.md
    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_getnext_request
    
    # TODO: derive expires from timeout
    method consume {stream consumer args} {
        if {![${conn}::my CheckSubject $stream]} {
            throw {NATS ErrInvalidArg} "Invalid stream name $stream"
        }
        if {![${conn}::my CheckSubject $consumer]} {
            throw {NATS ErrInvalidArg} "Invalid consumer name $consumer"
        }

        set subject "\$JS.API.CONSUMER.MSG.NEXT.$stream.$consumer"
        # timeout will shadow the member var
        nats::_parse_args $args {
            timeout timeout 0
            callback str ""
            expires pos_int null
            batch_size pos_int null
            idle_heartbeat pos_int null
            no_wait bool null
            _custom_reqID valid_str ""
        }
        
        # the JSON body is:
        # expires : nanoseconds
        # batch: int
        # no_wait: bool
        # idle_heartbeat: nanoseconds - I have no clue what it does! but it is present in the C# client
        # when all options=defaults, do not send JSON at all
        
        if {[info exists batch_size]} {
            set batch $batch_size
        }
        
        foreach opt {expires batch no_wait idle_heartbeat} {
            if {![info exists $opt]} {
                continue
            }
            set val [set $opt]
            if {$opt in {expires idle_heartbeat}} {
                # convert ms to ns
                set val [expr {entier($val*1000*1000)}]
            }
            dict set config_dict $opt $val
        }
        # custom_reqID
        # TODO no_wait conflicts with expires?
        set message ""
        if {[info exists config_dict]} {
            set message [::nats::_json_write_object {*}$config_dict]
        }
        set req_opts [list -dictmsg true -timeout $timeout -callback $callback]
        if {[info exists batch_size]} {
            lappend req_opts -max_msgs $batch_size
        } else {
            lappend req_opts -max_msgs 1 ;# trigger an old-style request
        }
        if {$_custom_reqID ne ""} {
            lappend req_opts -_custom_reqID $_custom_reqID
        }
        try {
            return [$conn request $subject $message {*}$req_opts]
        } trap {NATS ErrTimeout} err {
            throw {NATS ErrTimeout} "Consume $stream.$consumer timed out"
        }
    }

    # Ack acknowledges a message. This tells the server that the message was
    # successfully processed and it can move on to the next message.
    method ack {message} {
        $conn publish [dict get $message reply] ""
    }

    # Nak negatively acknowledges a message. This tells the server to redeliver
    # the message. You can configure the number of redeliveries by passing
    # nats.MaxDeliver when you Subscribe. The default is infinite redeliveries.
    method nak {message} {
        $conn publish [dict get $message reply] "-NAK"
    }

    # Term tells the server to not redeliver this message, regardless of the value
    # of nats.MaxDeliver.
    method term {message} {
        $conn publish [dict get $message reply] "+TERM"
    }

    # InProgress tells the server that this message is being worked on. It resets
    # the redelivery timer on the server.
    method in_progress {message} {
        $conn publish [dict get $message reply] "+WPI"
    }
    
    method publish {subject message args} {
        set opts [dict create {*}$args]
        set userCallback ""
        if {[dict exists $opts "-callback"]} {
            # we need to pass our own callback to "request"; other options are passed untouched
            set userCallback [dict get $opts "-callback"]
            dict set opts -callback [mymethod PublishCallback $userCallback]
        }
        
        # note: nats.go replaces ErrNoResponders with ErrNoStreamResponse, but I don't see much value in it
        set result [$conn request $subject $message {*}$opts -dictmsg true]

        if {$userCallback ne ""} {
            return
        }
        
        # can throw nats server error
        return [nats::_parsePublishResponse $result]
    }

    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_create_request
    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_create_response
    #"required":
    #    "deliver_policy",
    #    "ack_policy",
    #    "replay_policy"
    # 
    method add_consumer {stream args} {
        if {![${conn}::my CheckSubject $stream]} {
            throw {NATS ErrInvalidArg} "Invalid stream name $stream"
        }

        set common_arguments [dict create]
        set config_dict [dict create] ;# variable with formatted arguments

        # supported arguments
        set arguments_list [list description deliver_group deliver_policy opt_start_seq \
            opt_start_time ack_policy ack_wait max_deliver filter_subject replay_policy \
            rate_limit_bps sample_freq max_waiting max_ack_pending idle_heartbeat flow_control \
            deliver_subject durable_name]


        foreach {opt val} $args {
            switch -- $opt {
                -durable_name {
                    if {![${conn}::my CheckSubject $val]} {
                        throw {NATS ErrInvalidArg} "Invalid durable consumer name $val"
                    }

                    dict set config_dict durable_name $val
                    set durable_consumer_name $val
                }
                -flow_control {       
                    # flow control must be boolean          
                    if {![string is boolean $val]} {
                        throw {NATS ErrInvalidArg} "Argument flow_control should be boolean"
                    }
                    if {$val} {
                        dict set config_dict flow_control true
                    } else {
                        dict set config_dict flow_control false
                    }
                }
                -replay_policy {
                    # only values: instant/original are supported
                    if {$val ni [list instant original]} {
                        throw {NATS ErrInvalidArg} "Wrong replay_policy value, must be: instant or original"
                    }
                    dict set config_dict replay_policy $val
                }
                -ack_policy {
                    # only values: none/all/explicit are supported
                    if {$val ni [list none all explicit]} {
                        throw {NATS ErrInvalidArg} "Wrong ack_policy value, must be: none, all or explicit"
                    }
                    dict set config_dict ack_policy $val
                }                
                default {
                    set opt_raw [string range $opt 1 end] ;# remove flag
                    # duration args - provided in milliseconds should be formatted to nanoseconds 
                    if {$opt_raw in [list "idle_heartbeat" "ack_wait"]} {
                        if {![string is double -strict $val]} {
                            throw {NATS ErrInvalidArg} "Wrong duration value for argument $opt_raw it must be in milliseconds"
                        }
                        set val [expr {entier($val*1000*1000)}] ;#conversion milliseconds to nanoseconds
                    }
                    
                    # checking if all provided arguments are valid
                    if {$opt_raw ni $arguments_list} {
                        throw {NATS ErrInvalidArg} "Unknown option $opt"
                    } else {
                        dict set config_dict $opt_raw $val
                    }                    
                }
            }
        }

        # pull/push consumers validation
        if {[dict exists $config_dict deliver_subject]} {
            # push consumer
            foreach forbidden_arg [list max_waiting] {
                if {[dict exists $config_dict $forbidden_arg]} {
                    throw {NATS ErrInvalidArg} "Argument $forbidden_arg is forbbiden for push consumer"
                }
            }
        } else {
            # pull consumer
            foreach forbidden_arg [list idle_heartbeat flow_control] {
                if {[dict exists $config_dict $forbidden_arg]} {
                    throw {NATS ErrInvalidArg} "Argument $forbidden_arg is forbbiden for pull consumer"
                }
            }            
        }

        # string arguments need to be within quotation marks ""
        dict for {key value} $config_dict {
            if {![string is boolean -strict $value] && ![string is double -strict $value]} {
                dict set config_dict $key [json::write::string $value]
            }            
        }
        
        # create durable or ephemeral consumers
        if {[info exists durable_consumer_name]} {
            set subject "\$JS.API.CONSUMER.DURABLE.CREATE.$stream.$durable_consumer_name"
            set settings_json [::nats::_json_write_object \
                stream_name [json::write::string $stream] \
                name [json::write::string $durable_consumer_name] \
                config [::nats::_json_write_object {*}$config_dict] \
            ]
        } else {
            set subject "\$JS.API.CONSUMER.CREATE.$stream"
            set settings_json [::nats::_json_write_object \
                stream_name [json::write::string $stream] \
                config [::nats::_json_write_object {*}$config_dict] \
            ]
        }

        return [my SimpleRequest $subject $common_arguments "Creating consumer for $stream timed out" $settings_json]                
    }
    
    # no request body
    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_delete_response
    method delete_consumer {stream consumer args} {
        set subject "\$JS.API.CONSUMER.DELETE.$stream.$consumer"
        return [my SimpleRequest $subject $args "Deleting consumer $stream $consumer timed out"]         
    }
    
    # TODO split into 2 methods
    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_info_response
    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_list_request
    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_list_response
    method consumer_info {stream {consumer ""} args} {
        if {$consumer eq ""} {
            set subject "\$JS.API.CONSUMER.LIST.$stream"
            set timeout_message "Getting consumer list from $stream timed out"
        } else {
            set subject "\$JS.API.CONSUMER.INFO.$stream.$consumer"
            set timeout_message "Getting consumer info from $stream named $consumer timed out"
        }
        return [my SimpleRequest $subject $args $timeout_message]               
    }
    
    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_names_request
    # nats schema info --yaml io.nats.jetstream.api.v1.consumer_names_response
    # TODO add -subject filter
    method consumer_names {stream args} {
        set subject "\$JS.API.CONSUMER.NAMES.$stream"
        return [my SimpleRequest $subject $args "Getting consumer names from $stream timed out"]    
    }

    # nats schema info --yaml io.nats.jetstream.api.v1.stream_create_request
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_create_response
    # TODO update_stream?
    method add_stream {stream args} {
        if {![${conn}::my CheckSubject $stream]} {
            throw {NATS ErrInvalidArg} "Invalid stream name $stream"
        }

        set common_arguments [dict create]
        set config_dict [dict create] ;# variable with formatted arguments

        # supported arguments
        set arguments_list [list subjects retention max_consumers max_msgs max_bytes \
            max_age max_msgs_per_subject max_msg_size discard storage num_replicas duplicate_window \
            sealed deny_delete deny_purge allow_rollup_hdrs]


        foreach {opt val} $args {
            switch -- $opt {
                -subjects {
                    dict set config_dict subjects [::json::write array {*}[lmap subject $val {::json::write string $subject}]]
                }
                default {
                    set opt_raw [string range $opt 1 end] ;# remove flag
                    # duration args - provided in milliseconds should be formatted to nanoseconds 
                    if {$opt_raw in [list "duplicate_window" "max_age"]} {
                        if {![string is double -strict $val]} {
                            throw {NATS ErrInvalidArg} "Wrong duration value for argument $opt_raw it must be in milliseconds"
                        }
                        set val [expr {entier($val*1000*1000)}] ;#conversion milliseconds to nanoseconds
                    }
                    
                    # checking if all provided arguments are valid
                    if {$opt_raw ni $arguments_list} {
                        throw {NATS ErrInvalidArg} "Unknown option $opt"
                    }

                    if {![string is boolean -strict $val] && ![string is double -strict $val]} {
                        dict set config_dict $opt_raw [::json::write string $val]
                    } else {
                        dict set config_dict $opt_raw $val
                    }                 
                }
            }
        }
        
        set subject "\$JS.API.STREAM.CREATE.$stream"
        dict set config_dict name [::json::write string $stream]
        set settings_json [::nats::_json_write_object {*}$config_dict]
        return [my SimpleRequest $subject $common_arguments "Creating stream $stream timed out" $settings_json]
    }
    # no request body
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_delete_response
    method delete_stream {stream args} {
        set subject "\$JS.API.STREAM.DELETE.$stream"
        return [my SimpleRequest $subject $args "Deleting stream $stream timed out"]         
    }
    
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_purge_request
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_purge_response
    method purge_stream {stream args} {
        set subject "\$JS.API.STREAM.PURGE.$stream"
        return [my SimpleRequest $subject $args "Purging stream $stream timed out"]         
    }
    # TODO split in 2 methods
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_info_request
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_info_response
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_list_request
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_list_response
    method stream_info {{stream ""} args} {
        if {$stream eq ""} {
            set subject "\$JS.API.STREAM.LIST"
            set timeout_message "Getting stream list timed out"
        } else {
            set subject "\$JS.API.STREAM.INFO.$stream"
            set timeout_message "Getting stream info for $stream timed out"
        }
        return [my SimpleRequest $subject $args $timeout_message]                
    }
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_names_request
    # nats schema info --yaml io.nats.jetstream.api.v1.stream_names_response
    method stream_names {args} {
        set subject "\$JS.API.STREAM.NAMES"
        return [my SimpleRequest $subject $args "Getting stream names timed out"]
    }

    method SimpleRequest {subject common_arguments timeout_message {msg {}}} {
        if {![${conn}::my CheckSubject $subject]} {
            throw {NATS ErrInvalidArg} "Invalid stream or consumer name (target subject: $subject)"
        }

        set callback ""
        foreach {opt val} $common_arguments {
            switch -- $opt {
                -callback {
                    # receive status of adding consumer on callback proc
                    set callback [mymethod PublishCallback $val]
                }
                default {
                    throw {NATS ErrInvalidArg} "Unknown option $opt"        
                }
            }
        }

        try {
            set result [$conn request $subject $msg -dictmsg true -timeout $timeout -callback $callback -max_msgs 1]
        } trap {NATS ErrTimeout} err {
            throw {NATS ErrTimeout} $timeout_message
        }
        if {$callback ne ""} {
            return
        }
        
        # can throw nats server error
        return [nats::_parsePublishResponse $result] 
    }

    method PublishCallback {userCallback timedOut result} {
        if {$timedOut} {
            after 0 [list {*}$userCallback 1 "" ""]
            return
        }

        try {
            set pubAckResponse [nats::_parsePublishResponse $result]
            after 0 [list {*}$userCallback 0 $pubAckResponse ""]
        } trap {NATS ErrJSResponse} {msg opt} {
            # make a dict with the same structure as AsyncError
            # but the error code should be the same as reported in JSON, so remove "NATS ErrJSResponse" from it
            set errorCode [lindex [dict get $opt -errorcode] end]
            after 0 [list {*}$userCallback 0 "" [dict create code $errorCode errorMessage $msg]]
        } on error {msg opt} {
            [$conn logger]::error "Error while parsing JetStream response: $msg"
        }
    }
}

# nats schema info --yaml io.nats.jetstream.api.v1.pub_ack_response
proc ::nats::_parsePublishResponse {response} {
    # $response is a dict here
    try {
        set responseDict [::json::json2dict [dict get $response data]]

        if {[dict exists $responseDict type] && [string match "*stream_msg_get_response" [dict get $responseDict type]]} {
            if {[dict exists $responseDict message data]} {
                dict set responseDict message data [binary decode base64 [dict get $responseDict message data]]
            }
        }
    } trap JSON err {
        throw {NATS ErrInvalidJSAck} "JSON parsing error $err\n while parsing the stream response: $response"
    }
    # https://docs.nats.io/jetstream/nats_api_reference#error-handling
    # looks like nats.go doesn't have a specific error type for this? see func (js *js) PublishMsg
    # should I do anything with err_code?
    if {[dict exists $responseDict error]} {
        set errDict [dict get $responseDict error]
        throw [list NATS ErrJSResponse [dict get $errDict code]] [dict get $errDict description]
    }
    return $responseDict
}

proc ::nats::_format_json {name val type} {
    set errMsg "Invalid value for the $type option $name : $val"
    switch -- $type {
        valid_str {
            if {[string length $val] == 0} {
                throw {NATS ErrInvalidArg} $errMsg
            }
            return [json::write string $val]
        }
        int {
            if {![string is entier -strict $val]} {
                throw {NATS ErrInvalidArg} $errMsg
            }
            return $val
        }
        bool {
            if {![string is boolean -strict $val]} {
                throw {NATS ErrInvalidArg} $errMsg
            }
            return [expr $val? "true" : "false"]
        }
        list {
            if {[llength $val] == 0} {
                throw {NATS ErrInvalidArg} $errMsg
            }
            return [json::write array {*}[lmap element $val {
                        json::write string $element
                    }]]
        }
        ns {
            if {![string is entier -strict $val]} {
                throw {NATS ErrInvalidArg} $errMsg
            }
            # val must be in milliseconds
            return [expr {entier($val*1000*1000)}]
        }
        default {
            throw {NATS ErrInvalidArg} "Wrong type $type"  ;# should not happen
        }
    }
}

proc ::nats::_format_enum {name val type} {
    set allowed_vals [lrange $type 1 end] ;# drop the 1st element "enum"
    if {$val ni $allowed_vals} {
        throw {NATS ErrInvalidArg} "Invalid value for the enum $name : $val; allowed values: $allowed_vals"
    }
    return [json::write string $val]
}

proc ::nats::_choose_format {name val type} {
    if {[lindex $type 0] eq "enum"} {
        return [_format_enum $name $val $type]
    } else {
        return [_format_json $name $val $type]
    }
}

proc ::nats::_local2json {spec} {
    set json_dict [dict create]
    foreach {name type def} $spec {
        try {
            # is there a local variable with this name in the calling proc?
            set val [uplevel 1 [list set $name]]
            dict set json_dict $name [_choose_format $name $val $type]
        } trap {TCL LOOKUP VARNAME} {err errOpts} {
            # no local variable exists, so take a default value from the spec, unless it's required
            if {$def eq "NATS_TCL_REQUIRED"} {
                throw {NATS ErrInvalidArg} "Option $name is required"
            }
            if {$def ne "null"} {
                dict set json_dict $name [_choose_format $name $def $type]
            }
        }
    }
    if {[dict size $json_dict]} {
        return [json::write::object {*}$json_dict]
    } else {
        return ""
    }
}

proc ::nats::_dict2json {spec src} {
    set json_dict [dict create]
    foreach {k v} $src {
        dict set src_dict [string trimleft $k -] $v
    }
    foreach {name type def} $spec {
        set val [dict lookup $src_dict $name $def]
        if {$val eq "NATS_TCL_REQUIRED"} {
            throw {NATS ErrInvalidArg} "Option $name is required"
        }
        if {$val ne "null"} {
            dict set json_dict $name [_choose_format $name $def $type]
        }
    }
    if {[dict size $json_dict]} {
        return [json::write::object {*}$json_dict]
    } else {
        return ""
    }
}
