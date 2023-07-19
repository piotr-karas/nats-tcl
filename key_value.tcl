# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and  limitations under the License.

namespace eval ::nats {}

oo::class create ::nats::key_value {
  variable jetStream

  constructor {js} {
    set jetStream $js
  }

  method get {bucket key} {
    set stream "KV_${bucket}"
    set subject "\$KV.${bucket}.${key}"

    # handle case when no values have been set for this key
    try {
      set resp [$jetStream stream_msg_get $stream -last_by_subj $subject]
    } trap {NATS ErrJSResponse 404} {} {
      throw {NATS KeyNotFound} "Key ${key} not found"
    } 

    set msg $resp

    # handle case when key value has been deleted or purged
    if {[dict exists $msg header KV-Operation]} {
      set op [dict get $msg header KV-Operation]
      if {$op in [list "DEL" "PURGE"]} {
        throw {NATS KeyNotFound} "Key ${key} not found"
      }
    }

    return [my message_to_entry $msg]
  }

  method put {bucket key value} {
    set subject "\$KV.${bucket}.${key}"
    set resp [$jetStream publish $subject [dict create data $value]]
    return [dict get $resp seq]
  }

  method del {bucket {key ""}} {
    if {$key ne ""} {
      set subject "\$KV.${bucket}.${key}"
      set resp [$jetStream publish $subject [dict create data "" header [list KV-Operation DEL]]]
      return
    }

    set stream "KV_${bucket}"
    $jetStream delete_stream $stream

    return 
  }

####### ADDING IS UNTESTED ########
# method add {bucket args} {
#  set stream "KV_${bucket}"
#     set subject "\$KV.${bucket}.>"
#     set options {
#       -storage file
#       -retention limits
#       -discard new
#       -max_msgs_per_subject 1
#       -num_replicas 1
#       -max_msgs -1
#       -max_msg_size -1
#       -max_bytes -1
#       -max_age -1
#       -duplicate_window 120000
#       -deny_delete 1
#       -deny_purge 0
#       -allow_rollup_hdrs 1
#     }

#     set argsMap {
#       -history -max_msgs_per_subject
#       -storage -storage
#       -ttl -max_age
#       -replicas -num_replicas
#       -max_value_size -max_msg_size
#       -max_bucket_size -max_bytes
#     }

#     dict for {key value} $args {
#       if {![dict exists $argsMap $key]} {
#         throw {NATS ErrInvalidArg} "Unknown option ${key}"
#       }
#       switch $key -- {
#         -history {
#           if {$value < 1} {
#             throw {NATS ErrInvalidArg} "max_msgs_per_subject must be greater than 0"
#           }
#           if {$value > 64} {
#             throw {NATS ErrInvalidArg} "max_msgs_per_subject must be less than 64"
#           }

#           dict set options [dict get $argsMap $key] $value
#         }
#         default {
#           dict set options [dict get $argsMap $key] $value
#         }
#       }
#     }
#     puts $options

#     $jetStream add_stream $stream -subjects $subject {*}$options

#     return 
#   }

  method purge {bucket key} {
    set subject "\$KV.${bucket}.${key}"
    set resp [$jetStream publish $subject [dict create data "" header [list KV-Operation PURGE Nats-Rollup sub]]]
    return $resp
  }

  method message_to_entry {msg} {
    # return dict representation of Entry https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-8.md#entry
    lassign [my subject_to_bucket_key [dict get $msg subject]] bucket key
    set value ""
    if {[dict exists $msg data]} {
      set value [dict get $msg data]
    }
    set headers [dict create]
    if {[dict exist $msg headers]} {
      set headers [dict get $msg headers]
    }
    set operation [my headers_to_operation $headers]
    return [dict create \
      value $value \
      revision [dict get $msg seq] \
      created [::nats::time_to_millis [dict get $msg time]] \
      bucket $bucket \
      key $key \
      operation $operation]
  }

  method subject_to_bucket_key {subject} {
    # return list of bucket and key from subject
    set parts [split $subject "."]
    set bucket [lindex $parts 1]
    set key [lindex $parts 2]
    return [list $bucket $key]
  }

  method headers_to_operation {headers} {
    # return operation from headers
    if {[dict exists $headers KV-Operation]} {
      return [dict get $headers KV-Operation]
    }
    return "PUT"
  }
}

# 2023-05-30T07:06:22.864305Z
proc ::nats::time_to_millis {time} {
  set millis 0
  if {[regexp -all {^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d{6})Z$} $time -> year month day hour minute second micro]} {
    set micro [string trimleft $micro 0]
    set millis [clock scan "${year}-${month}-${day} ${hour}:${minute}:${second}" -format "%Y-%m-%d %T" -gmt 1]
    return  [expr {$millis + ($micro / 1000)}]
  }

  throw {NATS InvalidTime} "Invalid time format ${time}"
}