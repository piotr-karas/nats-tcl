# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and  limitations under the License.

namespace eval ::nats {}

# TODO timestamps format
# TODO validation of keys/bucket (with "." inside)
# TODO documentation

# based on https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-8.md
oo::class create ::nats::key_value {
  variable js
  variable check_bucket_default

  constructor {jet_stream check_bucket} {
    set js $jet_stream
    set check_bucket_default $check_bucket
  }

  method get {bucket key args} {
    nats::_parse_args $args {
      revision pos_int null
    }

    set stream "KV_${bucket}"
    set subject "\$KV.${bucket}.${key}"

    try {
      if {[info exists revision]} {
        set resp [$js stream_msg_get $stream -seq $revision]

        if {[dict exists $resp subject] && [dict get $resp subject] ne $subject} {
          throw {NATS KeyNotFound} "Key ${key} not found"
        }
      } else {
        set resp [$js stream_msg_get $stream -last_by_subj $subject]
      }
    } trap {NATS ErrJSResponse 404 10037} {} {
      throw {NATS KeyNotFound} "Key ${key} not found"
    } trap {NATS ErrJSResponse 404 10059} {} {
      throw {NATS BucketNotFound} "Bucket ${bucket} not found"
    }
    
    set msg $resp

    # handle case when key value has been deleted or purged
    if {[dict exists $msg header]} {
      set operation [nats::header lookup $msg KV-Operation ""]
      if {$operation in [list "DEL" "PURGE"]} {
        throw {NATS KeyNotFound} "Key ${key} not found"
      }
    }

    return [my message_to_entry $msg]
  }

  method put {bucket key value args} {
    nats::_parse_args $args [list \
      check_bucket bool $check_bucket_default \
    ]

    if {$check_bucket} {
      my _checkBucket $bucket
    }

    set subject "\$KV.${bucket}.${key}"
    set resp [$js publish $subject $value]
    return [dict get $resp seq]
  }

  method create {bucket key value args} {
    nats::_parse_args $args [list \
      check_bucket bool $check_bucket_default \
    ]

    if {$check_bucket} {
      my _checkBucket $bucket
    }

    set subject "\$KV.${bucket}.${key}"

    set msg [nats::msg create $subject -data $value]
    nats::header set msg Nats-Expected-Last-Subject-Sequence 0
    
    try {
      set resp [$js publish_msg $msg]
    } trap {NATS ErrJSResponse 400 10071} {msg} {
      throw {NATS WrongLastSequence} $msg
    }

    return [dict get $resp seq]
  }

  method update {bucket key value revision args} {
    nats::_parse_args $args [list \
      check_bucket bool $check_bucket_default \
    ]

    if {$check_bucket} {
      my _checkBucket $bucket
    }

    set subject "\$KV.${bucket}.${key}"

    set msg [nats::msg create $subject -data $value]
    nats::header set msg Nats-Expected-Last-Subject-Sequence $revision

    try {
      set resp [$js publish_msg $msg]
    } trap {NATS ErrJSResponse 400 10071} {msg} {
      throw {NATS WrongLastSequence} $msg
    }

    return [dict get $resp seq]
  }

  method del {bucket args} {
    set key ""

    # first argument is not a flag - it is key name
    if {[string index [lindex $args 0] 0] ne "-"} {
      set key [lindex $args 0]
      set args [lrange $args 1 end]
    }

    nats::_parse_args $args [list \
      check_bucket bool $check_bucket_default \
    ]

    if {$check_bucket} {
      my _checkBucket $bucket
    }

    if {$key ne ""} {
      set subject "\$KV.${bucket}.${key}"
      set msg [nats::msg create $subject]
      nats::header set msg KV-Operation DEL
      set resp [$js publish_msg $msg]
      return
    }

    set stream "KV_${bucket}"
    try {
      $js delete_stream $stream
    } trap {NATS ErrJSResponse 404 10059} {} {
      throw {NATS BucketNotFound} "Bucket ${bucket} not found"
    }

    return
  }

  method purge {bucket key args} {
    nats::_parse_args $args [list \
      check_bucket bool $check_bucket_default \
    ]

    if {$check_bucket} {
      my _checkBucket $bucket
    }

    set subject "\$KV.${bucket}.${key}"

    set msg [nats::msg create $subject]
    nats::header set msg KV-Operation PURGE Nats-Rollup sub
    set resp [$js publish_msg $msg]

    return $resp
  }

  method revert {bucket key revision} {
    set entry [my get $bucket $key -revision $revision]

    set subject "\$KV.${bucket}.${key}"
    set resp [$js publish $subject [dict get $entry value]]
    return [dict get $resp seq]
  }

  # check stream info to know if given stream even exists, in order to not wait for timeout if it doesn't
  method _checkBucket {bucket} {
    set stream "KV_${bucket}"
    try {
      set stream_info [$js stream_info $stream]
    } trap {NATS ErrJSResponse 404 10059} {} {
      throw {NATS BucketNotFound} "Bucket ${bucket} not found"
    }
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

#     $js add_stream $stream -subjects $subject {*}$options

#     return 
#   }

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
  if {[regexp -all {^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d+)Z$} $time -> year month day hour minute second micro]} {
    set micro [string trimleft $micro 0]
    set millis [clock scan "${year}-${month}-${day} ${hour}:${minute}:${second}" -format "%Y-%m-%d %T" -gmt 1]
    return  [expr {$millis + ($micro / 1000)}]
  }

  throw {NATS InvalidTime} "Invalid time format ${time}"
}