# Copyright 2020 Petro Kazmirchuk https://github.com/Kazmirchuk
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

lappend auto_path [file normalize .]

package require tcltest
package require nats

proc callback {param subj msg reply} {
    puts "callback $param $subj $msg $reply"
}
set nats::debug true
#remember to parse login & passwd
nats::connect "nats://localhost:4222"
nats::subscribe foo [list callback one]
after 1000
nats::publish foo bla
#after 1000 nats::disconnect
vwait forever

#reply = nats::request <subj> <msg>
#nats::asyncRequest <subj> <msg> <commandPrefix>
#id = nats::subscribe <subj> <commandPrefix>
nats::unsubscribe <id>
#
#commandPrefix subj msg reply sub_id
#
#queue??