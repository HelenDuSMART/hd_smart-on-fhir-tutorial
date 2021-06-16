######################################################################
# Name:         tps_candelis_filter.tcl
# Purpose:      Determine whether the message should be sent to Candelis or not.
#               This will be based on whether or not the schedule applies to 
#               a "Mammo Prefetch" resource group or not. If it doesn't, we don't want it here.
# UPoC type:    tps
# Args:         tps keyedlist containing the following keys:
#               MODE    run mode ("start", "run" or "time")
#               MSGID   message handle
#               ARGS    user-supplied arguments:
#
# History:    
# Nov 06, 2017  Aaron Andersen      Original code
# Jul 30, 2018 Brian CHapman        Update to use DSN.tbl, overhaul code
proc tps_candelis_filter { args } {

    keylget args MODE mode
    keylget args ARGS userArgs

    set dispList {}

    switch -exact -- $mode {

        run {

            # we have found in te sting that sometimes the data hasn't finished being committed
            # to the tables by the time this message gets here and is being processed. 
            # pause for a few seconds to give the backend a little time to get there 
            # If we still don't have what we need after this pause, we will not send the message (done later)
            sleeps 2


            package require HL7Tools
            keylget args MSGID mh
            set msg [msgget $mh]
            set SCH [HL7Tools::HL7getSegment $msg SCH]
            set SCH_1 [HL7Tools::HL7getSubfield $SCH 1 1 ^]
            set debug 0
            keylget userArgs DEBUG debug
            if { $debug } {echo "SCH 1 is::: $SCH_1"}

            set dispos 0
            set DSN [hcitbllookup DSN candelis]

            if { [catch {

                package require -exact lhodbc 1.0
                lhodbc::connect $DSN

                # in Cert/Prod 2065430.00 is the Resource_id for MammoPrefetch on SCH_RES_GROUP
                # role_cd 54970 = Patient. We don't need patient records, only the resource records.
                foreach row [lhodbc::runQuery "select resource_cd from sch_appt where sch_event_id = $SCH_1 and sch_role_cd != 54970"  ] {
                #foreach row [lhodbc::runQuery "select resource_cd from sch_appt where sch_event_id = $SCH_1"  ] 
                    if { $debug } { echo "resource_cd is>>>>>> $row" }
                    
                    set res_group_id [lhodbc::runQuery "select res_group_id from sch_res_list where resource_cd = $row"]
                    foreach res_group_id [lhodbc::runQuery "select res_group_id from sch_res_list where resource_cd = $row"] {
                        if { $debug } { echo "res_group_id>>>>>$res_group_id" }
                        if {$res_group_id eq "2065430" } {
                            # this is a mammo prefetch row that we want.
                            if { $debug } { echo "Mammo prefetch found, keep this message" }
                            set dispos 1
                            break
                        }
                    }
                }

            } err] } {

                set process [file tail [pwd]]

                # an unexpected error has occurred while working with the database
                echo "shutting down the process because of database error:"
                echo $err

                catch {

                    global HciSite
                    lhodbc::connect lhodbc_log

                    # log the db crash including all relevant details
                    # the lhodbc.lhodbc_log database will automatically include the current date and time on every insert
                    lhodbc::runQuery "insert into lhodbc.lhodbc_log (DSN, HciSite, process, filename, err) values ('$DSN', '$HciSite', '$process', 'tps_candelis_filter.tcl', '$err')"

                    # count how many db crashes have occurred in the last 10 minutes
                    # this query should be tailored per interface to be as specific or generic as required
                    set count [lhodbc::runQuery "select count(*) from lhodbc.lhodbc_log where date > (LOCALTIMESTAMP - INTERVAL '10 minutes') and DSN='$DSN' and HciSite='$HciSite' and process='$process'"]

                    if { $count < 4 } {
                        # execute the try-process-restart script in a minute from now which will hopefully be enough time for the hcienginestop call to shut this process down
                        # the try-process-restart will attempt to restart the process for roughly 3 minutes and then give up afterward if it cannot restart the process
                        exec echo "try-process-restart --site=$HciSite --process=$process" | at now + 1 minute
                    }

                }
                # shut down the process knowing that the try-process-restart script will attempt to restart the process in a minute from now
                exec hcienginestop -p $process
            }

            lhodbc::disconnect
            
            #decide what we want to do witht he message now.
            if {$dispos} {
                if { $debug } { echo "Continue message" }
                lappend dispList "CONTINUE $mh"
            } else {
                if { $debug } { echo "Kill the message" }
                lappend dispList "KILL $mh"
            }
        }
    }
    return $dispList
}

###############################################################################
# simple sleep command. Takes in # of seconds to sleep
###############################################################################

proc sleeps {N} {
   after [expr {int($N * 1000)}]
}
