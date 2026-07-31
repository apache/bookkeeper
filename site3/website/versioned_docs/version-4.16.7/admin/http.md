---
id: http
title: BookKeeper Admin REST API
---

This document introduces BookKeeper HTTP endpoints, which can be used for BookKeeper administration.
To use this feature, set `httpServerEnabled` to `true` in file `conf/bk_server.conf`.

## All the endpoints

Currently all the HTTP endpoints could be divided into these 5 components:
1. Heartbeat: heartbeat for a specific bookie.
2. Config: doing the server configuration for a specific bookie.
3. Ledger: HTTP endpoints related to ledgers.
4. Bookie: HTTP endpoints related to bookies.
5. AutoRecovery: HTTP endpoints related to auto recovery.

## Heartbeat

### Endpoint: /heartbeat
* Method: GET
* Description: Get heartbeat status for a specific bookie
* Response: 

    | Code   | Description |
    |:-------|:------------|
    |200 | Successful operation |

## Config

### Endpoint: /api/v1/config/server_config
1. Method: GET
    * Description: Get value of all configured values overridden on local server config
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
1. Method: PUT
    * Description: Update a local server config
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |configName  | String | Yes | Configuration name(key) |
        |configValue | String | Yes | Configuration value(value) |
    * Body: 
        ```json
        {
           "configName1": "configValue1",
           "configName2": "configValue2"
        }
        ```
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

## Metrics

### Endpoint: /metrics
1. Method: GET
    * Description: Get all metrics by calling `writeAllMetrics()` of `statsProvider` internally
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

## Ledger

### Endpoint: /api/v1/ledger/delete/?ledger_id=&lt;ledger_id&gt;
1. Method: DELETE
    * Description: Delete a ledger.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |ledger_id  | Long | Yes | ledger id of the ledger.  |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/ledger/list/?print_metadata=&lt;metadata&gt;
1. Method: GET
    * Description: List all the ledgers.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |print_metadata | Boolean | No |  whether print out metadata  |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "ledgerId1": "ledgerMetadata1",
          "ledgerId2": "ledgerMetadata2",
          ...
        }
        ```

### Endpoint: /api/v1/ledger/metadata/?ledger_id=&lt;ledger_id&gt;
1. Method: GET
    * Description: Get the metadata of a ledger.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |ledger_id  | Long | Yes | ledger id of the ledger.  |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "ledgerId1": "ledgerMetadata1"
        }
        ```    

### Endpoint: /api/v1/ledger/read/?ledger_id=&lt;ledger_id&gt;&start_entry_id=&lt;start_entry_id&gt;&end_entry_id=&lt;end_entry_id&gt;
1. Method: GET
    * Description: Read a range of entries from ledger.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |ledger_id       | Long | Yes| ledger id of the ledger.  |
        |start_entry_id  | Long | No | start entry id of read range. |
        |end_entry_id    | Long | No | end entry id of read range.  |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "entryId1": "entry content 1",
          "entryId2": "entry content 2",
          ...
        }
        ```    

## Bookie

### Endpoint: /api/v1/bookie/info
1. Method: GET
   * Description:  Get bookie info
   * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |501 | Not implemented |
   * Body:
      ```json
      {
         "freeSpace" : 0,
         "totalSpace" : 0
       }
      ```

### Endpoint: /api/v1/bookie/list_bookies/?type=&lt;type&gt;&print_hostnames=&lt;hostnames&gt;
1. Method: GET
    * Description:  Get all the available bookies.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |type            | String  | Yes |  value: "rw" or "ro" , list read-write/read-only bookies. |
        |print_hostnames | Boolean | No  |  whether print hostname of bookies. |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "bookieSocketAddress1": "hostname1",
          "bookieSocketAddress2": "hostname2",
          ...
        }
        ```    

### Endpoint: /api/v1/bookie/list_bookie_info
1. Method: GET
    * Description:  Get bookies disk usage info of this cluster.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "bookieAddress" : {free: xxx, total: xxx},
          "bookieAddress" : {free: xxx, total: xxx},
          ...
          "clusterInfo" : {total_free: xxx, total: xxx}
        }
        ```    

### Endpoint: /api/v1/bookie/cluster_info
1. Method: GET
    * Description:  Get top-level info of this cluster.
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |404 | Not found |
    * Response Body format:

        ```json
        {
          "auditorElected" : false,
          "auditorId" : "",
          "clusterUnderReplicated" : false,
          "ledgerReplicationEnabled" : true,
          "totalBookiesCount" : 1,
          "writableBookiesCount" : 1,
          "readonlyBookiesCount" : 0,
          "unavailableBookiesCount" : 0
        }
        ```    
   `clusterUnderReplicated` is true if there is any underreplicated ledger known currently. 
    Trigger audit to increase precision. Audit might not be possible if `auditorElected` is false or
    `ledgerReplicationEnabled` is false.

   `totalBookiesCount` = `writableBookiesCount` + `readonlyBookiesCount` + `unavailableBookiesCount`.


### Endpoint: /api/v1/bookie/last_log_mark
1. Method: GET
    * Description:  Get the last log marker.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          JournalId1 : position1,
          JournalId2 : position2,
          ...
        }
        ```   

### Endpoint: /api/v1/bookie/list_disk_file/?file_type=&lt;type&gt;
1. Method: GET
    * Description:  Get all the files on disk of current bookie.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |type  | String | No | file type: journal/entrylog/index. |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "journal files" : "filename1 filename2 ...",
          "entrylog files" : "filename1 filename2...",
          "index files" : "filename1 filename2 ..."
        }
        ```

### Endpoint: /api/v1/bookie/expand_storage
1. Method: PUT
    * Description:  Expand storage for a bookie.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/bookie/gc
1. Method: PUT
    * Description:  trigger gc for this bookie.
    * Parameters:
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |forceMajor  | Boolean | No | only trigger the forceMajor gc for this bookie. |
        |forceMinor  | Boolean | No | only trigger the forceMinor gc for this bookie. |
    * Response:  

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

1. Method: GET
    * Description:  whether force triggered Garbage Collection is running or not for this bookie. true for is running.
    * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Body:
       ```json
       {
          "is_in_force_gc" : "false"
       }
       ```

### Endpoint: /api/v1/bookie/gc_details
1. Method: GET
    * Description:  get details of Garbage Collection Thread, like whether it is in compacting, last compaction time, compaction counter, etc.
    * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Body:
       ```json
       [ {
          "forceCompacting" : false,
          "majorCompacting" : false,
          "minorCompacting" : false,
          "lastMajorCompactionTime" : 1544578144944,
          "lastMinorCompactionTime" : 1544578144944,
          "majorCompactionCounter" : 1,
          "minorCompactionCounter" : 0
        } ]
       ```
### Endpoint: /api/v1/bookie/gc/suspend_compaction
1. Method: PUT
    * Description:  suspend the next compaction stage for this bookie.
    * Body:
         ```json
         {
            "suspendMajor": "true",
            "suspendMinor": "true"
         }
         ```
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |404 | Not found |

2. Method: GET
    * Description:  whether major or minor compaction  is suspending or not for this bookie. true for is running.
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |404 | Not found |
    * Body:
       ```json
       {
          "isMajorGcSuspended" : "true",
          "isMinorGcSuspended" : "true"
      
       }
       ```

### Endpoint: /api/v1/bookie/gc/resume_compaction
1. Method: PUT
    * Description:  resume the suspended compaction for this bookie.
    * Body:
         ```json
         {
            "resumeMajor": "true",
            "resumeMinor": "true"
         }
         ```
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |404 | Not found |

### Endpoint: /api/v1/bookie/state
1. Method: GET
   * Description:  Exposes the current state of bookie
   * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
   * Body:
      ```json
      {
         "running" : true,
         "readOnly" : false,
         "shuttingDown" : false,
         "availableForHighPriorityWrites" : true
       }
      ```

### Endpoint: /api/v1/bookie/sanity
1. Method: GET
   * Description:  Exposes the bookie sanity state
   * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
   * Body:
      ```json
      {
         "passed" : true,
         "readOnly" : false
       }
      ```


### Endpoint: /api/v1/bookie/is_ready
1. Method: GET
   * Description:  Return true if the bookie is ready
   * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
        |503 | Bookie is not ready |
   * Body: &lt;empty&gt;

### Endpoint: /api/v1/bookie/entry_location_compact
1. Method: PUT
    * Description:  trigger entry location index rocksDB compact. Trigger all entry location rocksDB compact, if entryLocations not be specified.
    * Parameters: 

      | Name | Type | Required | Description |
      |:-----|:-----|:---------|:------------|
      |entryLocationRocksDBCompact  | String | Yes | Configuration name(key) |
      |entryLocations | String | no | entry location rocksDB path |
    * Body:
         ```json
         {
            "entryLocationRocksDBCompact": "true",
            "entryLocations":"/data1/bookkeeper/ledgers/current/locations,/data2/bookkeeper/ledgers/current/locations"
         }
         ```
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |405 | Method Not Allowed |

2. Method: GET
    * Description:  All entry location index rocksDB compact status on bookie. true for is running.
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |405 | Method Not Allowed |
    * Body:
       ```json
       {
          "/data1/bookkeeper/ledgers/current/locations" : true,
          "/data2/bookkeeper/ledgers/current/locations" : false
       }
       ```

### Endpoint: /api/v1/bookie/state/readonly
1. Method: GET
    * Description: Get bookie readOnly state.
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |404 | Not found |
    * Body:
       ```json
       {
          "readOnly" : false
        }
       ```

2. Method: PUT
    * Description: Set bookie readOnly state.
    * Body:
        ```json
        {
          "readOnly": true
        }
        ```
    * Parameters:

      | Name | Type | Required | Description |
      |:-----|:-----|:---------|:------------|
      | readOnly | Boolean | Yes |  Whether bookie readOnly state. |
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |404 | Not found |
    * Body:
        ```json
        {
          "readOnly": true
        }
        ```


## Auto recovery

### Endpoint: /api/v1/autorecovery/status?enabled=&lt;boolean&gt;
1. Method: GET
    * Description:  Get autorecovery enable status with cluster.
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |404 | Not found |
    * Response Body format:

        ```json
        {
          "enabled": true
        }
        ```

2. Method: PUT
    * Description:  Set autorecovery enable status with cluster.
    * Parameters:

      | Name | Type | Required | Description                        |
      |:-----|:---------|:-----------------------------------|:------------|
      |enabled    | Boolean | Yes      | Whether autorecovery enable status |
    * Response:

      | Code   | Description |
      |:-------|:------------|
      |200 | Successful operation |
      |403 | Permission denied |
      |404 | Not found |
    * Body:
        ```json
        {
          "enabled": true
        }
        ```

### Endpoint: /api/v1/autorecovery/bookie/
1. Method: PUT
    * Description:  Ledger data recovery for failed bookie
    * Body: 
        ```json
        {
          "bookie_src": [ "bookie_src1", "bookie_src2"... ],
          "bookie_dest": [ "bookie_dest1", "bookie_dest2"... ],
          "delete_cookie": <bool_value>
        }
        ```
    * Parameters:
     
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |bookie_src    | Strings | Yes | bookie source to recovery |
        |bookie_dest   | Strings | No  | bookie data recovery destination |
        |delete_cookie | Boolean | No  | Whether delete cookie |
    * Response: 
     
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/autorecovery/list_under_replicated_ledger/?missingreplica=&lt;bookie_address&gt;&excludingmissingreplica=&lt;bookie_address&gt;
1. Method: GET
    * Description:  Get all under replicated ledgers.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |missingreplica          | String | No | missing replica bookieId |
        |excludingmissingreplica | String | No | exclude missing replica bookieId |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          [ledgerId1, ledgerId2...]
        }
        ```

### Endpoint: /api/v1/autorecovery/who_is_auditor
1. Method: GET
    * Description:  Get auditor bookie id.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "Auditor": "hostname/hostAddress:Port"
        }
        ```

### Endpoint: /api/v1/autorecovery/trigger_audit
1. Method: PUT
    * Description: Force trigger audit by resting the lostBookieRecoveryDelay.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/autorecovery/lost_bookie_recovery_delay
1. Method: GET
    * Description: Get lostBookieRecoveryDelay value in seconds.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

1. Method: PUT
    * Description: Set lostBookieRecoveryDelay value in seconds.
    * Body: 
        ```json
        {
          "delay_seconds": <delay_seconds>
        }
        ```
    * Parameters:
     
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        | delay_seconds | Long | Yes |  set delay value in seconds. |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/autorecovery/decommission
1. Method: PUT
    * Description: Decommission Bookie, Force trigger Audit task and make sure all the ledgers stored in the decommissioning bookie are replicated.
    * Body: 
        ```json
        {
          "bookie_src": <bookie_src>
        }
        ```
    * Parameters:
     
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        | bookie_src | String | Yes |   Bookie src to decommission.. |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
