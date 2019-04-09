ruleset gossip {
  meta {
    shares __testing, get_rumors, get_peer_data, get_peer_scores
    use module io.picolabs.subscription alias Subscriptions
    use module temperature_store
    use module io.picolabs.wrangler alias Wrangler
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" }
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
      //, { "domain": "d2", "type": "t2", "attrs": [ "a1", "a2" ] }
      ]
    }
    
    get_message_num = function() {
      ent:message_number.defaultsTo(0)
    }
    
    get_peer = function() {
      Subscriptions:established("Tx_role","node").klog("subs").sort(function(a,b){
        aVal = get_peer_scores(){engine:getPicoIDByECI(a{"Tx"})}.defaultsTo(0);
        bVal = get_peer_scores(){engine:getPicoIDByECI(b{"Tx"})}.defaultsTo(0);
        aVal < bVal
      }).head(){"Tx"}
    }
    
    get_peer_scores = function() {
      ent:peer_scores.defaultsTo({})
    }
    
    get_peer_data = function() {
      ent:peers.defaultsTo({})
    }
    
    get_peer_message_nums = function() {
      get_peer_data().map(function(v,k){/*k + ":" + */get_last_contiguous(v)})
    }
   
    get_last_contiguous = function(list) {
     sl = list.sort();
     sl.filter(function(e){list.none(function(a){a == e + 1})}).head()
    }
    
    get_rumors = function() {
      ent:rumors.defaultsTo({})
    }
    
    get_interval = function() {
      4
    }
    
    get_status = function() {
      ent:status.defaultsTo("on")
    }
    
    get_first_missing = function(messageDict) {
      m = messageDict;
      get_rumors().filter(function(v, k){
        picoId = k.split(re#:#).head();
        messageDict{picoId}.isnull() || not messageDict.filter(function(vv, kk){(kk + ":" + (vv + 1)) == k}).keys().head().isnull()
      }).klog("filtered").values().head().klog("Chosen")
    }
    
    prepare_message = function(message_type) {
      (message_type == "rumor")
      => rumor_message()
      |  seen_message()
    }
    
    rumor_message = function() {
     index = random:integer(ent:rumor.keys().length() - 1);
      //key = ent:rumor.keys()[key_index];
      ent:rumors.values()[index].klog("Rumor generated: ")
    }
    
    seen_message = function() {
      get_peer_message_nums()
    }
    
    my_message = function() {
      temp = temperature_store:temperatures().reverse().head().defaultsTo("None");
      (temp != "None") => {
        "message_id" : meta:picoId + ":" + get_message_num(),
        "sensor_id" : meta:picoId,
        "sequence" : get_message_num(),
        "temperature" : temp["temp"],
        "timestamp" : temp["timestamp"]
      } | null
    }
    
    
    
  }
  
  
 rule gossip_hearbeat {
   select when gossip heartbeat where get_status() == "on"
   pre {
     peer = get_peer().klog("peer: ")
     message_type = (random:integer(5) <= 3) => "rumor" | "seen";
     message = prepare_message(message_type).klog("Sending: ")
   }
   if (not message.isnull() && not peer.isnull()) then
    event:send({
      "eci" : peer,
      "domain" : "gossip",
      "type" : message_type,
      "attrs" : {
        "picoId" : meta:picoId,
        "respond_eci" : Subscriptions:established("Tx", peer)[0]{"Rx"},
        "message" : message
      }
    })
    always {
      schedule gossip event "heartbeat" at time:add(time:now(), {"seconds" : get_interval()});
      raise gossip event "my_rumor";
    }
 }
 
rule my_gossip {
  select when gossip my_rumor
  pre {
    message = my_message().klog("my msg: ");
    duplicate = message["timestamp"].klog("my time") == get_rumors(){meta:picoId + ":" + (get_message_num() - 1)}["timestamp"].klog("other time")
    msg_num = (get_message_num()).klog("msgNum")
  }
  if (not duplicate && not message.isnull()) then noop()
  fired {
    ent:rumors{meta:picoId + ":" + msg_num} := message;
    ent:peers := ent:peers.defaultsTo({});
    ent:peers{meta:picoId} := ent:peers{meta:picoId}.defaultsTo([]).append(msg_num);
    ent:message_number := msg_num + 1;
  }
}
 
 
 rule gossip_rumor {
   select when gossip rumor 
   pre {
     a = event:attrs.klog("attrs: ")
     message = event:attrs["message"].klog("msg: ")
     message_id = message{"message_id"}
     peer_id = message_id.split(re#:#).head()
     sequence_number = message["sequence"]
     needed = get_rumors(){message_id}.isnull()
   }
   if (needed && not message_id.isnull()) then noop()
   fired {
     ent:rumors{message_id} := message;
     ent:peers{peer_id} := ent:peers{peer_id}.defaultsTo([]).append(sequence_number);
     ent:peer_scores := ent:peer_scores.defaultsTo({});
     ent:peer_scores{peer_id} := ent:peer_scores{peer_id}.defaultsTo(0) + 1;
   }
 }
 
 rule gossip_seen {
   select when gossip seen
   pre {
     peer_id = event:attrs["picoId"].klog("Gossip Peer")
     message = event:attrs["message"].klog("msg: ")
     peer_address = event:attrs["respond_eci"].klog("peer_address: ")
     response = get_first_missing(message).klog("seen response")
   }
   if not response.isnull() then
    event:send({
      "eci" : peer_address,
      "domain" : "gossip",
      "type" : "rumor", 
      "attrs" : {
        "pico_id" : meta:picoId,
        "message" : response
      }
    }) always {
      ent:peer_scores := ent:peer_scores.defaultsTo({});
      ent:peer_scores{peer_id} := message.values().reduce(function(a, b){a + b}) + message.values().length();
    }
 }
 
 rule process {
   select when gossip process
   pre {
     status = event:attrs["status"]
   }
   noop()
   always {
     ent:status := status 
   }
 }
 
 rule bootstrap {
   select when wrangler ruleset_added where rids >< meta:rid
   pre {
     interval = get_interval()
   }
   always {
     schedule gossip event "heartbeat" at time:add(time:now(), {"seconds" : interval})
   }
 }
 
 
rule add_sub {
    select when gossip connect
    pre {
      name = event:attrs["name"]
      eci = event:attrs["eci"]
    }
    noop()
    fired {
      raise wrangler event "subscription" attributes {
        "name" : name,
        "Rx_role": "node",
        "Tx_role": "node",
        "Tx_host": meta:host,
        "channel_type": "subscription",
        "wellKnown_Tx" : eci
      };
    }
  }
  
 
  rule auto_accept {
    select when wrangler inbound_pending_subscription_added
    fired {
      raise wrangler event "pending_subscription_approval"
        attributes event:attrs
    }
  }
}
