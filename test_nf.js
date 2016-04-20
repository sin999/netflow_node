var Collector = require('node-netflowv9');
var ip = require('ip');
var Deque = require("collections/deque");
var fs = require('fs');
var outFile=null;
var currentDate=new Date();
var deque = new Deque();
var path ="./logs/";


function addMinutes(date, minutes) {
    return new Date(date.getTime() + minutes*60000);
}

var timeout = function(){
    currentDate=new Date();
    var tempDate=new Date();
    tempDate.setSeconds(0);
    var nextTimeoutDate=new Date(addMinutes(tempDate,1));
    setTimeout(timeout, nextTimeoutDate.getTime()-currentDate.getTime());
    outFile = fs.createWriteStream(getFileName(currentDate));
}

function getFileName(date){
	
	var d =	date.getFullYear()	*100000000+
		(date.getMonth()+1)	*1000000+
		date.getDate()     	*10000+
		date.getHours()		*60+
		date.getMinutes();
	return path+"nat"+d+".flog"	
}

timeout();

function formRecord(f){
	return {evt:f.natEvent,
	    vrf:f.ingressVRFID,
	    time:f.observationTimeMilliseconds,
	    sip:ip.toLong(f.ipv4_src_addr),
	    xsip:ip.toLong(f.postNATSourceIPv4Address),
	    xsport:f.postNAPTSourceTransportPort,
	    xdip:ip.toLong(f.postNATDestinationIPv4Address),
	    xdport:f.postNAPTDestinationTransportPort
	    };
}

Collector({port: 2056,ipv4num: true}).on('data',function(flow) {
    flow.flows.forEach(function(f){
	if(deque.length<100000){
	    deque.push(formRecord(f));
	}else{
	    console.log("################ BUFFER OVERFLOWED!! ####################################");
	}
    })
});


deque.addRangeChangeListener(function(plus,minus,index){
    if(plus!=undefined && plus.length>0){
	while (deque.length) {
    	    outFile.write(JSON.stringify(deque.pop())+"\n");
	}
    }
}, false)
