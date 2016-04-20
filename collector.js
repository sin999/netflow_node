var Collector = require('node-netflowv9');
var ip = require('ip');
var Deque = require("collections/deque");
var fs = require('fs');

var collector=new TTK_Collector();



function TTK_Collector(){
    var self=this;
    var deque = new Deque();
    var outFile=null;
    var currentDate=new Date();
    var path ="./logs/";
    var port2listen="2056";
    var filePreffix="nat";
    var fileSuffix=".flog";
   
    this.addMinutes = function(date, minutes) {
        return new Date(date.getTime() + minutes*60000);
    };


    this.timeout = function(){
        currentDate=new Date();
        var tempDate=new Date();
        tempDate.setSeconds(0);
        var nextTimeoutDate=self.addMinutes(tempDate,1);
        setTimeout(self.timeout, nextTimeoutDate.getTime()-currentDate.getTime());
        if(outFile!=null) outFile.end();
        outFile = fs.createWriteStream(self.getFile(currentDate));
	deque=new Deque();
        deque.addRangeChangeListener(onQchanged,false);
    };

    this.getFile = function(){
	var date= new Date();
        var d = date.getFullYear()      *100000000+
            (date.getMonth()+1)     *1000000+
            date.getDate()          *10000+
            date.getHours()         *60+
            date.getMinutes();
        return path+filePreffix+d+fileSuffix;
    };

    this.formRecord = function(f){
        return {evt:f.natEvent,
            vrf:f.ingressVRFID,
            time:f.observationTimeMilliseconds,
            sip:ip.toLong(f.ipv4_src_addr),
            xsip:ip.toLong(f.postNATSourceIPv4Address),
            xsport:f.postNAPTSourceTransportPort,
            xdip:ip.toLong(f.postNATDestinationIPv4Address),
            xdport:f.postNAPTDestinationTransportPort};
    };

    this.start = function() {
        self.timeout();
        Collector({port: port2listen, ipv4num: true}).on('data', function (flow) {
            flow.flows.forEach(function (f) {
                if (deque.length < 100000) {
                    deque.push(self.formRecord(f));
                } else {
                    console.log("################ BUFFER OVERFLOWED!! ####################################");
                }
            })
        });
        
    };
    
    var onQchanged = function (plus, minus, index) {
	if (plus != undefined && plus.length > 0) {
    	    while (deque.length) {
		outFile.write(JSON.stringify(deque.pop()) + "\n");
	}}};
    
    
    
    this.start();
}

