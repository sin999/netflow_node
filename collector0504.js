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
    var port2listen="2055";
    var filePreffix="nat";
    var fileSuffix=".flog";
    var queueMaxLength=10000;
    var recordCount=0;
    var packetCount=0;
   
    this.addMinutes = function(date, minutes) {
        return new Date(date.getTime() + minutes*10000);
    };

    this.toNextMinuteInterval = function(date) {
	    date = (date===undefined)?(new Date()):date;
	    var minInterval=10000;
	    var minuteInterval=60000;
	    var nextMin = new Date(date.getTime() + minuteInterval);
	    nextMin.setSeconds(0);
	    var interval=nextMin.getTime()-date.getTime();
	    interval+=(interval>minInterval)?0:self.toNextMinuteInterval(nextMin);
	    return interval;
    };

    this.timeout = function(){
        currentDate=new Date();
        var tempDate=new Date();
        tempDate.setSeconds(0);
        var nextTimeoutDate=self.addMinutes(tempDate,1);
        setTimeout(self.timeout, self.toNextMinuteInterval());
        if(outFile!=null) outFile.end();
        outFile = fs.createWriteStream(self.getFile(currentDate));
	deque=new Deque();
        deque.addRangeChangeListener(onQchanged,false);
	console.log(" cur "+ currentDate);
        console.log(" pac/sec = "+packetCount+" record/sec = "+recordCount+" avgRecPerPacket = "+((packetCount==0)? 0 : (recordCount/packetCount)));
        recordCount=0;
        packetCount=0;
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

    this.formRecordV9 = function(f){
        return {
    	    ver: 9,
    	    evt:f.natEvent,
            vrf:f.ingressVRFID,
            time:f.observationTimeMilliseconds,
            sip:ip.toLong(f.ipv4_src_addr),
            xsip:ip.toLong(f.postNATSourceIPv4Address),
            xsport:f.postNAPTSourceTransportPort,
            xdip:ip.toLong(f.postNATDestinationIPv4Address),
            xdport:f.postNAPTDestinationTransportPort};
    };
    
    this.formRecordV5 = function(f,toArray){
        var obj= {
    	    ver: 5,
    	    packets:f.in_pkts,
    	    bytes: f.in_bytes,
            starttime:f.first_switched,
            stoptime:f.last_switched,
            sip:ip.toLong(f.ipv4_src_addr),
            sport:f.ipv4_src_port,
            dip:ip.toLong(f.ipv4_dst_addr),
            dport:f.ipv4_dst_port
            };
        var arr= [
    	    5,
    	    f.in_pkts,
    	    f.in_bytes,
            f.first_switched,
            f.last_switched,
            ip.toLong(f.ipv4_src_addr),
            f.ipv4_src_port,
            ip.toLong(f.ipv4_dst_addr),
            f.ipv4_dst_port
            ];
            return (toArray)?arr:obj;
    };

    //Главная функция
    this.start = function() {
    //
        self.timeout();
	//Запуск коллектора
        Collector({port: port2listen, ipv4num: true}).on('data', 
        function (flow) {
        var version = (flow && flow.header)?flow.header.version:0;
		packetCount++;
	    if(version && version!=0){
        	flow.flows.forEach(function (f) {
		    recordCount++;
		    //Очередь не должна быть больше максимально возможной длинны.
            	    if (deque.length < queueMaxLength) {
			//Для каждую полученную запись преобразовать и положить в очередь.
			recordCount++;
//                	deque.push((version==5)?self.formRecordV5(f):self.formRecordV9(f));
                	deque.push(Object.values((version==5)?self.formRecordV5(f,'array'):self.formRecordV9(f)));
            	    } else {
                	console.log("################ BUFFER OVERFLOWED!! ####################################");
            	    }
        	})
            }
        });
        
    };
    
    var onQchanged = function (plus, minus, index) {
	if (plus != undefined && plus.length > 0) {
    	    while (deque.length) {
		outFile.write(JSON.stringify(deque.pop()) + "\n");
	}}};
    
    
    
    this.start();
}

