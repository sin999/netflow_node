var readline = require('readline');
var ip=require("ip");
var SortedSet = require("collections/sorted-set");
var rl = readline.createInterface({
input: require('fs').createReadStream('./nfcapd.201603211050.txt')
//    input: process.stdin,
//    output: process.stdout,
//    terminal: false
});

/*
Date first seen          Event Proto      Src IP Addr:Port          Dst IP Addr:Port     X-Src IP Addr:Port        X-Dst IP Addr:Port
2016-03-21 10:50:30.521 DELETE TCP       10.1.188.221:32779 ->   148.251.238.36:80       188.168.5.178:9452  ->   148.251.238.36:80
2016-03-21 10:50:30.521 DELETE UDP         10.1.100.5:48796 ->    77.243.99.151:53414     188.168.4.71:42890 ->    77.243.99.151:53414
2016-03-21 10:50:30.521 CREATE UDP    192.168.242.127:49001 ->   194.126.118.47:38663    188.168.5.224:38849 ->   194.126.118.47:38663

					10.1.188.221	      188.168.5.178         148.251.238.36  
2|1458543030|521|1458543030|521|6|0|0|0|167886045|32779|0|0|0|3165128114|9452|0|0|0|2499538468|80|0|0|0|2499538468|80|0|0|0|0|0|0|0|0
2|1458543030|521|1458543030|521|17|0|0|0|167863301|48796|0|0|0|3165127751|42890|0|0|0|1307796375|53414|0|0|0|1307796375|53414|0|0|0|0|0|0|0|0

*/
var mainArray=[];
var sArray=new SuperArray();
console.log(new Date())
rl.on('close', function(){
//    console.log("destinations!"+Object.keys(sArray.array).length);
//    console.log("complited!"+ObjectKeys(mainArray).length);



console.log(new Date());
})

rl.on('line', function(line){
    mainArray.push(line.split("|"));
//    sArray.putLine(line);
})


function SuperArray(){
    var self=this;
    this.array=[];
    this.firstDate=null;
    this.getArrayByIpPort = function(ipPort){
	    if( self.array[ipPort] === undefined ){  self.array[ipPort]=[];}
	    return 	self.array[ipPort];
    }
    
    this.putLine = function(line){
	var rec=new Record(line);
	if(this.firstDate==null){ firstDate=rec.date();}
	var eventDeleteBit=rec.event()==="CREATE"?0:(1<<32);
	var map = self.getArrayByIpPort((rec.xSrcIp()<<16) + rec.xSrcPort());
	map[rec.date()]=rec.srcIp()+eventDeleteBit;
    }
}


function Record(line){
	
	this.fArray=line.split("|").map( function(item){ return parseInt(item);} );
	
	this.event = function(){
	    return this.fArray[0]==="2"?"DELETE":"CREATE";
	}
	
	this.date = function(){
	    var timestamp=(this.fArray[1]*1000)+(this.fArray[2]*1);
	    return new Date(timestamp);
	}
	
	this.srcIp = function(){ 
		    return this.fArray[9];
		},
	this.srcPort = function(){ 
		    return this.fArray[10];
		}
	this.xSrcIp = function(){ 
		    return this.fArray[14];
		},
	this.xSrcPort = function(){ 
		    return this.fArray[15];
		}
	this.dstIp = function(){ 
		    return this.fArray[19];
		},
	this.dstPort = function(){ 
		    return this.fArray[20];
		}
	this.xDstIp = function(){ 
		    return this.fArray[24];
		},
	this.xDstPort = function(){ 
		    return this.fArray[25];
		}
}
