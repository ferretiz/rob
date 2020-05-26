const fs = require('fs');
var dateTime = require('node-datetime');
const {BigQuery} = require('@google-cloud/bigquery');
var sSvckeyFilename = 'robin.json';
var sDatasetId = 'dbrob';
var sProjectId = 'rob105';
var aInstrumentKeys = ['BTC','PLUG', 'Snap', 'DIS', 'DAL', 'AAL', 'BAC', 'F', 'GE', 'ACB', 'HEXO', 'GRPN', 'NOK', 'GPRO', 'FB', 'NKE', 'TWTR', 'BTC', 'DOGE', 'AAPL', 'TSLA', 'NFLX','MSFT', 'SBUX', 'BABA', 'FIT', 'AMZN', 'PENN', 'V', 'ZM', 'NTDOY', 'SPHD', 'CSCO', 'ROKU', 'WMT', 'JNJ', 'PG', 'GOOGL', 'GOOG', 'QCLN', 'FENY', 'SPY', 'IBB', 'DIA', 'NDAQ', 'UBER', 'AUY', 'SNE'];
var credentials = {
    token: 'q5VYcgm4hfYogsaoX4sSgHoBEJXyWs-9ReBY4qUZRCtDkDZzGRHOTrs3MyFHjspyaUbrY'
};
var ordersOptions = {
    updated_at: '2020-01-01'
}

var oRobin = require('robinhood')(credentials);
var oBigquery = new BigQuery({keyFilename: sSvckeyFilename});
var bqDataset = oBigquery.dataset(sDatasetId);


//Instruments
async function getInstrumentsfromRobin(){
    return new Promise(function (resolve, reject) {
        try{
            let aInstruments = [];
            for (let i=0; i<aInstrumentKeys.length; i++){
                console.log("i: ",i);
                let oConn = oRobin.instruments(aInstrumentKeys[i],function(err, response, body){
                    //return callback(body);
                    aInstruments.push(body);
                });
                //let oConn = getInstruments(aInstrumentKeys[i], function(oInstruments){
                //    aInstruments.push(oInstruments);
                //});  
            }
            resolve(aInstruments);

        }catch(err){
            reject(err);
        }
    });
}

async function send2BigQuery(oTable, oRow){
    //Insert to BQ
    console.log('Inserting: ',oRow, typeof(oTable));
    try{
        //let bqTable = oDataset.table('instruments');
        oTable.insert(oRow, insertHandler);
    }catch(err){
    //    console.log('Error sending data to BQ: ',err);
    }

    function insertHandler(err, apiResponse) {
        if (err) {
            console.log('Error sending data to BQ-: ',err," ",err.errors[0].errors[0].reason, " ",err.errors[0].errors[0].message);
        }
    }
}

async function main() {
    //var oInst = {};
    try{
        //Get instruments from robin
        (async () => {  //anonymous function is needed for the await to work
            let aInst = await getInstrumentsfromRobin().then((aInstruments) => {
                console.log("aInstruments: ",aInstruments,typeof(aInstruments));
                //console.log("aInst: ",aInst,typeof(aInst));
            });
        })();
        
        //add lastrun and upload to bq
        var bqTable2 = bqDataset.table('instruments');

        var oDateTime = dateTime.create(Date.now());
        var dCurrentDate = oDateTime.format('Y-m-d H:M:S');
        //for (let i=0; i<aInstruments.length; i++){
        for (let i=0; i<1; i++){
            if (typeof(aInstruments[i].results[0]) != 'undefined'){
                console.log("i: ",i," ",aInstruments[i].results[0].symbol);
                aInstruments[i].results[0].lastrun = dCurrentDate
                //send2BigQuery(bqTable2,aInstruments[i].results[0]);
                try{
                    await send2BigQuery(bqTable2,aInstruments[0].results[0]);
                }catch(err){
                    console.log('Error sending to BQ: ',err);
                }
            }
        }
    }catch(err){
        console.log("Error: ",err);
    }
}

main();


