const polygon = require('polygon.io')
const https = require('https');
const moment = require('moment');
const { BlobServiceClient, StorageSharedKeyCredential } = require("@azure/storage-blob");

const DATE_FORMAT= 'YYYY-MM-DD';
const account = process.env.ACCOUNT_NAME 
const accountKey = process.env.ACCOUNT_KEY 
const polygon_apiKey = process.env.POLYGON_API_KEY 

function getPromise(s, d) {
	return new Promise((resolve, reject) => {
    context.log(`https://api.polygon.io/v2/ticks/stocks/trades/${s}/${d}?apiKey=${polygon_apiKey}`);
    https.get(`https://api.polygon.io/v2/ticks/stocks/trades/${s}/${d}?apiKey=${polygon_apiKey}`, (resp) => {
      context.log('statusCode:', resp.statusCode);
      context.log('headers:', resp.headers);
      let stock_data = [];
  
      // A chunk of data has been recieved.
      resp.on('data', (chunk) => {
        stock_data.push(chunk);        
      });
  
      // The whole response has been received. Print out the result.
      resp.on('end', () => {
        let response_body = Buffer.concat(stock_data);
        resolve(response_body);
      });
  
      resp.on("error", (err) => {
        reject(err);
      });

    });


  });
}


module.exports = async function (context, req) {
    context.log('Retrieving historical trade data');
    // local vars
    let i = 0;
    let passParams = true;
    let start_date = undefined;
    let end_date = undefined;
    let curr_date = undefined;
    let _responseMessage = "";
    let _status = 200;
    context.log('Azure account ' + account);

    const tickerSymbol = (req.query.ticker || (req.body && req.body.ticker));
    const startDate = (req.query.start || (req.body && req.body.start));
    const endDate = (req.query.end || (req.body && req.body.end));

    // validate parameters first
    
    context.log("Validating dates");
    start_date = moment(startDate, DATE_FORMAT, true);
    if(!start_date.isValid()){
        context.log("Invalid start day");
        passParams = false;
    }

    end_date = moment(endDate, DATE_FORMAT, true);
    if(!end_date.isValid()){
        context.log("Invalid end day");
        passParams = false;
    }

    let days = end_date.diff(start_date, 'days');
    if(days < 0){
        context.log("start day must be less than or equal to the end day");
        passParams = false;
    }

    if(passParams){
        // create necessary clients
        context.log("Creating clients");
        const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);
        const rest = polygon.restClient(polygon_apiKey);

        // List containers
        const blobServiceClient = new BlobServiceClient(
            // When using AnonymousCredential, following url should include a valid SAS or support public access
            `https://${account}.blob.core.windows.net`,
            sharedKeyCredential
        );

        // start processing
        context.log('Days to Process: ' + days + 1);
        curr_date = start_date; // set start date

        
        context.log("Processing Begins");
        do {
          i++; // track day count
          // extract information from current date to process
          let qDate = curr_date.format(DATE_FORMAT);
          let qYear = curr_date.format('YYYY');
          let qMonth = curr_date.format('MM');
          let qDay = curr_date.format('DD');
        
          const label = qDate.replace(/\-/g, '');
          let containerName = `master-data-store`;    
          let blobName = `stocks/trades/${tickerSymbol}/${qYear}/${qMonth}/${qDay}/${tickerSymbol}_${label}.json`;
          
          context.log(containerName);
          const containerClient = blobServiceClient.getContainerClient(containerName);
          const containerExists = await containerClient.exists();
      
          if(!containerExists){
            const createContainerResponse = await containerClient.create();
            context.log(`Create container ${containerName} successfully`, createContainerResponse.requestId);
          }
      
          const blockBlobClient = containerClient.getBlockBlobClient(blobName);
          const blobExists = await blockBlobClient.exists();
      
          if(!blobExists){
            context.log('Downloading data for blob');
            let http_promise = getPromise(tickerSymbol, qDate);
            let response_body = await http_promise;
            let obj_result = JSON.parse(response_body);
      
            if(obj_result.success){
              const content = JSON.stringify(obj_result.results);
              const uploadBlobResponse = await blockBlobClient.upload(content, Buffer.byteLength(content));
              context.log(`Upload block blob ${blobName} successfully`, uploadBlobResponse.requestId);
            }
          }else{
            context.log('blob already exists');
          }
          curr_date = curr_date.add(1,'days');
        }while(curr_date<=end_date);
        
        context.log("Days Processed: " + i);
        _responseMessage = "Processed " + i + " days of data for " + tickerSymbol;
            
    }else{
        _status = 501;
        _responseMessage = "Failed to process symbol and dates asked.";

    }


    context.res = {
        status: _status, 
        body: responseMessage
    };
}