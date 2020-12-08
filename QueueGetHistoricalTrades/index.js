const polygon = require('polygon.io')
const https = require('https');
const moment = require('moment');
const zlib = require('zlib');
const util = require('util');
const compress = util.promisify(zlib.gzip);
const { BlobServiceClient, StorageSharedKeyCredential } = require("@azure/storage-blob");

const DATE_FORMAT= 'YYYY-MM-DD';
const account = process.env.ACCOUNT_NAME 
const accountKey = process.env.ACCOUNT_KEY 
const polygon_apiKey = process.env.POLYGON_API_KEY 
const bRunInCloud = (process.env.LOCAL_RUN  === undefined || process.env.LOCAL_RUN === null) ? true : (process.env.LOCAL_RUN === 'true') ? true : false;

var write_data = [];

function getPromise(context, s, d, c) {
    let query_url = `https://api.polygon.io/v2/ticks/stocks/trades/${s}/${d}?apiKey=${polygon_apiKey}`;
    if(c != null){
        query_url = query_url + `&timestamp=${c}`
    }
    context.log(query_url);
      return new Promise((resolve, reject) => {
      https.get(query_url, (resp) => {
        // context.log('statusCode:', resp.statusCode);
        // context.log('headers:', resp.headers);
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
  
  async function streamToString(readableStream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      readableStream.on("data", (data) => {
        chunks.push(data.toString());
      });
      readableStream.on("end", () => {
        resolve(chunks.join(""));
      });
      readableStream.on("error", reject);
    });
  }

  async function storeAllTickerData(context, blobClient, symbol, day, oldBlobClient, cnt){
    const blobExists = await blobClient.exists();
    const oldBlobExists = (oldBlobClient === null) ? false : await oldBlobClient.exists();
    let retryCount = 0;

    if(!blobExists){
        //context.log('Downloading data for blob');
        if(oldBlobExists){
            const downloadBlobResponse = await oldBlobClient.download();
            const downloaded =  await streamToString(downloadBlobResponse.readableStreamBody);
            const olddata = JSON.parse(downloaded);
            const oe = olddata[olddata.length-1];
            write_data = [...write_data, ...olddata];
            await oldBlobClient.delete();
            await storeAllTickerData(context, blobClient, symbol, day, null, oe.t);              
            return true;
        }

        try{          
            let http_promise = getPromise(context, symbol, day, cnt);
            let response_body = await http_promise;
            let obj_result = JSON.parse(response_body);
            if(obj_result.success){
                write_data = [...write_data, ...obj_result.results];
                if(obj_result.results.length % 50000 === 0){
                    // get last element 
                    const le = obj_result.results[obj_result.results.length-1];
                    if(le.t !== undefined){
                      await storeAllTickerData(context, blobClient, symbol, day, oldBlobClient, le.t);  
                    }else{                      
                      context.log(le);
                      return false
                    }
                    return true;                  
                }else{             
                    const content = JSON.stringify(write_data);
                    const zipped = await compress(content);
                    const uploadBlobResponse = await blobClient.upload(zipped, Buffer.byteLength(zipped));
                    context.log(`Upload ${symbol} block blob successfully`, uploadBlobResponse.requestId);
                    return true;
                }
            }else{
                if(obj_result.errorcode != '001'){  
                  context.log(obj_result);
                }
                return false;
            }

        }catch(err){
            context.log("===================== START HARD ERROR =====================");
            context.log(err)
            _status = 500;
            context.log("===================== END HARD ERROR =====================");
            return false;
        }
    }else{
       context.log(`blob ${symbol} already exists`);
    }
  }

module.exports = async function (context, myQueueItem) {

  context.log('JavaScript queue trigger function processed work item', myQueueItem);
  const start =  moment(new Date(), 'YYYY-MM-DD HH:mm:ss'); 

  // local vars
  let i = 0;
  let passParams = true;
  let start_date = undefined;
  let end_date = undefined;
  let curr_date = undefined;
  let _responseMessage = "";
  let _status = 200;
  // context.log('Azure account ' + account);

  const tickerSymbol = myQueueItem.ticker;
  const startDate = myQueueItem.start;
  const endDate = myQueueItem.end;

  // context.log("Validating dates");
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
      // context.log("Creating clients");
      const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);
      const rest = polygon.restClient(polygon_apiKey);
      let retryCount = 0;

      // List containers
      const blobServiceClient = new BlobServiceClient(
          // When using AnonymousCredential, following url should include a valid SAS or support public access
          `https://${account}.blob.core.windows.net`,
          sharedKeyCredential
      );

      // start processing
      context.log('Days to Process: ' + days);
      curr_date = start_date; // set start date
      
      // context.log("Processing Begins");
      do {
        i++; // track day count
        // extract information from current date to process
        let qDate = curr_date.format(DATE_FORMAT);
        let qYear = curr_date.format('YYYY');
        let qMonth = curr_date.format('MM');
        let qDay = curr_date.format('DD');
      
        const label = qDate.replace(/\-/g, '');
        let containerName = `master-data-store`;    
        let blobName = `stocks/trades/${tickerSymbol}/${qYear}/${qMonth}/${qDay}/${tickerSymbol}_${label}.json.gz`;
        let oldBlobName = `stocks/trades/${tickerSymbol}/${qYear}/${qMonth}/${qDay}/${tickerSymbol}_${label}.json`;
        
        // context.log(containerName);
        const containerClient = blobServiceClient.getContainerClient(containerName);
        const containerExists = await containerClient.exists();
    
        if(!containerExists){
          const createContainerResponse = await containerClient.create();
          // context.log(`Create container ${containerName} successfully`, createContainerResponse.requestId);
        }
    
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
        const oldBlockBlobClient = containerClient.getBlockBlobClient(oldBlobName);

        const bReturn = await storeAllTickerData(context, blockBlobClient, tickerSymbol, qDate, oldBlockBlobClient, null);
        if(!bReturn){
            // TODO: Log failure
            context.log('Failed to retrieve day');
        }

        curr_date = curr_date.add(1,'days');
      }while(curr_date<=end_date);
      
      // context.log("Days Processed: " + i);
      _responseMessage = "Processed " + i + " days of data for " + tickerSymbol;
          
  }else{
      _status = 501;
      _responseMessage = "Failed to process symbol and dates asked.";

  }
  
  context.log("Status: " + _status + " Message: " + _responseMessage);
  let end = moment(new Date(), 'YYYY-MM-DD HH:mm:ss'); 
  let duration = moment.utc(end.diff(start)).format("HH:mm:ss.SSS");
  context.log("execution time: " + duration);
  if(_status == 200){
    // only complete if 100% no failures
    if(bRunInCloud){
      //  context.done();
    }else{
        context.log('Finished');
    }
  }
};