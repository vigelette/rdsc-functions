const polygon = require('polygon.io')
const https = require('https');
const moment = require('moment');
const { BlobServiceClient, StorageSharedKeyCredential } = require("@azure/storage-blob");

const DATE_FORMAT= 'YYYY-MM-DD';
const account = process.env.ACCOUNT_NAME 
const accountKey = process.env.ACCOUNT_KEY 
const polygon_apiKey = process.env.POLYGON_API_KEY 

function getPromise(context, s, d) {
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

module.exports = async function (context, myQueueItem) {
    context.log('JavaScript queue trigger function processed work item', myQueueItem);
};