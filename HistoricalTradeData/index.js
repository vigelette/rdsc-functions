const polygon = require('polygon.io')
const https = require('https');
const moment = require('moment');
const { BlobServiceClient, StorageSharedKeyCredential } = require("@azure/storage-blob");

const account = process.env.ACCOUNT_NAME 
const accountKey = process.env.ACCOUNT_KEY 

module.exports = async function (context, req) {
    context.log('Retrieving historical trade data');
    // local vars
    let i = 0;
    let start_date = undefined;
    let end_date = undefined;
    let curr_date = undefined;
  
    context.log('Azure account ' + account);

    const tickerSymbol = (req.query.ticker || (req.body && req.body.ticker));
    const startDate = (req.query.start || (req.body && req.body.start));
    const endDate = (req.query.end || (req.body && req.body.end));

    const responseMessage = name
        ? "Hello, " + name + ". This HTTP triggered function executed successfully."
        : "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.";

    context.res = {
        // status: 200, /* Defaults to 200 */
        body: responseMessage
    };
}