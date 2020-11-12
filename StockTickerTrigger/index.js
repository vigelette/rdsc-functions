const polygon = require('polygon.io');
const rest = polygon.restClient("AKWS84KBJKLLG5N1H825");

module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    rest.stocks.lastQuoteForSymbol("MSFT").then((value) => {
        console.log(value);
        // expected output: "Success!"
        if(value.status === 'success'){
            let unix_timestamp = value.last.timestamp;
            let date = new Date(unix_timestamp);
            console.log(date.toLocaleString());
        }
    });

    if (myTimer.IsPastDue)
    {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);   
};