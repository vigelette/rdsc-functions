const polygon = require('polygon.io');

module.exports = async function (context, myTimer) {
    let timeStamp = new Date().toISOString();
    
    let rest = polygon.restClient("AKWS84KBJKLLG5N1H825");
    rest.stocks.lastQuoteForSymbol("MSFT").then((value) => {
        context.log(value);
        // expected output: "Success!"
        if(value.status === 'success'){
            let unix_timestamp = value.last.timestamp;
            let date = new Date(unix_timestamp);
            context.log(date.toLocaleString());
        }
    });

    if (myTimer.IsPastDue)
    {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);   
};