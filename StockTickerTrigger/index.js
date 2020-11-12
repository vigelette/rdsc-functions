const polygon = require('polygon.io');

module.exports = async function (context, myTimer) {
    let timeStamp = new Date().toISOString();
    
    const rest = polygon.restClient("AKWS84KBJKLLG5N1H825");     
    let tickerList = ['AAPL','MSFT'];
    for (const idx in tickerList) {
        context.log(tickerList[idx].symbol);
        await rest.stocks.lastQuoteForSymbol(tickerList[idx].symbol).then((value) => {
            // expected output: "Success!"
            if(value.status === 'success'){
                let result = insertData(value)
                if(result){
                    context.log("success");
                }
            }});
    }
    

    if (myTimer.IsPastDue)
    {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);   
};