LSR:
https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Long-Short-Ratio	/futures/data/globalLongShortAccountRatio
	5 min example https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=1m&limit=500
	"symbol":"BTCUSDT", "longShortRatio":"1.9559", "timestamp":"1583139900000
Long/Short Ratio
API Description
Query symbol Long/Short Ratio

HTTP Request
GET /futures/data/globalLongShortAccountRatio

Request Weight
0

Request Parameters
Name	Type	Mandatory	Description
symbol	STRING	YES	
period	ENUM	YES	"5m","15m","30m","1h","2h","4h","6h","12h","1d"
limit	LONG	NO	default 30, max 500
startTime	LONG	NO	
endTime	LONG	NO	
If startTime and endTime are not sent, the most recent data is returned.
Only the data of the latest 30 days is available.
IP rate limit 1000 requests/5min
Response Example
[
    { 
         "symbol":"BTCUSDT",  // long/short account num ratio of all traders
	      "longShortRatio":"0.1960",  //long account num ratio of all traders
	      "longAccount": "0.6622",   // short account num ratio of all traders
	      "shortAccount":"0.3378", 
	      "timestamp":"1583139600000"
    
     },
     
     {
         
         "symbol":"BTCUSDT",
	      "longShortRatio":"1.9559",
	      "longAccount": "0.6617", 
	      "shortAccount":"0.3382", 	                
	      "timestamp":"1583139900000"
	               
        },   
	    
]

----------------------------------------------------------------------
OI:
https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics	GET /futures/data/openInterestHist
	5min 200 req/min, 500 records https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&interval=1m&limit=500
	"sumOpenInterest":"20401.36" is tokens "sumOpenInterestValue":"149940752.14" $USD
Open Interest Statistics
API Description
Open Interest Statistics

HTTP Request
GET /futures/data/openInterestHist

Request Weight
0

Request Parameters
Name	Type	Mandatory	Description
symbol	STRING	YES	
period	ENUM	YES	"5m","15m","30m","1h","2h","4h","6h","12h","1d"
limit	LONG	NO	default 30, max 500
startTime	LONG	NO	
endTime	LONG	NO	
If startTime and endTime are not sent, the most recent data is returned.
Only the data of the latest 1 month is available.
IP rate limit 1000 requests/5min
Response Example
[
    { 
         "symbol":"BTCUSDT",
	      "sumOpenInterest":"20403.63700000",  // total open interest 
	      "sumOpenInterestValue": "150570784.07809979",   // total open interest value
          "CMCCirculatingSupply": "165880.538", // circulating supply provided by CMC
	      "timestamp":"1583127900000"
    },     
    { 
         "symbol":"BTCUSDT",
         "sumOpenInterest":"20401.36700000",
         "sumOpenInterestValue":"149940752.14464448",
         "CMCCirculatingSupply": "165900.14853",
         "timestamp":"1583128200000"    
    },   
]
