var ElasticSearchClient = require('elasticsearchclient');

var serverOptions = {
	host: 'localhost',
	port: 9200
};

var elasticSearchClient = new ElasticSearchClient(serverOptions);

var readline = require('linebyline'),
    rl = readline('../yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json');

var commands = [];
var counter = 0;
rl.on('line', function(line) {
	// do something with the line of text
	jsonline = JSON.parse(line);
	//console.log(jsonline);
	//elasticSearchClient.index("yelp", "user", jsonline).exec()
	commands.push({ "index" : { "_index" :'yelp', "_type" : "business", "_id": jsonline.business_id} })
	commands.push(jsonline);
	counter += 1;
	if (counter===200) {
		elasticSearchClient.bulk(commands, {}).exec();
		console.log("Insertados 200");
		commands = [];
		counter = 0;
	}
})
.on('end', function(e) {
	if (counter > 0) {
		elasticSearchClient.bulk(commands, {}).exec();
		commands = [];
		counter = 0;
	}
	console.log("Insercion finalizada");
})
.on('error', function(e) {
	// something went wrong
	console.log("error");
});
