var MongoClient = require('mongodb').MongoClient
  , assert = require('assert');
var _ = require('underscore');
var ElasticSearchClient = require('elasticsearchclient');

var serverOptions = {
	host: 'localhost',
	port: 9200
};

var elasticSearchClient = new ElasticSearchClient(serverOptions);

var commands = [];

// Connection URL
var url = 'mongodb://localhost:27017/test';
// Use connect method to connect to the Server
MongoClient.connect(url, function(err, db) {
	assert.equal(null, err);
	console.log("Connected correctly to server");

	findDocuments(db, function() {
		elasticSearchClient.bulk(commands, {}).exec();
		db.close();
	});
});

var findDocuments = function(db, callback) {
  // Get the documents collection
  var collection = db.collection('movies');
  // Find some documents
  collection.find({}).toArray(function(err, docs) {
    console.log("Found the following records");
    var counter = 0;
    _.each(docs, function(movie) {
    	commands.push({ "index" : { "_index" :'movielens', "_type" : "movies", "_id": parseInt(movie._id)} })
    	commands.push({"item_id": parseInt(movie._id), "name": movie.name, "fix_me": movie.fix_me, "genres": movie.genres, "directors": movie.directors, "producers": movie.producers, "writers": movie.writers, "narrators": movie.narrators, "starrings": movie.starrings, "cinematographies": movie.cinematographies, "music_composers": movie.music_composers, "release_year": movie.release_year, "updated_at": movie.updated_at, "created_at": movie.created_at});
    	//elasticSearchClient.index("movielens", "movies", {"item_id": movie._id}).exec();
    	//console.log(movie);
    	counter++;
    });
    console.log(counter);
    //console.log(docs[0].genres[0]);
    //console.dir(docs);
    callback(docs);
  });
}
