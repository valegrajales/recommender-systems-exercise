var ElasticSearchClient = require('elasticsearchclient');
var async = require('async');
var request = require('request');

var serverOptions = {
	host: 'localhost',
	port: 9200
};

var elasticSearchClient = new ElasticSearchClient(serverOptions);

exports.getRecommendationCFUser = function(req, res) {
	request({
		url: 'http://localhost:9200/movielens/recommendation/_taste/user/'+req.params._userid,
		method: 'GET',
		json: true
	}, function(error, response, body) {
		if (!error && response.statusCode == 200) {
			res.jsonp(body);
		} else {
			res.sendStatus(400);
		}
	});
};

exports.getMovieData = function(req, res) {
	qryObj = {
		"query": {
			"match": {
				"_id": req.params._itemid
			}
		}
	};

	mySearchCall = elasticSearchClient.search('movielens', 'movies', qryObj);
	mySearchCall.exec(function(err, data){
		res.jsonp(JSON.parse(data));
	});
};

exports.getTagByMovie = function(req, res) {
	qryObj = {
		"query": {
			"range": {
				"user_id": {
					"gte": parseInt(req.params._userid),
					"lte": parseInt(req.params._userid)
				}
			}
		}
	};

	mySearchCall = elasticSearchClient.search('movielens', 'tags', qryObj);
	mySearchCall.exec(function(err, data){
		res.jsonp(JSON.parse(data));
	});
};

exports.getRatingsByUser = function(req, res) {
	qryObj = {
		"query": {
			"range": {
				"user_id": {
					"gte": parseInt(req.params._userid),
					"lte": parseInt(req.params._userid)
				}
			}
		}
	};

	mySearchCall = elasticSearchClient.search('movielens', 'ratings', qryObj);
	mySearchCall.exec(function(err, data){
		res.jsonp(JSON.parse(data));
	});
};

exports.getRecommendationContentHibridUser = function(req, res) {
	async.waterfall([
		function(callback){
			var qryObj2 = {
				"query": {
					"bool": {
						"must": [
							{
								"range": {
									"user_id": {
										"gte": parseInt(req.params._userid),
										"lte": parseInt(req.params._userid)
									}
								}
							}
						]
					}
				}
			};

			var mySearchCall1 = elasticSearchClient.search('movielens', 'ratings', qryObj2);
			mySearchCall1.exec(function(err, data1) {
				callback(null, data1);
			});
		},
		function(arg1, callback) {
			var previousResult = JSON.parse(arg1);
			var response = {
				"movies": []
			};
			for(var i = 0; i < previousResult.hits.hits.length; i++) {
				response.movies[i] = previousResult.hits.hits[i]._source.item_id;
			}
			callback(null, response);
		},
		function(arg1, callback) {
			var previousResult = arg1;
			var response = {
				"movies": arg1.movies,
				"genres": [],
				"directors": [],
				"producers": [],
				"writers": [],
				"narrators": [],
				"starrings": [],
				"cinematographies": [],
				"music_composers": [],
				"release_year": []
			}
			var totalMovies = 0;
			for(var i = 0; i<previousResult.movies.length; i++) {
				qryObj = {
					"query": {
						"match": {
							"_id": previousResult.movies[i]
						}
					}
				};
				mySearchCall = elasticSearchClient.search('movielens', 'movies', qryObj);
				mySearchCall.exec(function(err, data){
					var res2 = JSON.parse(data);
					((res2.hits.hits[0]._source.genres.length != 0) ? response.genres.push(res2.hits.hits[0]._source.genres) : null);
					((res2.hits.hits[0]._source.directors.length != 0) ? response.directors.push(res2.hits.hits[0]._source.directors) : null);
					((res2.hits.hits[0]._source.producers.length != 0) ? response.producers.push(res2.hits.hits[0]._source.producers) : null);
					((res2.hits.hits[0]._source.writers.length != 0) ? response.writers.push(res2.hits.hits[0]._source.writers) : null);
					((res2.hits.hits[0]._source.narrators.length != 0) ? response.narrators.push(res2.hits.hits[0]._source.narrators) : null);
					((res2.hits.hits[0]._source.starrings.length != 0) ? response.starrings.push(res2.hits.hits[0]._source.starrings) : null);
					((res2.hits.hits[0]._source.cinematographies.length != 0) ? response.cinematographies.push(res2.hits.hits[0]._source.cinematographies) : null);
					((res2.hits.hits[0]._source.music_composers.length != 0) ? response.music_composers.push(res2.hits.hits[0]._source.music_composers) : null);
					((res2.hits.hits[0]._source.release_year != null) ? response.release_year.push(res2.hits.hits[0]._source.release_year) : null);
					if(totalMovies == previousResult.movies.length-1) {
						callback(null, response);
					} else {
						totalMovies++;
					}
				});
			}
		},
		function(arg1, callback) {
			// Construct query
			var query = "";
			var existQuery = false;
			for(var i = 0; i<arg1.genres.length; i++) {
				for(var j = 0; j<arg1.genres[i].length; j++) {
					if(i!=0 || j!=0) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.genres[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.directors.length; i++) {
				for(var j = 0; j<arg1.directors[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.directors[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.producers.length; i++) {
				for(var j = 0; j<arg1.producers[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.producers[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.writers.length; i++) {
				for(var j = 0; j<arg1.writers[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.writers[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.narrators.length; i++) {
				for(var j = 0; j<arg1.narrators[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.narrators[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.starrings.length; i++) {
				for(var j = 0; j<arg1.starrings[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.starrings[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.cinematographies.length; i++) {
				for(var j = 0; j<arg1.cinematographies[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.cinematographies[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.music_composers.length; i++) {
				for(var j = 0; j<arg1.music_composers[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.music_composers[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			qryObj = {
				"size": 15,
				"query": {
					"bool": {
						"must": [
							{
								"query_string": {
									"query": query
								}
							}
						],
						"must_not": [
							{
								"terms": {
									"item_id": arg1.movies
								}
							}
						]
					}
				},
				"highlight": {
					"fields": {
						"genres": {},
						"directors": {},
						"producers": {},
						"writers": {},
						"narrators": {},
						"starrings": {},
						"cinematographies": {},
						"music_composers": {}
					}
				}
			};
			mySearchCall = elasticSearchClient.search('movielens', 'movies', qryObj);
			mySearchCall.exec(function(err, data){
				callback(null, JSON.parse(data));
			});
		}
	],
		// optional callback
	function(err, results){
		var maxValue = results.hits.hits[0]._score;
		for(var i = 0; i<results.hits.hits.length; i++) {
			results.hits.hits[i]._source.value = parseFloat(((results.hits.hits[i]._score * 5)/maxValue).toFixed(2));
		}
		res.jsonp(results);
	});
};

exports.getRecommendationCFContentHibridUser = function(req, res) {
	async.waterfall([
		function(callback){
			var qryObj2 = {
				"query": {
					"bool": {
						"must": [
							{
								"range": {
									"user_id": {
										"gte": parseInt(req.params._userid),
										"lte": parseInt(req.params._userid)
									}
								}
							}
						]
					}
				}
			};

			var mySearchCall1 = elasticSearchClient.search('movielens', 'ratings', qryObj2);
			mySearchCall1.exec(function(err, data1) {
				callback(null, data1);
			});
		},
		function(arg1, callback) {
			var previousResult = JSON.parse(arg1);
			var response = {
				"movies": []
			};
			for(var i = 0; i < previousResult.hits.hits.length; i++) {
				response.movies[i] = previousResult.hits.hits[i]._source.item_id;
			}
			callback(null, response);
		},
		function(arg1, callback) {
			var previousResult = arg1;
			var response = {
				"movies": arg1.movies,
				"genres": [],
				"directors": [],
				"producers": [],
				"writers": [],
				"narrators": [],
				"starrings": [],
				"cinematographies": [],
				"music_composers": [],
				"release_year": []
			}
			var totalMovies = 0;
			for(var i = 0; i<previousResult.movies.length; i++) {
				qryObj = {
					"query": {
						"match": {
							"_id": previousResult.movies[i]
						}
					}
				};
				mySearchCall = elasticSearchClient.search('movielens', 'movies', qryObj);
				mySearchCall.exec(function(err, data){
					var res2 = JSON.parse(data);
					((res2.hits.hits[0]._source.genres.length != 0) ? response.genres.push(res2.hits.hits[0]._source.genres) : null);
					((res2.hits.hits[0]._source.directors.length != 0) ? response.directors.push(res2.hits.hits[0]._source.directors) : null);
					((res2.hits.hits[0]._source.producers.length != 0) ? response.producers.push(res2.hits.hits[0]._source.producers) : null);
					((res2.hits.hits[0]._source.writers.length != 0) ? response.writers.push(res2.hits.hits[0]._source.writers) : null);
					((res2.hits.hits[0]._source.narrators.length != 0) ? response.narrators.push(res2.hits.hits[0]._source.narrators) : null);
					((res2.hits.hits[0]._source.starrings.length != 0) ? response.starrings.push(res2.hits.hits[0]._source.starrings) : null);
					((res2.hits.hits[0]._source.cinematographies.length != 0) ? response.cinematographies.push(res2.hits.hits[0]._source.cinematographies) : null);
					((res2.hits.hits[0]._source.music_composers.length != 0) ? response.music_composers.push(res2.hits.hits[0]._source.music_composers) : null);
					((res2.hits.hits[0]._source.release_year != null) ? response.release_year.push(res2.hits.hits[0]._source.release_year) : null);
					if(totalMovies == previousResult.movies.length-1) {
						callback(null, response);
					} else {
						totalMovies++;
					}
				});
			}
		},
		function(arg1, callback) {
			// Construct query
			var query = "";
			var existQuery = false;
			for(var i = 0; i<arg1.genres.length; i++) {
				for(var j = 0; j<arg1.genres[i].length; j++) {
					if(i!=0 || j!=0) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.genres[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.directors.length; i++) {
				for(var j = 0; j<arg1.directors[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.directors[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.producers.length; i++) {
				for(var j = 0; j<arg1.producers[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.producers[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.writers.length; i++) {
				for(var j = 0; j<arg1.writers[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.writers[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.narrators.length; i++) {
				for(var j = 0; j<arg1.narrators[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.narrators[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.starrings.length; i++) {
				for(var j = 0; j<arg1.starrings[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.starrings[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.cinematographies.length; i++) {
				for(var j = 0; j<arg1.cinematographies[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.cinematographies[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			for(var i = 0; i<arg1.music_composers.length; i++) {
				for(var j = 0; j<arg1.music_composers[i].length; j++) {
					if(existQuery) {
						query = query+" OR ";
					}
					query = query+"\""+arg1.music_composers[i][j].replace(/"/g, "")+"\"";
					existQuery = true;
				}
			}
			qryObj = {
				"size": 15,
				"query": {
					"bool": {
						"must": [
							{
								"query_string": {
									"query": query
								}
							}
						],
						"must_not": [
							{
								"terms": {
									"item_id": arg1.movies
								}
							}
						]
					}
				},
				"highlight": {
					"fields": {
						"genres": {},
						"directors": {},
						"producers": {},
						"writers": {},
						"narrators": {},
						"starrings": {},
						"cinematographies": {},
						"music_composers": {}
					}
				}
			};
			mySearchCall = elasticSearchClient.search('movielens', 'movies', qryObj);
			mySearchCall.exec(function(err, data){
				callback(null, JSON.parse(data));
			});
		},
		function(arg1, callback) {
			request({
				url: 'http://localhost:9200/movielens/recommendation/_taste/user/'+req.params._userid,
				method: 'GET',
				json: true
			}, function(error, response, body) {
				if (!error && response.statusCode == 200) {
					var maxValue = arg1.hits.hits[0]._score;
					for(var i = 0; i<arg1.hits.hits.length; i++) {
						arg1.hits.hits[i]._source.value = parseFloat(((arg1.hits.hits[i]._score * 5)/maxValue).toFixed(2));
					}
					var result={
						"hits": {
							"hits": []
						}
					}
					var currentPosA = 0;
					var currentPosB = 0;
					for(var i = 0; i<10; i++) {
						var actual = Math.floor(Math.random() * 2) + 1;
						if(body.hits.hits[0]._source.items[currentPosA].value == arg1.hits.hits[currentPosB]._source.value) {
							if(actual == 1) {
								result.hits.hits.push({
									"_source": {
										"item_id": body.hits.hits[0]._source.items[currentPosA].item.system_id,
										"value": body.hits.hits[0]._source.items[currentPosA].value
									}
								});
								currentPosA++;
							} else if(actual == 2) {
								result.hits.hits.push(arg1.hits.hits[currentPosB]);
								currentPosB++;
							}
						} else if(body.hits.hits[0]._source.items[currentPosA].value > arg1.hits.hits[currentPosB]._source.value) {
							result.hits.hits.push({
								"_source": {
									"item_id": body.hits.hits[0]._source.items[currentPosA].item.system_id,
									"value": body.hits.hits[0]._source.items[currentPosA].value
								}
							});
							currentPosA++;
						} else {
							result.hits.hits.push(arg1.hits.hits[currentPosB]);
							currentPosB++;
						}
					}
					for(var i = 10; i<15; i++) {
						result.hits.hits.push(arg1.hits.hits[currentPosB]);
						currentPosB++;
					}
					callback(null, result);
				}
			});
		}
	],
		// optional callback
	function(err, results){
		res.jsonp(results);
	});
};
