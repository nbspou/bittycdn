'use strict';

const fs = require('fs');
const homedir = require('os').homedir();
if (!process.env.BITTYCDN_CONFIG) {
	process.env.BITTYCDN_CONFIG = homedir + "/.bittycdn/config.json";
}
if (!fs.existsSync(process.env.BITTYCDN_CONFIG)) {
	console.error("Config file does not exist: " + process.env.BITTYCDN_CONFIG + ", using default");
	process.env.BITTYCDN_CONFIG = "./config";
}
const config = require(process.env.BITTYCDN_CONFIG);

const http = require('http');
const https = require('https');
const stream = require('stream');
const url = require('url');

const redis = require("redis");

let redisClient = redis.createClient({ url: config.Track.Redis });
let redisReady = false;
redisClient.on('ready', function() { redisReady = true; console.log("Redis ready", config.Track.Redis); });
redisClient.on('error', function(err) { console.error("Redis error", config.Track.Redis, err); });
redisClient.on('end', function() { redisReady = false; console.log("Redis offline"); });
// TODO: Manually buffer and collapse HINCRBY

function trackDownloadInvalid(container, object) {
	if (!redisReady) { return setTimeout(function() { trackDownloadInvalid(container, object); }, 1000); }
	redisClient.hincrby(container, 'invalid:count', 1);
}

function trackDownloadStart(container, object, size) {
	if (!redisReady) { return setTimeout(function() { trackDownloadStart(container, object, size); }, 1000); }
	redisClient.hincrby(container, 'start:count', 1);
	redisClient.hincrby(container, 'start:size', size);
}

function trackDownloadEnd(container, object, size, sent) {
	if (!redisReady) { return setTimeout(function() { trackDownloadEnd(container, object, size); }, 1000); }
	redisClient.hincrby(container, 'end:count', 1);
	redisClient.hincrby(container, 'end:size:file', size);
	redisClient.hincrby(container, 'end:size:sent', sent);
}

function trackOriginInvalid(container, object, origin) {
	if (!redisReady) { return setTimeout(function() { trackOriginInvalid(container, object, origin); }, 1000); }
	redisClient.hincrby(container, 'origin:invalid:count', 1);
}

function trackOriginReceived(container, object, origin, received) {
	if (!redisReady) { return setTimeout(function() { trackOriginReceived(container, object, origin, received); }, 1000); }
	redisClient.hincrby(container, 'origin:received:count', 1);
	redisClient.hincrby(container, 'origin:received:size', received);
}

let cache = { };

http.createServer(function (req, res) {
	if (req.method != 'GET') {
		res.writeHead(403, { });
		return res.end("Method Not Allowed");
	}
	let urlParsed = url.parse(req.url);
	let pathName = urlParsed.pathname;
	if (pathName == '/favicon.ico') {
		res.writeHead(404, { });
		return res.end("Favicon Disabled");
	}
	let hostName = req.headers.host.split(':', 2)[0];
	let hostConfig = config.Hosts[hostName];
	if (!hostConfig) {
		res.writeHead(403, { });
		return res.end("Host Unknown");
	}
	console.log("Request", hostName, pathName);
	let cacheContainer;
	let cacheObject;
	if (hostConfig.TrackDepth) {
		let trackDepth = hostConfig.TrackDepth - 1;
		let depthIndex = pathName.indexOf('/', 1);
		while (trackDepth && depthIndex > 0) {
			depthIndex = pathName.indexOf('/', depthIndex);
			--trackDepth;
		}
		if (depthIndex < 0) {
			res.writeHead(403, { });
			return res.end("Not Tracked");
		}
		cacheContainer = hostName + pathName.slice(0, depthIndex);
		cacheObject = pathName.slice(depthIndex);
	} else {
		cacheContainer = hostName;
		cacheObject = pathName;
	}
	console.log("Return", cacheContainer, cacheObject);
	// trackDownloadInvalid(cacheContainer, cacheObject);
	
	let originIdx = 0;
	let originNotFound = 0;
	let originError = 0;
	function nextOrigin() {
		
		if (originIdx >= hostConfig.Origin.length) {
			if (originError) {
				res.writeHead(500, { });
				return res.end("Origin Not Responding");
			} else if (originNotFound) {
				res.writeHead(404, { });
				return res.end("Origin Not Found");
			} else {
				res.writeHead(500, { });
				return res.end("Origin Not Configured");
			}
		}
		
		let origin = hostConfig.Origin[originIdx] + pathName;
		
		(origin.startsWith('https') ? https : http).get(origin, function(response) {
			
			if (response.statusCode != 200) {
				console.log("Status code", response.statusCode);
				if (response.statusCode == 404) {
					++originNotFound;
				} else {
					trackOriginInvalid(cacheContainer, cacheObject, origin);
					++originError;
				}
				// r.abort();
				++originIdx;
				nextOrigin();
			}
			
			let type = response.headers['content-type'];
			let size = ~~response.headers['content-length'];
			let sent = 0;
			
			res.writeHead(200, { 
				'content-type': type, 
				'content-length': size
			});
			
			trackDownloadStart(cacheContainer, cacheObject, size);
			
			response.on('data', function(chunk) {
				sent += chunk.length;
				res.write(chunk);
			});
			
			response.on('end', function() {
				trackOriginReceived(cacheContainer, cacheObject, origin, sent);
				trackDownloadEnd(cacheContainer, cacheObject, size, sent);
				res.end();
			});
			
		}).end();
	}
	nextOrigin();
	
}).listen(process.env.BITTYCDN_HTTP_PORT || config.Port);
console.log("HTTP server listening on port " + process.env.BITTYCDN_HTTP_PORT || config.Port);

/* end of file */
