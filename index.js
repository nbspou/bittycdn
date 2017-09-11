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
const stream = require('stream');
const url = require('url');

const redis = require("redis")

let redisClient = redis.createClient({ url: config.Track.Redis });

redisClient.on('ready', function() { console.log("Redis ready", config.Track.Redis); });
redisClient.on('error', function(err) { console.error("Redis error", config.Track.Redis, err); });

/*
// TODO: Manually buffer and collapse HINCRBY
let redisReady = false;
redisClient.on('ready', function() { redisReady = true; });
redisClient.on('end', function() { redisReady = false; });
*/

function trackDownloadFailure(container, object) {
	redisClient.hincrby(container, 'fail:count', 1);
}

function trackDownloadStart(container, object, size) {
	redisClient.hincrby(container, 'start:count', 1);
	redisClient.hincrby(container, 'start:size', size);
}

function trackDownloadEnd(container, object, size) {
	redisClient.hincrby(container, 'end:count', 1);
	redisClient.hincrby(container, 'end:size:file', size);
	redisClient.hincrby(container, 'end:size:sent', size);
}

let cache = { };

http.createServer(function (req, res) {
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
	res.end("wa");
}).listen(process.env.BITTYCDN_HTTP_PORT || config.Port);
console.log("HTTP server listening on port " + process.env.BITTYCDN_HTTP_PORT || config.Port);

/* end of file */
