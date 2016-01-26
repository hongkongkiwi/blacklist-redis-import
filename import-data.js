var fs = require('fs'),
    util = require('util'),
    stream = require('stream'),
    es = require("event-stream"),
    redis = require("redis"),
    wc = require('wc.js'),
    ProgressBar = require('progress'),
    a = require('async'),
    config = require('./config');

var categories = ['ads','blasphemy','chanology','cp','dating','dyn','facebook',
    'file','freeweb','gambling','gaming','image','malicious','new-tlds','pharma-rx',
    'piracy','porn','prime','proxies','racism','smedia','usg','video'];
var bar;

var totalCount = 0;

client = redis.createClient(config.redis.port, config.redis.server);

client.on("error", function (err) {
    console.log("Error " + err);
});



process.on('SIGINT', function() {
    console.log("Caught interrupt signal");
    if (!bar.complete) {
      console.log('\naborted\n');
    }

    process.exit(0);
});

function getDbSize () {
    return fs.statSync(databasePath + '/data.mdb').size;
}


function importCategory(category, callback) {
    var fileName = './blacklists/squid-' + category + '.acl';

    var reader = fs.createReadStream(fileName, {"encoding": 'utf-8', "flags": 'r', "fd": null});
    wc.wcStream(reader, {lines: true, bytes: false, words: false, maxLineLength: false}, function(err, lineCount) {
        lineCount = lineCount[0];
        console.log('Importing category \'' + category + '\':');
        bar = new ProgressBar(':bar', { total: lineCount });

        var domainNumber = 0;
        var s = fs.createReadStream(fileName)
            .pipe(es.split())
            .pipe(es.mapSync(function(domain){
                // pause the readstream
                s.pause();
                domainNumber += 1;
                totalCount++;

                (function() {
                    domain = domain.trim();
                    if (domain.charAt(0) == ".") {
                        domain = domain.substr(1);
                    }
                    if (!domain ||
                            domain.length === 0 ||
                            domain.charAt(0) === '#' ||
                            (domain.length === 1 && domain.trim().charAt(0) === '.')) {
                        // Skipping this as it's an invalid value
                    } else {
                        client.sadd(domain + ":category", category);
                    }

                    bar.tick();
                    // resume the readstream
                    s.resume();

                })();
            })
            .on('error', function(err){
                console.log('Error while reading file.');
                console.log('\nerror\n');
                count(err, domainNumber);
            })
            .on('end', function(){
                callback(null, domainNumber);
            })
        );
    });
}


client.on("ready", function () {
    a.eachSeries(categories, function(category, callback) {
        importCategory(category, function(err, count) {
            if (err) return callback(err);
            console.log('Imported ' + count + ' domains!\n');
            callback();
        });
    }, function(err) {
        if (err) return consoole.log(err);
        console.log("Successfully Imported",totalCount,"records");
    });
});
