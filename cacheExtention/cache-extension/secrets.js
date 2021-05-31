#!/usr/bin/env node
const yaml = require('js-yaml');
const fs = require('fs');
const AWS = require('aws-sdk');
const express = require("express");
const app = express();
const file = "/var/task/config.yaml";
const PORT = 8080;
const awsSecretManagerClient = new AWS.SecretsManager();
const awsS3Client = new AWS.S3();
const awsAppConfigClient = new AWS.AppConfig();
const awsDdbClient = new AWS.DynamoDB.DocumentClient();

async function checkForCacheConfigs() {
    fs.open(file, 'r', (err, fd) => {
        if (err) {
            if (err.code === 'ENOENT') {
                console.log('Config.yaml doesnt exist so no parsing required');
                return;
            }
        }
        cacheItems();
    });
}

var cache = {
    s3: {},
    dynamoDb: {},
    appConfig: {},
}

var cacheLastUpdated;


async function cacheSecrets(params) {}
async function cacheS3(params) {
    console.log(params)
    console.log('Caching S3 calls')
    try {
        for(var itemToCache of Object.keys(params)) {
            if(params[itemToCache].bucket && params[itemToCache].key) {
                const s3Params = {
                    Bucket: params[itemToCache].bucket,
                    Key: params[itemToCache].key
                }
                const getFromS3 = await awsS3Client.getObject(s3Params).promise()
                cache.s3[itemToCache] = getFromS3.Body.toString('utf-8')
            }
        }
    } catch (error) {
        console.error('Failed to Cache S3', params,  error)
        // throw error
    }
}

async function cacheDdb(params) {
    console.log('Caching DDB queries')
    try {
        for(var itemToCache of Object.keys(params)) {
            if(params[itemToCache].tableName && params[itemToCache].PK && params[itemToCache].SK) {
                let dbParams = {
                    TableName : params[itemToCache].tableName,
                    Key: {
                      PK: params[itemToCache].PK,
                      SK: params[itemToCache].SK
                    }
                }
                console.log(dbParams)
                const queryResult = await awsDdbClient.get(dbParams).promise();
                console.log(queryResult)
                if(Object.keys(queryResult).length > 0) {
                    cache.dynamoDb[itemToCache] = queryResult.Item
                } else {
                    console.error('Empty Item Returend for ', JSON.stringify(dbParams), 'not caching')
                }
            } else {
                console.log('Invalid Params, please add tableName, PK, SK')
            }
        }
    } catch (error) {
        console.error('Failed to Cache Ddb', params, error)
        // throw error
    }
}
async function cacheAppConfig(params) {}


async function cacheItems() {
    // Read the file
    try {
        var fileContents = fs.readFileSync(file, 'utf8');
        var data = yaml.safeLoad(fileContents);
        // console.log(JSON.stringify(data));

        if (data !== null) {       
            const {dynamoDb, s3, appConfig} = data;
            const cachePromises = []
            
            if(dynamoDb) cachePromises.push(cacheDdb(dynamoDb))
            if(s3) cachePromises.push(cacheS3(s3)) 
            if(appConfig) cachePromises.push(cacheAppConfig(appConfig))

            await Promise.all(cachePromises);
            // Read timeout from environment variable and set expiration timestamp
            var timeOut = parseInt(process.env.CACHE_TIMEOUT || 10);
            var s = new Date();
            s.setMinutes(s.getMinutes() + timeOut);
            cacheLastUpdated = s;
        }
    } catch (e) {
        console.error(e);
    }
}

async function processPayload(req, res) {
    console.log(cacheLastUpdated, new Date())
    if ( !cacheLastUpdated || new Date() > cacheLastUpdated) {
        await cacheItems();
        console.log("Cache update is complete")
    }
    res.setHeader("Content-Type", "application/json");
    const apiResponse = JSON.stringify(cache[req.params.service][req.params.name])
    if(!apiResponse) {
        res.status(400);
        res.end('Invalid cache params');
    }
    res.status(200);
    res.end(apiResponse);
}

async function startHttpServer() {
    app.get("/cache/:service/:name", function (req, res) {
        return processPayload(req, res);
    });

    app.listen(PORT, function (error) {
        if (error) throw error
        console.log("Server created Successfully on PORT", PORT)
    });
}

module.exports = {
    cacheSecrets,
    startHttpServer
};
