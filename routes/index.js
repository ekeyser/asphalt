var express = require("express");
var router = new express.Router();
var AWS = require("aws-sdk");
var common = require("../common");
var config = common.config();
var util = require("util");
AWS.config.loadFromPath(config.aws_config_path);
var dynamodb = new AWS.DynamoDB({endpoint: new AWS.Endpoint(config.dynamodb_endpoint)});
var objAppConfig = {};
var sKey;
var response;

var getConfiguration = function () {
    "use strict";

    var objParams = {
        TableName: config.dynamo_table_configuration,
        Key: {
            "configuration": {
                S: sKey
            }
        }
    };

    dynamodb.getItem(objParams, function (err, data) {
        if (err) {
            console.warn(err, err.stack);
        } else {
            console.log(data);
            response.setHeader("content-type", "application/json");
            response.send(data.Item.content);
            // response.send(JSON.stringify(data.Item.content));
        }
    });
};

router.get("/", function (req, res, next) {
    "use strict";

    response = res;
    // console.log(util.inspect(req.query.key, {showHidden: true, depth: null}));
    sKey = req.query.key;
    getConfiguration();
});

module.exports = router;
